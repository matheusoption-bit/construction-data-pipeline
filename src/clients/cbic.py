"""
Cliente para API/dados do CBICdados (Câmara Brasileira da Indústria da Construção).

Este módulo fornece acesso aos dados estatísticos publicados pela CBIC,
incluindo CUB (Custo Unitário Básico), indicadores econômicos e séries históricas.

Características:
- Cache local de arquivos Excel
- Metadata tracking (checksums, datas)
- Retry automático com backoff exponencial
- Parsing robusto de múltiplos formatos de Excel

Exemplo de uso:
    >>> from src.clients.cbic import CBICClient
    >>> 
    >>> client = CBICClient()
    >>> 
    >>> # Baixar e parsear CUB por estado
    >>> df = client.fetch_cub_historical(uf="SC")
    >>> print(df.head())
    >>> 
    >>> # Download direto de tabela
    >>> filepath = client.download_table("06.A.06", "BI", 53)
    >>> print(f"Arquivo salvo em: {filepath}")
"""

import hashlib
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, Tuple
import re

import pandas as pd
import requests
import structlog
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type
)

logger = structlog.get_logger(__name__)


class CBICClient:
    """
    Cliente para acessar dados do CBICdados.
    
    Gerencia downloads, cache e parsing de arquivos Excel da CBIC.
    
    Attributes:
        base_url: URL base da CBIC para anexos
        cache_dir: Diretório local para cache de arquivos
        timeout: Timeout em segundos para requests HTTP
    
    Example:
        >>> client = CBICClient()
        >>> df_cub = client.fetch_cub_historical("SC")
        >>> print(f"CUB SC histórico: {len(df_cub)} meses")
    """
    
    # URLs conhecidas de tabelas importantes
    KNOWN_TABLES = {
        "cub_global": {
            "table_id": "06.A.06",
            "table_type": "BI",
            "number": 53,
            "description": "CUB/m² por UF - Global",
            "url": "http://www.cbicdados.com.br/media/anexos/tabela_06.A.06_BI_53.xlsx"
        },
        "cub_materiais": {
            "table_id": "06.A.07",
            "table_type": "BI",
            "number": 53,
            "description": "CUB/m² por UF - Materiais",
            "url": "http://www.cbicdados.com.br/media/anexos/tabela_06.A.07_BI_53.xlsx"
        },
        "cub_mao_obra": {
            "table_id": "06.A.08",
            "table_type": "BI",
            "number": 53,
            "description": "CUB/m² por UF - Mão de obra",
            "url": "http://www.cbicdados.com.br/media/anexos/tabela_06.A.08_BI_53.xlsx"
        },
        "cub_medio_brasil": {
            "table_id": "06.A.01",
            "table_type": "BI",
            "number": 54,
            "description": "CUB Médio Brasil",
            "url": "http://www.cbicdados.com.br/media/anexos/tabela_06.A.01_BI_54.xlsx"
        },
        "ipca_inpc": {
            "table_id": "09.B.14",
            "table_type": "n",
            "number": 70,
            "description": "IPCA/INPC",
            "url": "http://www.cbicdados.com.br/media/anexos/tabela_09.B.14_n_70.xlsx"
        },
        "selic_cdi": {
            "table_id": "09.B.10",
            "table_type": "n",
            "number": 20,
            "description": "Selic/CDI",
            "url": "http://www.cbicdados.com.br/media/anexos/tabela_09.B.10_n_20.xlsx"
        },
        "cambio": {
            "table_id": "09.B.08",
            "table_type": "n",
            "number": 60,
            "description": "Taxa de Câmbio",
            "url": "http://www.cbicdados.com.br/media/anexos/tabela_09.B.08_n_60.xlsx"
        },
        "salario_minimo": {
            "table_id": "09.B.13",
            "table_type": "n",
            "number": 6,
            "description": "Salário Mínimo",
            "url": "http://www.cbicdados.com.br/media/anexos/tabela_09.B.13_n_6.xlsx"
        }
    }
    
    def __init__(
        self,
        base_url: str = "http://www.cbicdados.com.br/media/anexos/",
        cache_dir: Optional[Path] = None,
        timeout: int = 60
    ):
        """
        Inicializa cliente CBIC.
        
        Args:
            base_url: URL base para downloads
            cache_dir: Diretório de cache (padrão: data/cache/cbic/)
            timeout: Timeout em segundos para requests
        """
        self.base_url = base_url
        self.timeout = timeout
        
        # Configurar diretório de cache
        if cache_dir is None:
            self.cache_dir = Path("data/cache/cbic")
        else:
            self.cache_dir = Path(cache_dir)
        
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(
            "cbic_client_initialized",
            cache_dir=str(self.cache_dir),
            base_url=self.base_url
        )
    
    def _build_url(self, table_id: str, table_type: str, number: int) -> str:
        """
        Constrói URL para tabela específica.
        
        Args:
            table_id: ID da tabela (ex: "06.A.06")
            table_type: Tipo da tabela (ex: "BI", "n")
            number: Número da tabela (ex: 53)
        
        Returns:
            URL completa para download
        
        Example:
            >>> client._build_url("06.A.06", "BI", 53)
            'http://www.cbicdados.com.br/media/anexos/tabela_06.A.06_BI_53.xlsx'
        """
        filename = f"tabela_{table_id}_{table_type}_{number}.xlsx"
        return self.base_url + filename
    
    def _calculate_checksum(self, filepath: Path) -> str:
        """
        Calcula SHA256 do arquivo.
        
        Args:
            filepath: Caminho do arquivo
        
        Returns:
            Hash SHA256 em hexadecimal
        """
        sha256_hash = hashlib.sha256()
        
        with open(filepath, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        
        return sha256_hash.hexdigest()
    
    def _save_metadata(
        self,
        filepath: Path,
        url: str,
        table_id: str,
        description: str
    ) -> None:
        """
        Salva metadata do arquivo baixado.
        
        Args:
            filepath: Caminho do arquivo Excel
            url: URL de origem
            table_id: ID da tabela
            description: Descrição da tabela
        """
        metadata = {
            "url": url,
            "download_date": datetime.utcnow().isoformat() + "Z",
            "checksum_sha256": self._calculate_checksum(filepath),
            "size_bytes": filepath.stat().st_size,
            "table_id": table_id,
            "description": description
        }
        
        meta_path = filepath.with_suffix(filepath.suffix + ".meta.json")
        
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)
        
        logger.info(
            "metadata_saved",
            meta_path=str(meta_path),
            checksum=metadata["checksum_sha256"][:16]
        )
    
    def _load_metadata(self, filepath: Path) -> Optional[Dict[str, Any]]:
        """
        Carrega metadata de arquivo.
        
        Args:
            filepath: Caminho do arquivo Excel
        
        Returns:
            Dicionário com metadata ou None se não existir
        """
        meta_path = filepath.with_suffix(filepath.suffix + ".meta.json")
        
        if not meta_path.exists():
            return None
        
        try:
            with open(meta_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.warning("failed_to_load_metadata", error=str(e))
            return None
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((requests.RequestException, requests.Timeout))
    )
    def download_table(
        self,
        table_id: str,
        table_type: str,
        number: int,
        force_download: bool = False
    ) -> Path:
        """
        Baixa tabela da CBIC com retry automático.
        
        Faz cache local do arquivo Excel. Se já existe no cache e não está
        corrompido, retorna o caminho sem fazer download novamente.
        
        Args:
            table_id: ID da tabela (ex: "06.A.06")
            table_type: Tipo da tabela (ex: "BI", "n")
            number: Número da tabela (ex: 53)
            force_download: Força redownload mesmo se existe no cache
        
        Returns:
            Path para arquivo Excel baixado
        
        Raises:
            requests.RequestException: Erro ao baixar arquivo
            IOError: Erro ao salvar arquivo
        
        Example:
            >>> client = CBICClient()
            >>> filepath = client.download_table("06.A.06", "BI", 53)
            >>> print(f"Arquivo: {filepath}")
        """
        url = self._build_url(table_id, table_type, number)
        filename = f"tabela_{table_id}_{table_type}_{number}.xlsx"
        filepath = self.cache_dir / filename
        
        # Verificar se já existe no cache
        if filepath.exists() and not force_download:
            logger.info("using_cached_file", filepath=str(filepath))
            return filepath
        
        logger.info("downloading_table", url=url, table_id=table_id)
        
        try:
            response = requests.get(url, timeout=self.timeout, stream=True)
            response.raise_for_status()
            
            # Salvar arquivo
            with open(filepath, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            # Verificar integridade básica
            if filepath.stat().st_size == 0:
                raise IOError(f"Arquivo baixado está vazio: {filepath}")
            
            # Salvar metadata
            description = self._get_table_description(table_id, table_type, number)
            self._save_metadata(filepath, url, table_id, description)
            
            logger.info(
                "table_downloaded",
                filepath=str(filepath),
                size_bytes=filepath.stat().st_size
            )
            
            return filepath
        
        except requests.RequestException as e:
            logger.error(
                "download_failed",
                url=url,
                error=str(e),
                error_type=type(e).__name__
            )
            raise
    
    def _get_table_description(
        self,
        table_id: str,
        table_type: str,
        number: int
    ) -> str:
        """
        Obtém descrição da tabela a partir de tabelas conhecidas.
        
        Args:
            table_id: ID da tabela
            table_type: Tipo da tabela
            number: Número da tabela
        
        Returns:
            Descrição ou string padrão
        """
        for table_info in self.KNOWN_TABLES.values():
            if (table_info["table_id"] == table_id and
                table_info["table_type"] == table_type and
                table_info["number"] == number):
                return table_info["description"]
        
        return f"Tabela {table_id}_{table_type}_{number}"
    
    def _parse_date_column(self, date_str: Any) -> Optional[str]:
        """
        Faz parsing de coluna de data em múltiplos formatos.
        
        Formatos suportados:
        - "jan/24" -> "2024-01-01"
        - "01/2024" -> "2024-01-01"
        - "2024-01" -> "2024-01-01"
        - "janeiro/2024" -> "2024-01-01"
        
        Args:
            date_str: String com data
        
        Returns:
            Data no formato YYYY-MM-DD ou None se inválida
        """
        if pd.isna(date_str):
            return None
        
        date_str = str(date_str).strip().lower()
        
        # Mapeamento de nomes de meses
        month_map = {
            "jan": "01", "janeiro": "01",
            "fev": "02", "fevereiro": "02",
            "mar": "03", "março": "03", "marco": "03",
            "abr": "04", "abril": "04",
            "mai": "05", "maio": "05",
            "jun": "06", "junho": "06",
            "jul": "07", "julho": "07",
            "ago": "08", "agosto": "08",
            "set": "09", "setembro": "09",
            "out": "10", "outubro": "10",
            "nov": "11", "novembro": "11",
            "dez": "12", "dezembro": "12"
        }
        
        # Padrão: "jan/24" ou "janeiro/2024"
        match = re.match(r"([a-z]+)[/\-](\d{2,4})", date_str)
        if match:
            month_name, year = match.groups()
            month = month_map.get(month_name)
            if month:
                # Converter ano de 2 dígitos
                if len(year) == 2:
                    year = "20" + year if int(year) < 50 else "19" + year
                return f"{year}-{month}-01"
        
        # Padrão: "01/2024" ou "1/2024"
        match = re.match(r"(\d{1,2})[/\-](\d{4})", date_str)
        if match:
            month, year = match.groups()
            return f"{year}-{month.zfill(2)}-01"
        
        # Padrão: "2024-01"
        match = re.match(r"(\d{4})[/\-](\d{1,2})", date_str)
        if match:
            year, month = match.groups()
            return f"{year}-{month.zfill(2)}-01"
        
        logger.warning("unparseable_date", date_str=date_str)
        return None
    
    def _clean_numeric_value(self, value: Any) -> Optional[float]:
        """
        Limpa valor numérico (remove pontos de milhar, converte vírgula).
        
        Args:
            value: Valor a limpar
        
        Returns:
            Float ou None se inválido
        
        Example:
            >>> client._clean_numeric_value("1.234,56")
            1234.56
            >>> client._clean_numeric_value("R$ 2.500,00")
            2500.0
        """
        if pd.isna(value):
            return None
        
        # Se já é número, retornar
        if isinstance(value, (int, float)):
            return float(value)
        
        # Converter para string e limpar
        value_str = str(value).strip()
        
        # Remover símbolos comuns
        value_str = re.sub(r"[R$%\s]", "", value_str)
        
        # Remover pontos de milhar e converter vírgula decimal
        value_str = value_str.replace(".", "")
        value_str = value_str.replace(",", ".")
        
        try:
            return float(value_str)
        except ValueError:
            logger.warning("invalid_numeric_value", value=value)
            return None
    
    def parse_cub_by_state(
        self,
        filepath: Path,
        uf: str = "SC",
        tipo_cub: str = "R1-N"
    ) -> pd.DataFrame:
        """
        Faz parsing de arquivo CUB por UF.
        
        Lê sheet específica da UF e extrai série histórica completa.
        Arquivos CBIC têm uma aba por estado com série temporal completa.
        
        Estrutura do arquivo:
        - Coluna 0 (MÊS): ANO (2007, NaN, NaN, ..., 2008, NaN, ...)
        - Coluna 1 (Unnamed: 1): MÊS (FEV, MAR, ABR, ...)
        - Coluna 2 (Unnamed: 2): Valores em R$/m² (611.28, 615.43, ...)
        - Demais colunas: Variações percentuais
        
        Args:
            filepath: Caminho do arquivo Excel
            uf: Sigla do estado (ex: "SC", "SP")
            tipo_cub: Tipo de CUB (DEPRECATED - arquivo não especifica tipo)
        
        Returns:
            DataFrame com colunas:
            - data_referencia: YYYY-MM-DD
            - uf: Sigla do estado
            - tipo_cub: "CUB-MEDIO" (arquivo não especifica tipo)
            - custo_m2: Custo em R$/m²
            - fonte_url: URL original
            - checksum_dados: SHA256 dos dados
            - metodo_versao: Versão do método de parsing
            - created_at: Timestamp da criação
        
        Raises:
            FileNotFoundError: Arquivo não encontrado
            ValueError: Erro ao parsear dados
        
        Example:
            >>> client = CBICClient()
            >>> filepath = client.download_table("06.A.06", "BI", 53)
            >>> df = client.parse_cub_by_state(filepath, uf="SC")
            >>> print(f"{len(df)} meses de histórico")
        """
        if not filepath.exists():
            raise FileNotFoundError(f"Arquivo não encontrado: {filepath}")
        
        logger.info("parsing_cub_by_state", filepath=str(filepath), uf=uf)
        
        try:
            # Carregar metadata
            metadata = self._load_metadata(filepath)
            
            # Ler Excel - skiprows=7 é o padrão identificado
            df = pd.read_excel(
                filepath,
                sheet_name=uf,
                skiprows=7
            )
            
            if df.empty:
                logger.error("empty_dataframe", uf=uf)
                return pd.DataFrame()
            
            logger.info(
                "excel_loaded",
                shape=df.shape,
                columns=list(df.columns)
            )
            
            # Estrutura esperada:
            # Col 0: MÊS (ano aparece periodicamente)
            # Col 1: Unnamed: 1 (nome do mês)
            # Col 2: Unnamed: 2 (valor em R$/m²)
            
            # Renomear colunas
            col_names = {
                df.columns[0]: 'ano_raw',
                df.columns[1]: 'mes_raw',
                df.columns[2]: 'valor_raw'
            }
            df_clean = df.rename(columns=col_names)[['ano_raw', 'mes_raw', 'valor_raw']].copy()
            
            # Preencher anos (forward fill)
            df_clean['ano'] = pd.to_numeric(df_clean['ano_raw'], errors='coerce')
            df_clean['ano'] = df_clean['ano'].ffill()
            
            # Converter mês para número
            month_map = {
                'JAN': 1, 'FEV': 2, 'MAR': 3, 'ABR': 4,
                'MAI': 5, 'JUN': 6, 'JUL': 7, 'AGO': 8,
                'SET': 9, 'OUT': 10, 'NOV': 11, 'DEZ': 12
            }
            
            df_clean['mes'] = df_clean['mes_raw'].str.upper().str.strip().map(month_map)
            
            # Construir data_referencia
            df_clean['data_referencia'] = pd.to_datetime(
                df_clean['ano'].astype('Int64').astype(str) + '-' +
                df_clean['mes'].astype('Int64').astype(str).str.zfill(2) + '-01',
                errors='coerce'
            )
            
            # Limpar valores numéricos
            df_clean['custo_m2'] = pd.to_numeric(df_clean['valor_raw'], errors='coerce')
            
            # Filtrar linhas válidas
            df_result = df_clean[
                (df_clean['data_referencia'].notna()) &
                (df_clean['custo_m2'].notna()) &
                (df_clean['custo_m2'] > 0)
            ].copy()
            
            if df_result.empty:
                logger.warning("no_valid_rows_after_cleaning", uf=uf)
                return pd.DataFrame()
            
            # Adicionar metadados
            df_result['uf'] = uf
            df_result['tipo_cub'] = 'CUB-MEDIO'  # Arquivo não especifica tipo
            df_result['fonte_url'] = metadata['url'] if metadata else str(filepath)
            df_result['checksum_dados'] = metadata['checksum_sha256'][:16] if metadata else ''
            df_result['metodo_versao'] = '1.0.0'
            df_result['created_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # Selecionar colunas finais
            result = df_result[[
                'data_referencia',
                'uf',
                'tipo_cub',
                'custo_m2',
                'fonte_url',
                'checksum_dados',
                'metodo_versao',
                'created_at'
            ]].sort_values('data_referencia').reset_index(drop=True)
            
            # Converter data_referencia para string
            result['data_referencia'] = result['data_referencia'].dt.strftime('%Y-%m-%d')
            
            logger.info(
                "cub_parsed",
                uf=uf,
                rows=len(result),
                date_range=f"{result['data_referencia'].min()} até {result['data_referencia'].max()}",
                value_range=f"R$ {result['custo_m2'].min():.2f} - R$ {result['custo_m2'].max():.2f}"
            )
            
            return result
        
        except Exception as e:
            logger.error(
                "parse_cub_failed",
                filepath=str(filepath),
                uf=uf,
                error=str(e),
                error_type=type(e).__name__
            )
            raise
    
    def fetch_cub_historical(
        self,
        uf: str = "SC",
        tipo_cub: str = "R1-N",
        force_download: bool = False
    ) -> pd.DataFrame:
        """
        Baixa e parseia série histórica completa de CUB para um estado.
        
        Método de conveniência que combina download + parsing.
        
        Args:
            uf: Sigla do estado (padrão: "SC")
            tipo_cub: Tipo de CUB (padrão: "R1-N")
            force_download: Força redownload mesmo se existe cache
        
        Returns:
            DataFrame com série histórica de CUB
        
        Example:
            >>> client = CBICClient()
            >>> df = client.fetch_cub_historical("SC")
            >>> print(f"CUB/SC: {len(df)} meses de histórico")
            >>> print(f"Último valor: R$ {df.iloc[-1]['custo_m2']:.2f}/m²")
        """
        logger.info("fetching_cub_historical", uf=uf, tipo_cub=tipo_cub)
        
        # Baixar tabela CUB global
        filepath = self.download_table(
            table_id="06.A.06",
            table_type="BI",
            number=53,
            force_download=force_download
        )
        
        # Parsear dados
        df = self.parse_cub_by_state(filepath, uf=uf, tipo_cub=tipo_cub)
        
        return df
    
    # =========================================================================
    # MÉTODOS DO SISTEMA CUB COMPLETO - BI Construção Civil Master
    # =========================================================================
    
    def get_cub_global_oneroso_complete(self, force_download: bool = False) -> pd.DataFrame:
        """
        Busca CUB Global Brasil Oneroso (TODOS os tipos).
        
        Retorna série histórica completa com:
        - 10 tipos de CUB (R1-N, R8-N, R16-N, PP4-N, PIS, RP1Q, CSL8-N, CSL16-N, CAL8-N, GI)
        - Dados desde 2015 até presente
        - Variações MoM e YoY calculadas
        
        Returns:
            DataFrame com colunas:
            - data_referencia: datetime
            - tipo_cub: str
            - valor_m2: float
            - variacao_mensal: float
            - variacao_anual: float
            - regime: str ('oneroso')
        """
        logger.info("fetching_cub_global_oneroso_complete")
        
        # Download
        filepath = self.download_table(
            table_id="06.A.01",
            table_type="BI",
            number=54,
            force_download=force_download
        )
        
        # Parse Excel
        df = pd.read_excel(filepath, sheet_name=0)
        
        # Estrutura: primeira coluna é data, demais são tipos de CUB
        date_col = df.columns[0]
        
        # Melt (unpivot)
        df_long = df.melt(
            id_vars=[date_col],
            var_name='tipo_cub',
            value_name='valor_m2'
        )
        
        df_long = df_long.rename(columns={date_col: 'data_referencia'})
        
        # Converter data (formato pode ser 'jan/2015', 'Jan-15', etc)
        df_long['data_referencia'] = pd.to_datetime(
            df_long['data_referencia'],
            format='%b/%Y',
            errors='coerce'
        )
        
        # Se falhou, tentar outro formato
        if df_long['data_referencia'].isna().all():
            df_long['data_referencia'] = pd.to_datetime(
                df_long['data_referencia'],
                format='%b-%y',
                errors='coerce'
            )
        
        # Limpar valores
        df_long['valor_m2'] = pd.to_numeric(df_long['valor_m2'], errors='coerce')
        df_long['regime'] = 'oneroso'
        
        # Remover NaN
        df_long = df_long.dropna(subset=['data_referencia', 'valor_m2'])
        
        # Ordenar
        df_long = df_long.sort_values(['tipo_cub', 'data_referencia'])
        
        # Calcular variações
        df_long['variacao_mensal'] = df_long.groupby('tipo_cub')['valor_m2'].pct_change() * 100
        df_long['variacao_anual'] = df_long.groupby('tipo_cub')['valor_m2'].pct_change(periods=12) * 100
        
        # Tratar inf
        df_long['variacao_mensal'] = df_long['variacao_mensal'].replace([float('inf'), float('-inf')], None)
        df_long['variacao_anual'] = df_long['variacao_anual'].replace([float('inf'), float('-inf')], None)
        
        logger.info(
            "cub_global_oneroso_fetched",
            rows=len(df_long),
            tipos=df_long['tipo_cub'].nunique(),
            periodo=f"{df_long['data_referencia'].min().date()} até {df_long['data_referencia'].max().date()}"
        )
        
        return df_long
    
    def get_cub_por_uf_complete(self, force_download: bool = False) -> pd.DataFrame:
        """
        Busca CUB por UF (TODAS as UFs, TODOS os tipos).
        
        Estrutura dimensional completa:
        - 27 UFs
        - 10 tipos de CUB
        - Série histórica 2015-2025
        
        Returns:
            DataFrame com colunas:
            - data_referencia: datetime
            - uf: str
            - tipo_cub: str
            - valor_m2: float
            - variacao_mensal: float
            - variacao_anual: float
            - regime: str
        """
        logger.info("fetching_cub_por_uf_complete")
        
        # Download
        filepath = self.download_table(
            table_id="06.A.06",
            table_type="BI",
            number=53,
            force_download=force_download
        )
        
        # Parse Excel
        df = pd.read_excel(filepath, sheet_name=0)
        
        # Estrutura típica:
        # - Coluna 0: UF
        # - Coluna 1: Tipo CUB
        # - Demais colunas: Meses
        
        uf_col = df.columns[0]
        tipo_col = df.columns[1]
        date_cols = df.columns[2:]
        
        # Melt
        df_long = df.melt(
            id_vars=[uf_col, tipo_col],
            value_vars=date_cols,
            var_name='data_referencia',
            value_name='valor_m2'
        )
        
        df_long = df_long.rename(columns={
            uf_col: 'uf',
            tipo_col: 'tipo_cub'
        })
        
        # Converter data
        df_long['data_referencia'] = pd.to_datetime(
            df_long['data_referencia'],
            format='%b/%Y',
            errors='coerce'
        )
        
        # Limpar
        df_long['valor_m2'] = pd.to_numeric(df_long['valor_m2'], errors='coerce')
        df_long['regime'] = 'oneroso'
        
        df_long = df_long.dropna(subset=['data_referencia', 'valor_m2'])
        
        # Ordenar
        df_long = df_long.sort_values(['uf', 'tipo_cub', 'data_referencia'])
        
        # Calcular variações por UF + tipo
        df_long['variacao_mensal'] = df_long.groupby(['uf', 'tipo_cub'])['valor_m2'].pct_change() * 100
        df_long['variacao_anual'] = df_long.groupby(['uf', 'tipo_cub'])['valor_m2'].pct_change(periods=12) * 100
        
        # Tratar inf
        df_long['variacao_mensal'] = df_long['variacao_mensal'].replace([float('inf'), float('-inf')], None)
        df_long['variacao_anual'] = df_long['variacao_anual'].replace([float('inf'), float('-inf')], None)
        
        logger.info(
            "cub_por_uf_fetched",
            rows=len(df_long),
            ufs=df_long['uf'].nunique(),
            tipos=df_long['tipo_cub'].nunique(),
            periodo=f"{df_long['data_referencia'].min().date()} até {df_long['data_referencia'].max().date()}"
        )
        
        return df_long
    
    def get_cub_componentes_complete(self, force_download: bool = False) -> pd.DataFrame:
        """
        Busca TODOS os componentes de CUB (materiais, mão de obra, despesas, equipamentos).
        
        Returns:
            DataFrame consolidado com colunas:
            - data_referencia: datetime
            - tipo_cub: str
            - componente: str ('materiais', 'mao_obra', 'despesa_adm', 'equipamento')
            - valor_m2: float
            - participacao_percentual: float
        """
        logger.info("fetching_cub_componentes_complete")
        
        componentes_map = {
            'materiais': ('06.A.02', 'BI', 52),
            'mao_obra': ('06.A.03', 'BI', 52),
            'despesa_adm': ('06.A.04', 'BI', 52),
            'equipamento': ('06.A.05', 'BI', 52)
        }
        
        all_dfs = []
        
        for comp_name, (table_id, table_type, number) in componentes_map.items():
            logger.info("fetching_componente", componente=comp_name)
            
            # Download
            filepath = self.download_table(
                table_id=table_id,
                table_type=table_type,
                number=number,
                force_download=force_download
            )
            
            # Parse
            df = pd.read_excel(filepath, sheet_name=0)
            
            date_col = df.columns[0]
            
            df_long = df.melt(
                id_vars=[date_col],
                var_name='tipo_cub',
                value_name='valor_m2'
            )
            
            df_long = df_long.rename(columns={date_col: 'data_referencia'})
            
            df_long['data_referencia'] = pd.to_datetime(
                df_long['data_referencia'],
                format='%b/%Y',
                errors='coerce'
            )
            
            df_long['componente'] = comp_name
            df_long['valor_m2'] = pd.to_numeric(df_long['valor_m2'], errors='coerce')
            
            df_long = df_long.dropna(subset=['data_referencia', 'valor_m2'])
            
            all_dfs.append(df_long)
        
        # Consolidar
        result = pd.concat(all_dfs, ignore_index=True)
        
        # Calcular participação percentual
        # Para cada tipo_cub + data, somar total e calcular %
        result['total_cub'] = result.groupby(['tipo_cub', 'data_referencia'])['valor_m2'].transform('sum')
        result['participacao_percentual'] = (result['valor_m2'] / result['total_cub']) * 100
        
        result = result.drop('total_cub', axis=1)
        
        logger.info(
            "cub_componentes_fetched",
            rows=len(result),
            componentes=result['componente'].nunique(),
            tipos=result['tipo_cub'].nunique()
        )
        
        return result
    
    def get_cub_medio_complete(self, force_download: bool = False) -> pd.DataFrame:
        """
        Busca CUB Médio (residencial, multifamiliar, comercial, industrial).
        
        Returns:
            DataFrame com colunas:
            - data_referencia: datetime
            - categoria: str ('residencial', 'multifamiliar', 'comercial', 'industrial')
            - valor_m2: float
            - variacao_mensal: float
            - variacao_anual: float
        """
        logger.info("fetching_cub_medio_complete")
        
        categorias_map = {
            'residencial': ('06.C.01', 'Global_Brasil_Serie_Historica_BI', 52),
            'multifamiliar': ('06.C.02', 'Global_Brasil_Serie_Historica_BI', 52),
            'comercial': ('06.C.03', 'Global_Brasil_Serie_Historica_BI', 52),
            'industrial': ('06.C.04', 'Global_Brasil_Serie_Historica_BI', 52)
        }
        
        all_dfs = []
        
        for categoria, (table_id, table_type, number) in categorias_map.items():
            logger.info("fetching_cub_medio_categoria", categoria=categoria)
            
            # URL customizada (formato diferente)
            url = f"http://www.cbicdados.com.br/media/anexos/tabela_{table_id}_{table_type}_{number}.xlsx"
            
            # Construir filename para cache
            filename = url.split('/')[-1]
            cache_filepath = self.cache_dir / filename
            
            # Download
            if not cache_filepath.exists() or force_download:
                response = requests.get(url, timeout=self.timeout)
                response.raise_for_status()
                
                with open(cache_filepath, 'wb') as f:
                    f.write(response.content)
            
            # Parse
            df = pd.read_excel(cache_filepath, sheet_name=0)
            
            # Assumindo primeira coluna é data, segunda é valor
            date_col = df.columns[0]
            value_col = df.columns[1] if len(df.columns) > 1 else df.columns[0]
            
            df_clean = df[[date_col, value_col]].copy()
            df_clean.columns = ['data_referencia', 'valor_m2']
            
            df_clean['data_referencia'] = pd.to_datetime(
                df_clean['data_referencia'],
                format='%b/%Y',
                errors='coerce'
            )
            
            df_clean['categoria'] = categoria
            df_clean['valor_m2'] = pd.to_numeric(df_clean['valor_m2'], errors='coerce')
            
            df_clean = df_clean.dropna(subset=['data_referencia', 'valor_m2'])
            
            all_dfs.append(df_clean)
        
        # Consolidar
        result = pd.concat(all_dfs, ignore_index=True)
        
        # Ordenar
        result = result.sort_values(['categoria', 'data_referencia'])
        
        # Calcular variações
        result['variacao_mensal'] = result.groupby('categoria')['valor_m2'].pct_change() * 100
        result['variacao_anual'] = result.groupby('categoria')['valor_m2'].pct_change(periods=12) * 100
        
        # Tratar inf
        result['variacao_mensal'] = result['variacao_mensal'].replace([float('inf'), float('-inf')], None)
        result['variacao_anual'] = result['variacao_anual'].replace([float('inf'), float('-inf')], None)
        
        logger.info(
            "cub_medio_fetched",
            rows=len(result),
            categorias=result['categoria'].nunique()
        )
        
        return result
