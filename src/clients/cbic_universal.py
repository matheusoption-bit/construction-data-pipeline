"""
Cliente Universal para todas as fontes CBIC.

Suporta:
- CUB (oneroso/desonerado, global/UF, componentes)
- SINAPI
- Cimento
- Mercado Imobiliário
- Indicadores Econômicos

Fonte: http://www.cbicdados.com.br
"""

import requests
import pandas as pd
from pathlib import Path
import json
from datetime import datetime
from typing import Optional, Dict, List
import structlog

logger = structlog.get_logger(__name__)


class CBICUniversalClient:
    """
    Cliente para buscar TODAS as fontes CBIC.
    
    113+ arquivos suportados.
    """
    
    def __init__(self):
        self.cache_dir = Path("data/cache/cbic")
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        # Carregar configuração
        config_path = Path("configs/cbic_sources.json")
        with open(config_path) as f:
            self.sources = json.load(f)
        
        logger.info("cbic_universal_client_initialized", sources=len(self.sources))
    
    def download_source(
        self,
        categoria: str,
        subcategoria: str,
        force: bool = False
    ) -> Optional[Path]:
        """
        Baixa arquivo de uma fonte específica.
        
        Args:
            categoria: "cub_oneroso", "sinapi", "cimento", etc
            subcategoria: "global", "por_uf", "materiais", etc
            force: Forçar download mesmo se cache existe
        
        Returns:
            Path do arquivo baixado ou None se erro
        """
        # Navegar config
        source_config = self.sources.get(categoria, {})
        
        if subcategoria:
            source_config = source_config.get(subcategoria, {})
        
        url = source_config.get("url")
        
        if not url:
            logger.error("source_not_found", categoria=categoria, sub=subcategoria)
            return None
        
        # Definir cache path
        filename = url.split("/")[-1]
        cache_file = self.cache_dir / f"{categoria}_{subcategoria}_{filename}"
        
        # Verificar cache
        if cache_file.exists() and not force:
            logger.info("using_cache", file=str(cache_file))
            return cache_file
        
        # Baixar
        try:
            logger.info("downloading", url=url)
            
            response = requests.get(url, timeout=60)
            response.raise_for_status()
            
            cache_file.write_bytes(response.content)
            
            logger.info(
                "download_success",
                file=str(cache_file),
                size_kb=len(response.content) // 1024
            )
            
            return cache_file
        
        except Exception as e:
            logger.error("download_failed", url=url, error=str(e))
            return None
    
    def parse_cub_por_uf(self, filepath: Path) -> pd.DataFrame:
        """
        Parse arquivo CUB por UF (multi-sheet).
        
        Estrutura real CBIC:
        - Col 0: Contém ANO (ex: 2007) na primeira linha, depois NaN
        - Col 1: Nome do mês (FEV, MAR, ABR, ...)
        - Col 2+: Valores dos CUBs
        
        Returns:
            DataFrame long:
            | data | uf | tipo_cub | valor |
        """
        logger.info("parsing_cub_uf", file=filepath.name)
        
        all_data = []
        
        # Mapeamento de meses
        meses_map = {
            'JAN': 1, 'FEV': 2, 'MAR': 3, 'ABR': 4, 'MAI': 5, 'JUN': 6,
            'JUL': 7, 'AGO': 8, 'SET': 9, 'OUT': 10, 'NOV': 11, 'DEZ': 12
        }
        
        # Ler TODAS as sheets (27 UFs)
        excel_file = pd.ExcelFile(filepath)
        
        for sheet_name in excel_file.sheet_names:
            try:
                # Ler dados
                df = pd.read_excel(filepath, sheet_name=sheet_name, skiprows=7)
                
                # Primeira coluna tem ANO, segunda tem MÊS (nome)
                df.columns = ['ano_col', 'mes_nome'] + [f'val_{i}' for i in range(len(df.columns)-2)]
                
                # Forward fill do ano (2007 aparece só uma vez no topo)
                df['ano_col'] = df['ano_col'].ffill()
                
                # Converter ano para numeric
                df['ano'] = pd.to_numeric(df['ano_col'], errors='coerce')
                
                # Remover linhas sem ano válido
                df = df.dropna(subset=['ano'])
                
                # Converter mês nome para número
                df['mes'] = df['mes_nome'].str.upper().map(meses_map)
                
                # Remover linhas sem mês válido
                df = df.dropna(subset=['mes'])
                
                # Criar data
                df['data'] = pd.to_datetime(
                    df['ano'].astype(int).astype(str) + '-' +
                    df['mes'].astype(int).astype(str).str.zfill(2) + '-01',
                    errors='coerce'
                )
                
                # Pegar apenas colunas de valor
                value_cols = [col for col in df.columns if col.startswith('val_')]
                
                # Melt
                df_long = df.melt(
                    id_vars=['data'],
                    value_vars=value_cols,
                    var_name='tipo_cub',
                    value_name='valor'
                )
                
                # Adicionar UF
                df_long['uf'] = sheet_name
                
                all_data.append(df_long)
                
                logger.info("sheet_parsed", uf=sheet_name, rows=len(df_long))
            
            except Exception as e:
                logger.warning("sheet_parse_failed", sheet=sheet_name, error=str(e))
        
        if not all_data:
            return pd.DataFrame()
        
        df_final = pd.concat(all_data, ignore_index=True)
        
        # Limpar
        df_final = df_final.dropna(subset=['valor', 'data'])
        df_final['valor'] = pd.to_numeric(df_final['valor'], errors='coerce')
        df_final = df_final.dropna(subset=['valor'])
        
        # Renomear tipo_cub para algo mais legível (remover val_ prefix)
        df_final['tipo_cub'] = df_final['tipo_cub'].str.replace('val_', 'Coluna_')
        
        logger.info("parse_complete", rows=len(df_final), ufs=df_final['uf'].nunique())
        
        return df_final
    
    def get_cub_detalhado(
        self,
        tipo: str = "oneroso",
        granularidade: str = "uf"
    ) -> pd.DataFrame:
        """
        Busca CUB detalhado por tipo e UF.
        
        Args:
            tipo: "oneroso" ou "desonerado"
            granularidade: "brasil" ou "uf"
        
        Returns:
            DataFrame com série histórica completa
        """
        categoria = f"cub_{tipo}"
        subcategoria = "global" if granularidade == "brasil" else "por_uf"
        
        filepath = self.download_source(categoria, subcategoria)
        
        if not filepath:
            return pd.DataFrame()
        
        if granularidade == "uf":
            return self.parse_cub_por_uf(filepath)
        else:
            # TODO: parse Brasil (mais simples, 1 sheet só)
            pass
    
    def parse_cub_global(self, filepath: Path, tipo: str = "oneroso") -> pd.DataFrame:
        """
        Parse arquivo CUB global Brasil.
        
        Estrutura:
        - 1 sheet com Data + tipos de CUB
        
        Returns:
            DataFrame long:
            | data | tipo_cub | valor |
        """
        logger.info("parsing_cub_global", file=filepath.name)
        
        try:
            # Ler Excel (primeira sheet)
            df = pd.read_excel(filepath, skiprows=3)
            
            # Primeira coluna é data
            date_col = df.columns[0]
            df = df.rename(columns={date_col: "data"})
            
            # Converter para long format
            df_long = df.melt(
                id_vars=["data"],
                var_name="tipo_cub",
                value_name="valor"
            )
            
            # Limpar
            df_long = df_long.dropna(subset=["valor"])
            df_long["valor"] = pd.to_numeric(df_long["valor"], errors="coerce")
            df_long = df_long.dropna(subset=["valor"])
            
            logger.info("parse_complete", rows=len(df_long))
            
            return df_long
        
        except Exception as e:
            logger.error("parse_failed", error=str(e))
            return pd.DataFrame()
    
    def get_cub_global(self, tipo: str = "oneroso") -> pd.DataFrame:
        """
        Busca CUB global Brasil.
        
        Args:
            tipo: "oneroso" ou "desonerado"
        
        Returns:
            DataFrame com série histórica completa
        """
        categoria = f"cub_{tipo}"
        subcategoria = "global"
        
        filepath = self.download_source(categoria, subcategoria)
        
        if not filepath:
            return pd.DataFrame()
        
        return self.parse_cub_global(filepath, tipo)
    
    def parse_componentes(
        self,
        filepath: Path,
        componente: str
    ) -> pd.DataFrame:
        """
        Parse arquivo de componente do CUB (materiais, mão de obra, etc).
        
        Args:
            filepath: Path do arquivo Excel
            componente: "materiais", "mao_obra", "despesas_admin", "equipamentos"
        
        Returns:
            DataFrame long:
            | data | tipo_cub | valor_componente | percentual |
        """
        logger.info("parsing_componente", file=filepath.name, componente=componente)
        
        try:
            df = pd.read_excel(filepath, skiprows=3)
            
            # Primeira coluna é data
            date_col = df.columns[0]
            df = df.rename(columns={date_col: "data"})
            
            # Converter para long format
            df_long = df.melt(
                id_vars=["data"],
                var_name="tipo_cub",
                value_name="valor"
            )
            
            # Adicionar coluna de componente
            df_long["componente"] = componente
            
            # Limpar
            df_long = df_long.dropna(subset=["valor"])
            df_long["valor"] = pd.to_numeric(df_long["valor"], errors="coerce")
            df_long = df_long.dropna(subset=["valor"])
            
            logger.info("parse_complete", rows=len(df_long))
            
            return df_long
        
        except Exception as e:
            logger.error("parse_failed", error=str(e))
            return pd.DataFrame()
    
    def get_cub_componentes(self, tipo: str = "oneroso") -> pd.DataFrame:
        """
        Busca TODOS os componentes do CUB e consolida.
        
        Args:
            tipo: "oneroso" ou "desonerado"
        
        Returns:
            DataFrame consolidado:
            | data | tipo_cub | componente | valor | percentual |
        """
        categoria = f"cub_{tipo}"
        
        componentes_map = {
            "materiais": "materiais",
            "mao_obra": "mao_obra",
            "despesas_admin": "despesas_admin",
            "equipamentos": "equipamentos"
        }
        
        all_data = []
        
        for subcategoria, nome_componente in componentes_map.items():
            filepath = self.download_source(categoria, subcategoria)
            
            if filepath:
                df = self.parse_componentes(filepath, nome_componente)
                if not df.empty:
                    all_data.append(df)
        
        if not all_data:
            return pd.DataFrame()
        
        df_final = pd.concat(all_data, ignore_index=True)
        
        # Calcular percentual de cada componente
        # Agrupar por data + tipo_cub e calcular total
        df_total = df_final.groupby(["data", "tipo_cub"])["valor"].sum().reset_index()
        df_total = df_total.rename(columns={"valor": "valor_total"})
        
        # Join para calcular percentual
        df_final = df_final.merge(df_total, on=["data", "tipo_cub"])
        df_final["percentual"] = (df_final["valor"] / df_final["valor_total"] * 100).round(2)
        
        logger.info("componentes_consolidated", rows=len(df_final))
        
        return df_final
    
    def get_sinapi(self, tipo: str = "nacional") -> pd.DataFrame:
        """
        Busca índices SINAPI.
        
        Args:
            tipo: "nacional" ou "regional"
        
        Returns:
            DataFrame com série histórica SINAPI
        """
        categoria = "sinapi"
        subcategoria = f"indice_{tipo}"
        
        filepath = self.download_source(categoria, subcategoria)
        
        if not filepath:
            return pd.DataFrame()
        
        try:
            df = pd.read_excel(filepath, skiprows=3)
            
            # Parse similar ao CUB
            date_col = df.columns[0]
            df = df.rename(columns={date_col: "data"})
            
            df_long = df.melt(
                id_vars=["data"],
                var_name="categoria",
                value_name="indice"
            )
            
            # Limpar
            df_long = df_long.dropna(subset=["indice"])
            df_long["indice"] = pd.to_numeric(df_long["indice"], errors="coerce")
            df_long = df_long.dropna(subset=["indice"])
            
            logger.info("sinapi_parsed", rows=len(df_long))
            
            return df_long
        
        except Exception as e:
            logger.error("sinapi_parse_failed", error=str(e))
            return pd.DataFrame()
    
    def get_cimento(self, tipo: str = "preco") -> pd.DataFrame:
        """
        Busca dados de cimento.
        
        Args:
            tipo: "preco" ou "consumo"
        
        Returns:
            DataFrame com série histórica de cimento
        """
        categoria = "cimento"
        subcategoria = tipo
        
        filepath = self.download_source(categoria, subcategoria)
        
        if not filepath:
            return pd.DataFrame()
        
        try:
            df = pd.read_excel(filepath, skiprows=3)
            
            # Parse genérico
            date_col = df.columns[0]
            df = df.rename(columns={date_col: "data"})
            
            df_long = df.melt(
                id_vars=["data"],
                var_name="categoria",
                value_name="valor"
            )
            
            # Limpar
            df_long = df_long.dropna(subset=["valor"])
            df_long["valor"] = pd.to_numeric(df_long["valor"], errors="coerce")
            df_long = df_long.dropna(subset=["valor"])
            
            logger.info("cimento_parsed", rows=len(df_long), tipo=tipo)
            
            return df_long
        
        except Exception as e:
            logger.error("cimento_parse_failed", error=str(e))
            return pd.DataFrame()
    
    def get_mercado_imobiliario(self, metrica: str = "lancamentos") -> pd.DataFrame:
        """
        Busca dados do mercado imobiliário.
        
        Args:
            metrica: "lancamentos", "vendas" ou "estoque"
        
        Returns:
            DataFrame com série histórica do mercado imobiliário
        """
        categoria = "mercado_imobiliario"
        subcategoria = metrica
        
        filepath = self.download_source(categoria, subcategoria)
        
        if not filepath:
            return pd.DataFrame()
        
        try:
            df = pd.read_excel(filepath, skiprows=3)
            
            # Parse genérico
            date_col = df.columns[0]
            df = df.rename(columns={date_col: "data"})
            
            df_long = df.melt(
                id_vars=["data"],
                var_name="categoria",
                value_name="valor"
            )
            
            # Limpar
            df_long = df_long.dropna(subset=["valor"])
            df_long["valor"] = pd.to_numeric(df_long["valor"], errors="coerce")
            df_long = df_long.dropna(subset=["valor"])
            
            logger.info("mercado_imob_parsed", rows=len(df_long), metrica=metrica)
            
            return df_long
        
        except Exception as e:
            logger.error("mercado_imob_parse_failed", error=str(e))
            return pd.DataFrame()
    
    def listar_fontes_disponiveis(self) -> Dict:
        """
        Lista todas as fontes disponíveis no config.
        
        Returns:
            Dict com estrutura completa das fontes
        """
        return self.sources
    
    def get_metadados_fonte(self, categoria: str, subcategoria: str = None) -> Dict:
        """
        Retorna metadados de uma fonte específica.
        
        Args:
            categoria: Categoria da fonte
            subcategoria: Subcategoria (opcional)
        
        Returns:
            Dict com metadados (url, descrição, frequência, etc)
        """
        config = self.sources.get(categoria, {})
        
        if subcategoria:
            config = config.get(subcategoria, {})
        
        return config
