"""
Módulo para carregar dados no Google Sheets.

Este módulo fornece uma interface para interagir com Google Sheets usando
gspread e oauth2client. Implementa padrão singleton para cache de conexão.

Exemplo de uso:
    >>> import os
    >>> import pandas as pd
    >>> from src.etl.sheets import SheetsLoader
    >>> 
    >>> # Configurar variáveis de ambiente
    >>> os.environ["GOOGLE_CREDENTIALS_PATH"] = "credentials.json"
    >>> os.environ["GOOGLE_SPREADSHEET_ID"] = "1a2b3c4d5e6f7g8h9i0j"
    >>> 
    >>> # Criar instância do loader
    >>> loader = SheetsLoader()
    >>> 
    >>> # Escrever dados
    >>> data = [["Col1", "Col2"], ["A", "B"], ["C", "D"]]
    >>> loader.write_to_sheet("MinhaAba", data)
    >>> 
    >>> # Escrever séries temporais
    >>> df = pd.DataFrame({
    ...     "data_referencia": ["2023-01-01", "2023-02-01"],
    ...     "valor": [100.5, 102.3]
    ... })
    >>> loader.write_fact_series("ipca", df, "exec_20230101_120000")
"""

import os
import time
from datetime import datetime
from typing import Any, Dict, List, Optional
from functools import wraps

import gspread
import pandas as pd
import structlog
from oauth2client.service_account import ServiceAccountCredentials

logger = structlog.get_logger(__name__)


def rate_limit_api_call(calls_per_minute=30):
    """
    Decorator para rate limiting de chamadas à API do Google Sheets.
    
    Args:
        calls_per_minute: Número máximo de chamadas por minuto
    """
    min_interval = 60.0 / calls_per_minute
    last_called = [0.0]
    
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            elapsed = time.time() - last_called[0]
            left_to_wait = min_interval - elapsed
            if left_to_wait > 0:
                time.sleep(left_to_wait)
            result = func(*args, **kwargs)
            last_called[0] = time.time()
            return result
        return wrapper
    return decorator


class SheetsLoader:
    """
    Carregador de dados para Google Sheets com padrão singleton.
    
    Gerencia conexão com Google Sheets usando credenciais de serviço,
    fornecendo métodos para leitura e escrita de dados.
    
    Attributes:
        spreadsheet_id: ID da planilha do Google Sheets
        credentials_path: Caminho para arquivo de credenciais JSON
        _client: Cliente gspread (singleton)
        _spreadsheet: Objeto spreadsheet (singleton)
    """
    
    _instance: Optional['SheetsLoader'] = None
    _client: Optional[gspread.Client] = None
    _spreadsheet: Optional[gspread.Spreadsheet] = None
    
    def __new__(cls):
        """Implementa padrão singleton."""
        if cls._instance is None:
            cls._instance = super(SheetsLoader, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        """
        Inicializa o carregador de Google Sheets.
        
        Lê configurações de variáveis de ambiente:
        - GOOGLE_CREDENTIALS_PATH: Caminho para arquivo credentials.json
        - GOOGLE_SPREADSHEET_ID: ID da planilha
        
        Raises:
            ValueError: Se variáveis de ambiente não estiverem configuradas
        """
        # Evitar reinicialização em singleton
        if hasattr(self, '_initialized'):
            return
        
        self.credentials_path = os.getenv("GOOGLE_CREDENTIALS_PATH")
        self.spreadsheet_id = os.getenv("GOOGLE_SPREADSHEET_ID")
        
        if not self.credentials_path:
            raise ValueError(
                "GOOGLE_CREDENTIALS_PATH não configurado. "
                "Configure a variável de ambiente com o caminho para credentials.json"
            )
        
        if not self.spreadsheet_id:
            raise ValueError(
                "GOOGLE_SPREADSHEET_ID não configurado. "
                "Configure a variável de ambiente com o ID da planilha"
            )
        
        logger.info(
            "sheets_loader_initialized",
            credentials_path=self.credentials_path,
            spreadsheet_id=self.spreadsheet_id
        )
        
        self._initialized = True
    
    def _get_client(self) -> gspread.Client:
        """
        Obtém cliente gspread (cached).
        
        Returns:
            Cliente gspread autenticado
        
        Raises:
            FileNotFoundError: Se arquivo de credenciais não existir
            Exception: Erro ao autenticar com Google API
        """
        if SheetsLoader._client is None:
            try:
                logger.info("authenticating_google_sheets", path=self.credentials_path)
                
                if not os.path.exists(self.credentials_path):
                    raise FileNotFoundError(
                        f"Arquivo de credenciais não encontrado: {self.credentials_path}"
                    )
                
                # Definir escopos necessários
                scope = [
                    "https://spreadsheets.google.com/feeds",
                    "https://www.googleapis.com/auth/drive"
                ]
                
                # Autenticar
                credentials = ServiceAccountCredentials.from_json_keyfile_name(
                    self.credentials_path,
                    scope
                )
                
                SheetsLoader._client = gspread.authorize(credentials)
                
                logger.info("google_sheets_authenticated")
            
            except FileNotFoundError:
                logger.error(
                    "credentials_file_not_found",
                    path=self.credentials_path
                )
                raise
            
            except Exception as e:
                logger.error(
                    "authentication_failed",
                    error=str(e),
                    error_type=type(e).__name__
                )
                raise
        
        return SheetsLoader._client
    
    def _get_spreadsheet(self) -> gspread.Spreadsheet:
        """
        Obtém objeto spreadsheet (cached).
        
        Returns:
            Objeto Spreadsheet do gspread
        
        Raises:
            gspread.exceptions.SpreadsheetNotFound: Planilha não encontrada
            gspread.exceptions.APIError: Erro de permissão ou API
        """
        if SheetsLoader._spreadsheet is None:
            try:
                client = self._get_client()
                
                logger.info("opening_spreadsheet", spreadsheet_id=self.spreadsheet_id)
                
                SheetsLoader._spreadsheet = client.open_by_key(self.spreadsheet_id)
                
                logger.info(
                    "spreadsheet_opened",
                    title=SheetsLoader._spreadsheet.title,
                    sheet_count=len(SheetsLoader._spreadsheet.worksheets())
                )
            
            except gspread.exceptions.SpreadsheetNotFound:
                logger.error(
                    "spreadsheet_not_found",
                    spreadsheet_id=self.spreadsheet_id
                )
                raise
            
            except gspread.exceptions.APIError as e:
                logger.error(
                    "spreadsheet_api_error",
                    error=str(e),
                    spreadsheet_id=self.spreadsheet_id
                )
                raise
            
            except Exception as e:
                logger.error(
                    "spreadsheet_open_failed",
                    error=str(e),
                    error_type=type(e).__name__
                )
                raise
        
        return SheetsLoader._spreadsheet
    
    def create_sheet_if_not_exists(
        self,
        sheet_name: str,
        headers: Optional[List[str]] = None
    ) -> gspread.Worksheet:
        """
        Cria aba na planilha se não existir.
        
        Args:
            sheet_name: Nome da aba
            headers: Lista de cabeçalhos para primeira linha (opcional)
        
        Returns:
            Objeto Worksheet
        
        Raises:
            gspread.exceptions.APIError: Erro ao criar aba
        """
        spreadsheet = self._get_spreadsheet()
        
        try:
            # Tentar abrir aba existente
            worksheet = spreadsheet.worksheet(sheet_name)
            
            logger.debug("worksheet_already_exists", sheet_name=sheet_name)
            
            return worksheet
        
        except gspread.exceptions.WorksheetNotFound:
            logger.info("creating_worksheet", sheet_name=sheet_name)
            
            try:
                # Criar nova aba
                worksheet = spreadsheet.add_worksheet(
                    title=sheet_name,
                    rows=1000,
                    cols=26
                )
                
                # Adicionar cabeçalhos se fornecidos
                if headers:
                    worksheet.append_row(headers)
                    logger.info(
                        "worksheet_created_with_headers",
                        sheet_name=sheet_name,
                        headers=headers
                    )
                else:
                    logger.info("worksheet_created", sheet_name=sheet_name)
                
                return worksheet
            
            except gspread.exceptions.APIError as e:
                logger.error(
                    "worksheet_creation_failed",
                    sheet_name=sheet_name,
                    error=str(e)
                )
                raise
    
    def write_to_sheet(
        self,
        sheet_name: str,
        data: List[List[Any]],
        start_cell: str = "A1"
    ) -> None:
        """
        Escreve dados em aba específica.
        
        Cria aba se não existir. Sobrescreve dados existentes na região especificada.
        
        Args:
            sheet_name: Nome da aba
            data: Lista de listas com dados (incluindo cabeçalhos)
            start_cell: Célula inicial (notação A1)
        
        Raises:
            gspread.exceptions.APIError: Erro ao escrever dados
            ValueError: Dados inválidos
        """
        if not data:
            logger.warning("empty_data_provided", sheet_name=sheet_name)
            return
        
        logger.info(
            "writing_to_sheet",
            sheet_name=sheet_name,
            rows=len(data),
            cols=len(data[0]) if data else 0,
            start_cell=start_cell
        )
        
        try:
            worksheet = self.create_sheet_if_not_exists(sheet_name)
            
            # Limpar conteúdo existente se começando em A1
            if start_cell == "A1":
                worksheet.clear()
            
            # Escrever dados
            worksheet.update(start_cell, data)
            
            logger.info(
                "data_written_successfully",
                sheet_name=sheet_name,
                rows_written=len(data)
            )
        
        except gspread.exceptions.APIError as e:
            logger.error(
                "write_failed",
                sheet_name=sheet_name,
                error=str(e)
            )
            raise
        
        except Exception as e:
            logger.error(
                "unexpected_write_error",
                sheet_name=sheet_name,
                error=str(e),
                error_type=type(e).__name__
            )
            raise
    
    @rate_limit_api_call(calls_per_minute=20)
    def append_to_sheet(self, sheet_name: str, rows: List[List[Any]]) -> None:
        """
        Adiciona linhas ao final da aba.
        
        Args:
            sheet_name: Nome da aba
            rows: Lista de listas com linhas a adicionar
        
        Raises:
            gspread.exceptions.APIError: Erro ao adicionar dados
        """
        if not rows:
            logger.warning("empty_rows_provided", sheet_name=sheet_name)
            return
        
        logger.info(
            "appending_to_sheet",
            sheet_name=sheet_name,
            rows_count=len(rows)
        )
        
        try:
            worksheet = self.create_sheet_if_not_exists(sheet_name)
            
            # Adicionar linhas
            for row in rows:
                worksheet.append_row(row)
            
            logger.info(
                "rows_appended_successfully",
                sheet_name=sheet_name,
                rows_appended=len(rows)
            )
        
        except gspread.exceptions.APIError as e:
            logger.error(
                "append_failed",
                sheet_name=sheet_name,
                error=str(e)
            )
            raise
        
        except Exception as e:
            logger.error(
                "unexpected_append_error",
                sheet_name=sheet_name,
                error=str(e),
                error_type=type(e).__name__
            )
            raise
    
    def read_sheet(
        self,
        sheet_name: str,
        range_notation: Optional[str] = None
    ) -> List[List[Any]]:
        """
        Lê dados de uma aba.
        
        Args:
            sheet_name: Nome da aba
            range_notation: Notação de range (ex: "A1:D10"), None para ler tudo
        
        Returns:
            Lista de listas com dados da aba
        
        Raises:
            gspread.exceptions.WorksheetNotFound: Aba não encontrada
            gspread.exceptions.APIError: Erro ao ler dados
        """
        logger.info(
            "reading_sheet",
            sheet_name=sheet_name,
            range_notation=range_notation
        )
        
        try:
            spreadsheet = self._get_spreadsheet()
            worksheet = spreadsheet.worksheet(sheet_name)
            
            if range_notation:
                data = worksheet.get(range_notation)
            else:
                data = worksheet.get_all_values()
            
            logger.info(
                "sheet_read_successfully",
                sheet_name=sheet_name,
                rows_read=len(data)
            )
            
            return data
        
        except gspread.exceptions.WorksheetNotFound:
            logger.error("worksheet_not_found", sheet_name=sheet_name)
            raise
        
        except gspread.exceptions.APIError as e:
            logger.error(
                "read_failed",
                sheet_name=sheet_name,
                error=str(e)
            )
            raise
        
        except Exception as e:
            logger.error(
                "unexpected_read_error",
                sheet_name=sheet_name,
                error=str(e),
                error_type=type(e).__name__
            )
            raise
    
    def write_fact_series(
        self,
        series_id: str,
        data: pd.DataFrame,
        exec_id: str
    ) -> None:
        """
        Escreve dados de séries temporais na aba fact_series.
        
        Args:
            series_id: Identificador da série (ex: "ipca", "selic")
            data: DataFrame com colunas 'data_referencia' e 'valor'
            exec_id: ID da execução para rastreamento
        
        Raises:
            ValueError: Se DataFrame não tiver colunas necessárias
            gspread.exceptions.APIError: Erro ao escrever dados
        """
        required_cols = {"data_referencia", "valor"}
        if not required_cols.issubset(data.columns):
            missing = required_cols - set(data.columns)
            raise ValueError(
                f"DataFrame deve conter colunas {required_cols}. Faltando: {missing}"
            )
        
        logger.info(
            "writing_fact_series",
            series_id=series_id,
            exec_id=exec_id,
            rows=len(data)
        )
        
        # Criar cópia para não modificar original
        df = data.copy()
        
        # Adicionar colunas de metadados
        df["id_fato"] = [f"{series_id}_{row['data_referencia']}" for _, row in df.iterrows()]
        df["series_id"] = series_id
        df["fonte_original"] = "bcb_sgs"
        df["created_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Calcular variações se houver dados suficientes
        df = df.sort_values("data_referencia")
        df["variacao_mom"] = df["valor"].pct_change() * 100  # Month-over-month %
        df["variacao_yoy"] = df["valor"].pct_change(periods=12) * 100  # Year-over-year %
        
        # Reordenar colunas
        columns_order = [
            "id_fato",
            "series_id",
            "data_referencia",
            "valor",
            "variacao_mom",
            "variacao_yoy",
            "fonte_original",
            "created_at"
        ]
        df = df[columns_order]
        
        # Converter para lista de listas
        headers = [columns_order]
        rows = df.values.tolist()
        
        # Converter NaN para string vazia
        rows = [
            ['' if pd.isna(val) else val for val in row]
            for row in rows
        ]
        
        data_to_write = headers + rows
        
        try:
            # Criar aba se não existir
            self.create_sheet_if_not_exists("fact_series", headers=columns_order)
            
            # Adicionar dados
            self.append_to_sheet("fact_series", rows)
            
            logger.info(
                "fact_series_written",
                series_id=series_id,
                exec_id=exec_id,
                rows_written=len(rows)
            )
        
        except Exception as e:
            logger.error(
                "write_fact_series_failed",
                series_id=series_id,
                exec_id=exec_id,
                error=str(e),
                error_type=type(e).__name__
            )
            raise
    
    def write_ingestion_log(
        self,
        exec_id: str,
        fonte: str,
        status: str,
        linhas: int,
        erros: Optional[List[str]] = None
    ) -> None:
        """
        Registra log de ingestão na aba _ingestion_log.
        
        Args:
            exec_id: ID da execução
            fonte: Nome da fonte de dados (ex: "bcb_ipca")
            status: Status da execução ("success", "error", "partial")
            linhas: Número de linhas processadas
            erros: Lista de mensagens de erro (opcional)
        
        Raises:
            gspread.exceptions.APIError: Erro ao escrever log
        """
        logger.info(
            "writing_ingestion_log",
            exec_id=exec_id,
            fonte=fonte,
            status=status,
            linhas=linhas
        )
        
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        erros_str = "; ".join(erros) if erros else ""
        
        log_row = [
            exec_id,
            timestamp,
            fonte,
            status,
            linhas,
            erros_str
        ]
        
        try:
            # Criar aba se não existir
            headers = [
                "exec_id",
                "timestamp",
                "fonte",
                "status",
                "linhas_processadas",
                "erros"
            ]
            self.create_sheet_if_not_exists("_ingestion_log", headers=headers)
            
            # Adicionar log
            self.append_to_sheet("_ingestion_log", [log_row])
            
            logger.info(
                "ingestion_log_written",
                exec_id=exec_id,
                fonte=fonte,
                status=status
            )
        
        except Exception as e:
            logger.error(
                "write_ingestion_log_failed",
                exec_id=exec_id,
                fonte=fonte,
                error=str(e),
                error_type=type(e).__name__
            )
            raise
