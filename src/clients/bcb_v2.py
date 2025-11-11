"""
Cliente para API do Banco Central do Brasil (SGS - Sistema Gerenciador de Séries Temporais).

Fonte: https://www3.bcb.gov.br/sgspub
Licença: Dados públicos
"""

import requests
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, Optional
import structlog

logger = structlog.get_logger(__name__)


class BCBClient:
    """
    Cliente para buscar séries temporais do BCB.
    
    Séries disponíveis:
    - 432: Taxa Selic
    - 226: Taxa Referencial (TR)
    - 1: Taxa de Câmbio (USD/BRL)
    - 433: IPCA
    - 189: IGP-M
    - 7478: Rendimento Poupança
    - 4189: INPC
    - 4390: Crédito Pessoa Física
    - 1207: Produção Construção Civil
    - 24364: Estoque Crédito Habitacional
    """
    
    BASE_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{series_id}/dados"
    
    # Mapeamento de séries (código → nome descritivo)
    SERIES_NAMES = {
        "432": "Taxa Selic (% a.a.)",
        "226": "Taxa Referencial - TR (% a.m.)",
        "1": "Taxa de Câmbio - USD/BRL (R$/US$)",
        "433": "IPCA - Inflação (% m/m)",
        "189": "IGP-M - Inflação (% m/m)",
        "7478": "Rendimento da Poupança (% a.m.)",
        "4189": "INPC - Inflação (% m/m)",
        "4390": "Crédito Pessoa Física (R$ milhões)",
        "1207": "Produção da Indústria da Construção (índice)",
        "24364": "Estoque de Crédito Habitacional (R$ bilhões)"
    }
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Centro-Inteligencia-Construcao-Civil/1.0"
        })
        
        logger.info("bcb_client_initialized")
    
    def get_series(
        self,
        series_id: str,
        data_inicio: str = "01/01/2018",
        data_fim: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Busca série temporal do BCB.
        
        Args:
            series_id: Código da série (ex: "432" para Selic)
            data_inicio: Data inicial no formato DD/MM/YYYY
            data_fim: Data final (se None, usa data atual)
        
        Returns:
            DataFrame com colunas:
            - data: datetime
            - valor: float
            - series_id: str
            - nome_indicador: str
        """
        if not data_fim:
            # Usar data ATUAL: 2025-11-11
            data_fim = "11/11/2025"
        
        url = self.BASE_URL.format(series_id=series_id)
        
        params = {
            "formato": "json",
            "dataInicial": data_inicio,
            "dataFinal": data_fim
        }
        
        logger.info(
            "fetching_series",
            series_id=series_id,
            data_inicio=data_inicio,
            data_fim=data_fim
        )
        
        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            if not data:
                logger.warning("empty_response", series_id=series_id)
                return pd.DataFrame()
            
            # Converter para DataFrame
            df = pd.DataFrame(data)
            
            # Converter data (formato: "DD/MM/YYYY")
            df['data'] = pd.to_datetime(df['data'], format="%d/%m/%Y")
            
            # Converter valor
            df['valor'] = pd.to_numeric(df['valor'], errors='coerce')
            
            # Remover NaN
            df = df.dropna(subset=['valor'])
            
            # Adicionar metadados
            df['series_id'] = f"BCB_SGS_{series_id}"
            df['nome_indicador'] = self.SERIES_NAMES.get(
                series_id,
                f"Série BCB {series_id}"
            )
            
            # Validação: não permitir datas futuras (> hoje)
            hoje = datetime(2025, 11, 11)  # Data atual
            df = df[df['data'] <= hoje]
            
            # Validação: remover valores absurdos (> 1 milhão ou < -1000)
            df = df[(df['valor'] < 1_000_000) & (df['valor'] > -1000)]
            
            logger.info(
                "series_fetched",
                series_id=series_id,
                rows=len(df),
                date_range=f"{df['data'].min().date()} até {df['data'].max().date()}" if len(df) > 0 else "vazio",
                value_range=f"{df['valor'].min():.2f} - {df['valor'].max():.2f}" if len(df) > 0 else "N/A"
            )
            
            return df
        
        except requests.exceptions.RequestException as e:
            logger.error(
                "api_request_failed",
                series_id=series_id,
                error=str(e)
            )
            return pd.DataFrame()
        
        except Exception as e:
            logger.error(
                "unexpected_error",
                series_id=series_id,
                error=str(e),
                exc_info=True
            )
            return pd.DataFrame()
    
    def get_all_series(
        self,
        data_inicio: str = "01/01/2018",
        data_fim: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Busca TODAS as séries configuradas.
        
        Returns:
            DataFrame consolidado com todas as séries
        """
        all_data = []
        
        for series_id in self.SERIES_NAMES.keys():
            logger.info("processing_series", series_id=series_id)
            
            df = self.get_series(series_id, data_inicio, data_fim)
            
            if not df.empty:
                all_data.append(df)
            else:
                logger.warning("series_returned_empty", series_id=series_id)
        
        if all_data:
            result = pd.concat(all_data, ignore_index=True)
            
            logger.info(
                "all_series_fetched",
                total_rows=len(result),
                unique_series=result['series_id'].nunique(),
                date_range=f"{result['data'].min().date()} até {result['data'].max().date()}"
            )
            
            return result
        else:
            logger.error("no_series_data_fetched")
            return pd.DataFrame()
    
    def calculate_variations(self, df: pd.DataFrame, frequencia: str) -> pd.DataFrame:
        """
        Calcula variações diária, mensal e anual.
        
        Args:
            df: DataFrame com colunas 'data' e 'valor'
            frequencia: 'diaria' ou 'mensal'
        
        Returns:
            DataFrame com colunas de variação adicionadas
        """
        df = df.sort_values('data').copy()
        
        # Variação diária (apenas para séries diárias)
        if frequencia == 'diaria':
            df['variacao_diaria'] = df['valor'].pct_change() * 100
        else:
            df['variacao_diaria'] = None
        
        # Variação mensal
        if frequencia == 'diaria':
            # Para séries diárias, comparar com 30 dias atrás
            df['variacao_mensal'] = df['valor'].pct_change(periods=30) * 100
        else:
            # Para séries mensais, comparar com 1 mês atrás
            df['variacao_mensal'] = df['valor'].pct_change(periods=1) * 100
        
        # Variação anual (YoY)
        if frequencia == 'diaria':
            # Para séries diárias, comparar com ~252 dias úteis
            df['variacao_anual'] = df['valor'].pct_change(periods=252) * 100
        else:
            # Para séries mensais, comparar com 12 meses atrás
            df['variacao_anual'] = df['valor'].pct_change(periods=12) * 100
        
        # Substituir inf por None
        df['variacao_diaria'] = df['variacao_diaria'].replace([float('inf'), float('-inf')], None)
        df['variacao_mensal'] = df['variacao_mensal'].replace([float('inf'), float('-inf')], None)
        df['variacao_anual'] = df['variacao_anual'].replace([float('inf'), float('-inf')], None)
        
        return df
