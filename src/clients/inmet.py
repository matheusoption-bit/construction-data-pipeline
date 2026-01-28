"""
Cliente para dados meteorológicos do INMET - Instituto Nacional de Meteorologia.

FONTE OFICIAL: INMET / Ministério da Agricultura
- Portal: https://portal.inmet.gov.br/
- API: https://apitempo.inmet.gov.br/
- Dados Históricos: https://bdmep.inmet.gov.br/

ESTAÇÕES METEOROLÓGICAS:
- Convencionais: ~400 estações
- Automáticas: ~600 estações
- Cobertura nacional

VARIÁVEIS DISPONÍVEIS:
- Precipitação (mm)
- Temperatura (°C)
- Umidade relativa (%)
- Vento (m/s, direção)
- Pressão atmosférica (hPa)
- Radiação solar (W/m²)

RELEVÂNCIA PARA CONSTRUÇÃO:
- Dias de chuva afetam cronograma de obras
- Umidade impacta cura de concreto
- Vento forte paralisa trabalhos em altura

Autor: Pipeline de Dados
Data: 2026-01-28
"""

import pandas as pd
import requests
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import structlog

logger = structlog.get_logger(__name__)


class INMETClient:
    """
    Cliente para API do INMET.
    
    Fornece dados meteorológicos históricos e em tempo real
    para análise de impacto climático na construção civil.
    """
    
    BASE_URL = "https://apitempo.inmet.gov.br/"
    
    # Estações principais por capital (código INMET)
    ESTACOES_CAPITAIS = {
        'SP': {'codigo': 'A701', 'nome': 'São Paulo - Mirante', 'lat': -23.496, 'lon': -46.620},
        'RJ': {'codigo': 'A652', 'nome': 'Rio de Janeiro - Forte Copacabana', 'lat': -22.988, 'lon': -43.190},
        'MG': {'codigo': 'A521', 'nome': 'Belo Horizonte - Pampulha', 'lat': -19.883, 'lon': -43.969},
        'RS': {'codigo': 'A801', 'nome': 'Porto Alegre', 'lat': -30.054, 'lon': -51.175},
        'PR': {'codigo': 'A807', 'nome': 'Curitiba', 'lat': -25.449, 'lon': -49.231},
        'SC': {'codigo': 'A806', 'nome': 'Florianópolis', 'lat': -27.580, 'lon': -48.566},
        'BA': {'codigo': 'A401', 'nome': 'Salvador - Ondina', 'lat': -13.005, 'lon': -38.509},
        'PE': {'codigo': 'A301', 'nome': 'Recife - Curado', 'lat': -8.059, 'lon': -34.959},
        'CE': {'codigo': 'A305', 'nome': 'Fortaleza', 'lat': -3.815, 'lon': -38.538},
        'GO': {'codigo': 'A002', 'nome': 'Goiânia', 'lat': -16.643, 'lon': -49.220},
        'DF': {'codigo': 'A001', 'nome': 'Brasília', 'lat': -15.789, 'lon': -47.926},
        'AM': {'codigo': 'A101', 'nome': 'Manaus', 'lat': -3.103, 'lon': -60.016},
        'PA': {'codigo': 'A201', 'nome': 'Belém', 'lat': -1.411, 'lon': -48.439},
    }
    
    def __init__(self, token: str = None):
        """
        Inicializa cliente INMET.
        
        Args:
            token: Token de autenticação (opcional para alguns endpoints)
        """
        self.token = token
        self.session = requests.Session()
    
    def get_estacoes_all(self) -> pd.DataFrame:
        """
        Lista todas as estações meteorológicas.
        
        Returns:
            DataFrame com estações
        """
        url = f"{self.BASE_URL}/estacoes/T"
        
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            df = pd.DataFrame(data)
            return df
            
        except Exception as e:
            logger.error(f"Erro ao buscar estações: {e}")
            return pd.DataFrame()
    
    def get_dados_estacao(
        self,
        codigo_estacao: str,
        data_inicio: str,
        data_fim: str = None
    ) -> pd.DataFrame:
        """
        Busca dados históricos de uma estação.
        
        Args:
            codigo_estacao: Código da estação INMET
            data_inicio: Data inicial (YYYY-MM-DD)
            data_fim: Data final (YYYY-MM-DD, default=hoje)
            
        Returns:
            DataFrame com dados meteorológicos
        """
        if data_fim is None:
            data_fim = datetime.now().strftime('%Y-%m-%d')
        
        url = f"{self.BASE_URL}/estacao/{data_inicio}/{data_fim}/{codigo_estacao}"
        
        try:
            response = self.session.get(url, timeout=60)
            response.raise_for_status()
            data = response.json()
            
            if not data:
                return pd.DataFrame()
            
            df = pd.DataFrame(data)
            return self._normalize_dados(df)
            
        except Exception as e:
            logger.error(f"Erro ao buscar dados estação {codigo_estacao}: {e}")
            return pd.DataFrame()
    
    def _normalize_dados(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normaliza dados do INMET para schema padrão."""
        col_map = {
            'DC_NOME': 'nome_estacao',
            'CD_ESTACAO': 'codigo_estacao',
            'DT_MEDICAO': 'data_referencia',
            'HR_MEDICAO': 'hora',
            'CHUVA': 'precipitacao_mm',
            'TEM_INS': 'temperatura_inst',
            'TEM_MAX': 'temperatura_max',
            'TEM_MIN': 'temperatura_min',
            'UMD_INS': 'umidade_inst',
            'UMD_MAX': 'umidade_max',
            'UMD_MIN': 'umidade_min',
            'VEN_VEL': 'vento_velocidade',
            'VEN_DIR': 'vento_direcao',
            'PRE_INS': 'pressao_inst',
            'RAD_GLO': 'radiacao_global',
        }
        
        df = df.rename(columns=col_map)
        
        # Converte tipos
        numeric_cols = ['precipitacao_mm', 'temperatura_inst', 'temperatura_max', 
                       'temperatura_min', 'umidade_inst', 'vento_velocidade']
        
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        return df
    
    def get_dim_clima_data(self) -> pd.DataFrame:
        """
        Retorna dados para tabela dim_clima.
        
        Returns:
            DataFrame no schema dim_clima
        """
        records = []
        
        for uf, info in self.ESTACOES_CAPITAIS.items():
            records.append({
                'id_clima': len(records) + 1,
                'id_cidade': None,  # Precisa mapear para dim_cidade
                'nome_estacao': info['nome'],
                'codigo_estacao': info['codigo'],
                'uf': uf,
                'latitude': info['lat'],
                'longitude': info['lon'],
                'tipo_estacao': 'AUTOMATICA',
                'fonte': 'INMET'
            })
        
        return pd.DataFrame(records)


def create_dim_clima_data() -> pd.DataFrame:
    """
    Cria dados dimensionais de estações climáticas.
    """
    client = INMETClient()
    return client.get_dim_clima_data()


def create_fact_clima_sample() -> pd.DataFrame:
    """
    Cria dados de exemplo para fact_clima.
    
    Em produção, usar API do INMET ou dados históricos.
    """
    import numpy as np
    
    estacoes = [
        ('A701', 'São Paulo'),
        ('A652', 'Rio de Janeiro'),
        ('A521', 'Belo Horizonte'),
        ('A801', 'Porto Alegre'),
        ('A807', 'Curitiba'),
    ]
    
    # 12 meses de dados
    meses = pd.date_range('2024-01-01', '2024-12-01', freq='MS')
    
    records = []
    for codigo, nome in estacoes:
        for mes in meses:
            # Precipitação varia por estação (verão mais chuvoso)
            precip_base = 150 if mes.month in [1, 2, 3, 11, 12] else 50
            precip = max(0, precip_base + np.random.normal(0, 40))
            
            # Dias de chuva
            dias_chuva = int(precip / 15) + np.random.randint(0, 5)
            dias_chuva = min(dias_chuva, 28)
            
            records.append({
                'id_fato': len(records) + 1,
                'cod_estacao_inmet': codigo,
                'data_referencia': mes.strftime('%Y-%m-%d'),
                'precipitacao_mm': round(precip, 1),
                'dias_com_chuva': dias_chuva,
                'temperatura_media': round(22 + np.random.uniform(-5, 5), 1),
                'umidade_media': round(70 + np.random.uniform(-15, 15), 1),
                'fonte': 'INMET'
            })
    
    return pd.DataFrame(records)


if __name__ == "__main__":
    # Exemplo de uso
    print("=== ESTAÇÕES CLIMÁTICAS (dim_clima) ===")
    df_dim = create_dim_clima_data()
    print(df_dim.to_string())
    
    print("\n\n=== DADOS CLIMÁTICOS (fact_clima) ===")
    df_fact = create_fact_clima_sample()
    print(df_fact.head(20).to_string())
