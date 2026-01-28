"""
Cliente para dados do IBGE via API SIDRA e malhas territoriais.

FONTE OFICIAL: Instituto Brasileiro de Geografia e Estatística
- SIDRA: https://sidra.ibge.gov.br/
- API: https://servicodados.ibge.gov.br/api/docs
- Malhas: https://servicodados.ibge.gov.br/api/v3/malhas/

DADOS DISPONÍVEIS:
- Tabelas econômicas e sociais
- Malhas territoriais (UF, município, bairro)
- Coordenadas geográficas
- Códigos IBGE

TABELAS RELEVANTES PARA CONSTRUÇÃO:
- 6579: PIB dos municípios
- 1620: Índices de preços
- 4090: IPCA por componentes
- 6691: Cadastro de empresas

Autor: Pipeline de Dados
Data: 2026-01-28
"""

import pandas as pd
import requests
from typing import Dict, List, Optional
import structlog

try:
    import sidrapy
    SIDRAPY_AVAILABLE = True
except ImportError:
    SIDRAPY_AVAILABLE = False

logger = structlog.get_logger(__name__)


class IBGEClient:
    """
    Cliente para API do IBGE (SIDRA e malhas territoriais).
    
    Fornece acesso a dados estatísticos e geográficos do IBGE.
    """
    
    API_LOCALIDADES = "https://servicodados.ibge.gov.br/api/v1/localidades"
    API_MALHAS = "https://servicodados.ibge.gov.br/api/v3/malhas"
    API_SIDRA = "https://apisidra.ibge.gov.br/values"
    
    # Mapeamento de tabelas SIDRA relevantes
    TABELAS_SIDRA = {
        'PIB_MUNICIPIOS': {
            'tabela': 6579,
            'variavel': '37',
            'descricao': 'PIB a preços correntes por município'
        },
        'IPCA_GERAL': {
            'tabela': 1419,
            'variavel': '63',
            'descricao': 'IPCA - Variação mensal'
        },
        'INCC_GERAL': {
            'tabela': 2296,
            'variavel': '355',
            'descricao': 'INCC - Variação mensal'
        },
        'POPULACAO': {
            'tabela': 6579,
            'variavel': '93',
            'descricao': 'População residente'
        },
        'EMPRESAS_CONSTRUCAO': {
            'tabela': 6691,
            'variavel': '630',
            'descricao': 'Número de empresas - Seção F (Construção)'
        }
    }
    
    def __init__(self):
        self.session = requests.Session()
    
    def get_ufs(self) -> pd.DataFrame:
        """Retorna lista de UFs."""
        url = f"{self.API_LOCALIDADES}/estados"
        response = self.session.get(url)
        response.raise_for_status()
        return pd.DataFrame(response.json())
    
    def get_municipios(self, uf: str = None) -> pd.DataFrame:
        """
        Retorna lista de municípios.
        
        Args:
            uf: Sigla UF para filtrar (opcional)
        """
        if uf:
            url = f"{self.API_LOCALIDADES}/estados/{uf}/municipios"
        else:
            url = f"{self.API_LOCALIDADES}/municipios"
        
        response = self.session.get(url)
        response.raise_for_status()
        return pd.DataFrame(response.json())
    
    def get_distritos(self, municipio_id: int) -> pd.DataFrame:
        """Retorna distritos/bairros de um município."""
        url = f"{self.API_LOCALIDADES}/municipios/{municipio_id}/distritos"
        response = self.session.get(url)
        response.raise_for_status()
        return pd.DataFrame(response.json())
    
    def fetch_sidra_table(
        self,
        tabela: int,
        variavel: str,
        periodo: str = "last",
        nivel_territorial: str = "1",  # 1=Brasil, 2=UF, 3=Município
        localidades: str = "all"
    ) -> pd.DataFrame:
        """
        Busca dados do SIDRA.
        
        Args:
            tabela: Número da tabela SIDRA
            variavel: Código da variável
            periodo: Período (ex: "202401" ou "last")
            nivel_territorial: Nível geográfico
            localidades: Códigos das localidades
        """
        if SIDRAPY_AVAILABLE:
            try:
                df = sidrapy.get_table(
                    table_code=str(tabela),
                    territorial_level=nivel_territorial,
                    ibge_territorial_code=localidades,
                    variable=variavel,
                    period=periodo
                )
                return df
            except Exception as e:
                logger.error(f"Erro sidrapy: {e}")
        
        # Fallback para API direta
        url = f"{self.API_SIDRA}/t/{tabela}/n{nivel_territorial}/{localidades}/v/{variavel}/p/{periodo}"
        
        try:
            response = self.session.get(url, timeout=60)
            response.raise_for_status()
            data = response.json()
            
            if data:
                df = pd.DataFrame(data[1:], columns=data[0].values())
                return df
            
        except Exception as e:
            logger.error(f"Erro SIDRA: {e}")
        
        return pd.DataFrame()
    
    def get_map_sidra_data(self) -> pd.DataFrame:
        """
        Retorna mapeamento de tabelas SIDRA para DW.
        
        Returns:
            DataFrame no schema _map_sidra
        """
        records = []
        
        for nome, info in self.TABELAS_SIDRA.items():
            records.append({
                'tabela_sidra': info['tabela'],
                'variavel': info['variavel'],
                'nome_variavel': nome,
                'nivel_territorial': '1,2,3',  # Disponível em todos
                'descricao': info['descricao'],
                'url_documentacao': f"https://sidra.ibge.gov.br/tabela/{info['tabela']}"
            })
        
        return pd.DataFrame(records)


def create_dim_bairro_data(uf: str = 'SP', cidade: str = 'São Paulo') -> pd.DataFrame:
    """
    Cria dados dimensionais de bairros para uma cidade.
    
    Nota: O IBGE tem dados limitados de bairros. Para dados detalhados,
    usar Correios (CEP) ou prefeituras.
    """
    # Bairros principais de São Paulo (exemplo)
    bairros_sp = [
        {'nome': 'Consolação', 'cep_base': '01301'},
        {'nome': 'Jardim Paulista', 'cep_base': '01401'},
        {'nome': 'Pinheiros', 'cep_base': '05401'},
        {'nome': 'Vila Mariana', 'cep_base': '04101'},
        {'nome': 'Moema', 'cep_base': '04501'},
        {'nome': 'Itaim Bibi', 'cep_base': '04531'},
        {'nome': 'Brooklin', 'cep_base': '04561'},
        {'nome': 'Vila Olímpia', 'cep_base': '04551'},
        {'nome': 'Morumbi', 'cep_base': '05651'},
        {'nome': 'Butantã', 'cep_base': '05501'},
        {'nome': 'Perdizes', 'cep_base': '05001'},
        {'nome': 'Santana', 'cep_base': '02001'},
        {'nome': 'Tatuapé', 'cep_base': '03301'},
        {'nome': 'Mooca', 'cep_base': '03101'},
        {'nome': 'Ipiranga', 'cep_base': '04201'},
    ]
    
    records = []
    for i, b in enumerate(bairros_sp):
        records.append({
            'id_bairro': i + 1,
            'nome_bairro': b['nome'],
            'id_cidade': 3550308,  # Código IBGE São Paulo
            'codigo_postal_base': b['cep_base'],
            'uf': 'SP',
            'cidade': 'São Paulo'
        })
    
    return pd.DataFrame(records)


def create_dim_geo_data() -> pd.DataFrame:
    """
    Cria dados dimensionais geográficos.
    
    Coordenadas de bairros principais de SP.
    """
    # Coordenadas aproximadas de bairros SP
    geo_data = [
        {'bairro': 'Consolação', 'lat': -23.5519, 'lon': -46.6566},
        {'bairro': 'Jardim Paulista', 'lat': -23.5698, 'lon': -46.6561},
        {'bairro': 'Pinheiros', 'lat': -23.5617, 'lon': -46.6892},
        {'bairro': 'Vila Mariana', 'lat': -23.5895, 'lon': -46.6347},
        {'bairro': 'Moema', 'lat': -23.6034, 'lon': -46.6628},
        {'bairro': 'Itaim Bibi', 'lat': -23.5858, 'lon': -46.6755},
        {'bairro': 'Brooklin', 'lat': -23.6178, 'lon': -46.6897},
        {'bairro': 'Vila Olímpia', 'lat': -23.5964, 'lon': -46.6864},
        {'bairro': 'Morumbi', 'lat': -23.6000, 'lon': -46.7231},
        {'bairro': 'Butantã', 'lat': -23.5686, 'lon': -46.7313},
    ]
    
    records = []
    for i, g in enumerate(geo_data):
        records.append({
            'id_geo': i + 1,
            'id_bairro': i + 1,
            'latitude': g['lat'],
            'longitude': g['lon'],
            'altitude_m': None,
            'area_km2': None
        })
    
    return pd.DataFrame(records)


def create_map_sidra_data() -> pd.DataFrame:
    """Cria mapeamento de tabelas SIDRA."""
    client = IBGEClient()
    return client.get_map_sidra_data()


if __name__ == "__main__":
    # Exemplo de uso
    client = IBGEClient()
    
    print("=== MAPEAMENTO SIDRA ===")
    df_map = create_map_sidra_data()
    print(df_map.to_string())
    
    print("\n\n=== DIMENSÃO BAIRROS (SP) ===")
    df_bairro = create_dim_bairro_data()
    print(df_bairro.to_string())
    
    print("\n\n=== DIMENSÃO GEO ===")
    df_geo = create_dim_geo_data()
    print(df_geo.to_string())
