"""
Cliente para dados do SINAPI - Sistema Nacional de Pesquisa de Custos e Índices da Construção Civil.

FONTE OFICIAL: Caixa Econômica Federal / IBGE
- Portal: https://www.caixa.gov.br/poder-publico/modernizacao-gestao/sinapi/
- IBGE: https://www.ibge.gov.br/estatisticas/economicas/precos-e-custos/9270-sistema-nacional-de-pesquisa-de-custos-e-indices-da-construcao-civil.html

ESTRUTURA DOS DADOS:
- Tabelas de preços de insumos e composições
- Atualizadas mensalmente por UF
- Formato: Excel/CSV com layout padronizado

CATEGORIAS DE MATERIAIS:
- Agregados (areia, brita, etc.)
- Cimento e argamassas
- Aço e metais
- Cerâmicas e revestimentos
- Tubos e conexões
- Materiais elétricos
- Tintas e vernizes
- Madeiras

Autor: Pipeline de Dados
Data: 2026-01-28
"""

import pandas as pd
import requests
from typing import Dict, List, Optional
from datetime import datetime
import structlog
import re

logger = structlog.get_logger(__name__)


class SINAPIClient:
    """
    Cliente para processar dados do SINAPI.
    
    O SINAPI é a referência oficial para custos de construção civil
    em obras públicas no Brasil. Fornece preços de insumos e 
    composições de serviços.
    """
    
    # Categorias de materiais do SINAPI
    CATEGORIAS_MATERIAIS = {
        'ARGA': 'Argamassas',
        'CIME': 'Cimento',
        'AGRE': 'Agregados',
        'ACO': 'Aço e Ferro',
        'CERA': 'Cerâmicas',
        'TUBO': 'Tubos e Conexões',
        'ELET': 'Materiais Elétricos',
        'TINT': 'Tintas e Vernizes',
        'MADE': 'Madeiras',
        'IMPE': 'Impermeabilizantes',
        'VIDR': 'Vidros',
        'ESQU': 'Esquadrias'
    }
    
    # Mapeamento de UFs
    UF_SIGLAS = [
        'AC', 'AL', 'AM', 'AP', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA',
        'MG', 'MS', 'MT', 'PA', 'PB', 'PE', 'PI', 'PR', 'RJ', 'RN',
        'RO', 'RR', 'RS', 'SC', 'SE', 'SP', 'TO'
    ]
    
    # Materiais principais para construção
    MATERIAIS_PRINCIPAIS = {
        'CIMENTO_CP_II_50KG': {
            'codigo_sinapi': '00000370',
            'unidade': 'KG',
            'descricao': 'Cimento Portland CP II-32, saco 50kg'
        },
        'AREIA_MEDIA': {
            'codigo_sinapi': '00000367',
            'unidade': 'M3',
            'descricao': 'Areia média - posto obra'
        },
        'BRITA_1': {
            'codigo_sinapi': '00000369',
            'unidade': 'M3',
            'descricao': 'Pedra britada nº 1'
        },
        'ACO_CA50_10MM': {
            'codigo_sinapi': '00000043',
            'unidade': 'KG',
            'descricao': 'Aço CA-50, 10mm, vergalhão'
        },
        'TIJOLO_CERAMICO': {
            'codigo_sinapi': '00000557',
            'unidade': 'UN',
            'descricao': 'Tijolo cerâmico furado'
        },
        'CONCRETO_FCK25': {
            'codigo_sinapi': '00034494',
            'unidade': 'M3',
            'descricao': 'Concreto usinado fck=25MPa'
        }
    }
    
    def __init__(self):
        self.base_url = "https://www.caixa.gov.br/Downloads/sinapi/"
    
    def process_sinapi_file(
        self,
        filepath: str,
        uf: str = None,
        mes_ref: str = None
    ) -> pd.DataFrame:
        """
        Processa arquivo do SINAPI (Excel ou CSV).
        
        O usuário baixa o arquivo do portal da Caixa e este método
        faz o parsing e transformação para o schema do DW.
        
        Args:
            filepath: Caminho do arquivo SINAPI baixado
            uf: UF de referência (se não estiver no arquivo)
            mes_ref: Mês de referência AAAA-MM
            
        Returns:
            DataFrame no schema fact_materiais
        """
        logger.info("Processando arquivo SINAPI", filepath=filepath)
        
        # Lê arquivo
        df = self._read_sinapi_file(filepath)
        
        if df.empty:
            logger.warning("Arquivo SINAPI vazio ou formato não reconhecido")
            return pd.DataFrame()
        
        # Normaliza colunas
        df = self._normalize_columns(df)
        
        # Filtra materiais (exclui mão de obra e serviços)
        df = self._filter_materiais(df)
        
        # Transforma para schema
        result = self._transform_to_schema(df, uf, mes_ref)
        
        logger.info(f"SINAPI processado: {len(result)} registros")
        return result
    
    def _read_sinapi_file(self, filepath: str) -> pd.DataFrame:
        """Lê arquivo SINAPI detectando formato."""
        try:
            if filepath.endswith(('.xlsx', '.xls')):
                # Tenta diferentes abas (padrão SINAPI varia)
                try:
                    df = pd.read_excel(filepath, sheet_name='INSUMOS')
                except:
                    try:
                        df = pd.read_excel(filepath, sheet_name=0)
                    except:
                        df = pd.read_excel(filepath)
            else:
                df = pd.read_csv(filepath, sep=';', encoding='latin-1')
            
            return df
            
        except Exception as e:
            logger.error(f"Erro ao ler arquivo SINAPI: {e}")
            return pd.DataFrame()
    
    def _normalize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normaliza nomes de colunas."""
        col_map = {
            'código': 'codigo',
            'codigo': 'codigo',
            'código_sinapi': 'codigo',
            'descrição': 'descricao',
            'descricao': 'descricao',
            'descriçãodoinsumo': 'descricao',
            'unidade': 'unidade',
            'un': 'unidade',
            'preço': 'preco',
            'preco': 'preco',
            'preçomediano': 'preco',
            'valor': 'preco',
            'tipo': 'tipo_insumo',
            'classe': 'classe',
            'origem': 'origem'
        }
        
        df.columns = df.columns.str.lower().str.strip().str.replace(' ', '')
        df = df.rename(columns=col_map)
        
        return df
    
    def _filter_materiais(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filtra apenas materiais (exclui mão de obra)."""
        # Tipos de insumo do SINAPI:
        # MAT = Material
        # MO = Mão de obra
        # EQ = Equipamento
        # SER = Serviço
        
        if 'tipo_insumo' in df.columns:
            return df[df['tipo_insumo'].str.upper().isin(['MAT', 'MATERIAL', 'INSUMO'])]
        
        if 'classe' in df.columns:
            # Exclui classes de mão de obra
            mao_obra = ['SERVENTE', 'PEDREIRO', 'ELETRICISTA', 'ENCANADOR', 'PINTOR']
            mask = ~df['classe'].str.upper().str.contains('|'.join(mao_obra), na=False)
            return df[mask]
        
        return df
    
    def _transform_to_schema(
        self, 
        df: pd.DataFrame, 
        uf: str = None,
        mes_ref: str = None
    ) -> pd.DataFrame:
        """Transforma para schema fact_materiais."""
        
        # Detecta UF do arquivo se não informada
        if uf is None:
            # Tenta extrair do nome de coluna ou valor
            for col in df.columns:
                for sigla in self.UF_SIGLAS:
                    if sigla in col.upper():
                        uf = sigla
                        break
            if uf is None:
                uf = 'BR'  # Nacional
        
        # Detecta mês de referência
        if mes_ref is None:
            mes_ref = datetime.now().strftime('%Y-%m')
        
        # Monta schema final
        output = pd.DataFrame()
        output['id_fato'] = range(1, len(df) + 1)
        output['material'] = df['descricao'] if 'descricao' in df.columns else df['codigo']
        output['regiao'] = uf
        output['data_referencia'] = f"{mes_ref}-01"
        
        # Campos adicionais
        if 'codigo' in df.columns:
            output['codigo_sinapi'] = df['codigo']
        if 'preco' in df.columns:
            output['preco_unitario'] = pd.to_numeric(
                df['preco'].astype(str).str.replace(',', '.'), 
                errors='coerce'
            )
        if 'unidade' in df.columns:
            output['unidade'] = df['unidade']
        
        output['fonte'] = 'SINAPI/CAIXA'
        
        return output
    
    def get_materiais_principais_preco(self, uf: str = 'SP') -> pd.DataFrame:
        """
        Retorna preços de referência dos materiais principais.
        
        Dados de referência baseados no SINAPI (valores aproximados).
        Em produção, usar dados reais do portal da Caixa.
        """
        # Preços médios de referência (2024-2025)
        precos_ref = {
            'SP': {'cimento': 0.72, 'areia': 120, 'brita': 95, 'aco': 6.80, 'tijolo': 0.85, 'concreto': 450},
            'RJ': {'cimento': 0.78, 'areia': 135, 'brita': 105, 'aco': 7.10, 'tijolo': 0.92, 'concreto': 480},
            'MG': {'cimento': 0.68, 'areia': 110, 'brita': 90, 'aco': 6.50, 'tijolo': 0.78, 'concreto': 420},
            'RS': {'cimento': 0.75, 'areia': 125, 'brita': 98, 'aco': 6.90, 'tijolo': 0.88, 'concreto': 460},
            'PR': {'cimento': 0.70, 'areia': 115, 'brita': 92, 'aco': 6.60, 'tijolo': 0.82, 'concreto': 440},
        }
        
        # Default para outras UFs
        default = {'cimento': 0.73, 'areia': 120, 'brita': 95, 'aco': 6.75, 'tijolo': 0.85, 'concreto': 450}
        precos = precos_ref.get(uf, default)
        
        data = [
            {'material': 'Cimento CP II 50kg', 'preco': precos['cimento'], 'unidade': 'KG'},
            {'material': 'Areia média m³', 'preco': precos['areia'], 'unidade': 'M3'},
            {'material': 'Brita nº1 m³', 'preco': precos['brita'], 'unidade': 'M3'},
            {'material': 'Aço CA-50 10mm', 'preco': precos['aco'], 'unidade': 'KG'},
            {'material': 'Tijolo cerâmico', 'preco': precos['tijolo'], 'unidade': 'UN'},
            {'material': 'Concreto fck25 usinado', 'preco': precos['concreto'], 'unidade': 'M3'},
        ]
        
        return pd.DataFrame(data)


def create_sample_sinapi_data() -> pd.DataFrame:
    """
    Cria dados de exemplo do SINAPI para teste.
    
    Em produção, usar dados reais do portal da Caixa.
    """
    import numpy as np
    
    materiais = [
        ('Cimento Portland CP II-32 50kg', 'KG'),
        ('Areia média lavada', 'M3'),
        ('Pedra britada nº 1', 'M3'),
        ('Aço CA-50 10.0mm vergalhão', 'KG'),
        ('Tijolo cerâmico furado 9x19x19cm', 'UN'),
        ('Concreto usinado fck=25MPa', 'M3'),
        ('Argamassa colante AC-I', 'KG'),
        ('Tubo PVC esgoto 100mm', 'M'),
        ('Fio elétrico 2.5mm²', 'M'),
        ('Tinta látex PVA branca', 'L'),
        ('Impermeabilizante acrílico', 'L'),
        ('Telha cerâmica colonial', 'UN'),
    ]
    
    ufs = ['SP', 'RJ', 'MG', 'RS', 'PR', 'SC', 'BA', 'PE', 'CE', 'GO']
    meses = pd.date_range('2024-01-01', '2025-12-01', freq='MS')
    
    records = []
    for uf in ufs:
        for mes in meses:
            # Fator regional
            fator_uf = {'SP': 1.0, 'RJ': 1.08, 'MG': 0.92, 'RS': 1.02, 'PR': 0.95}.get(uf, 1.0)
            
            for mat, unid in materiais:
                # Preço base com variação
                base_price = {
                    'Cimento': 0.72, 'Areia': 120, 'Pedra': 95, 'Aço': 6.80,
                    'Tijolo': 0.85, 'Concreto': 450, 'Argamassa': 1.20,
                    'Tubo': 15.50, 'Fio': 2.80, 'Tinta': 18.90,
                    'Impermeabilizante': 22.50, 'Telha': 2.30
                }
                
                # Encontra preço base
                preco_base = 10.0
                for key, val in base_price.items():
                    if key in mat:
                        preco_base = val
                        break
                
                # Aplica fatores
                inflacao = 1 + (mes.month - 1) * 0.005  # ~0.5% ao mês
                variacao = np.random.uniform(0.95, 1.05)
                preco_final = round(preco_base * fator_uf * inflacao * variacao, 2)
                
                records.append({
                    'id_fato': len(records) + 1,
                    'material': mat,
                    'regiao': uf,
                    'data_referencia': mes.strftime('%Y-%m-%d'),
                    'preco_unitario': preco_final,
                    'unidade': unid,
                    'fonte': 'SINAPI/CAIXA'
                })
    
    return pd.DataFrame(records)


if __name__ == "__main__":
    # Exemplo de uso
    client = SINAPIClient()
    
    # Para processar arquivo real:
    # df = client.process_sinapi_file("caminho/para/SINAPI_Preco_Ref_Insumos_SP_202412.xlsx", uf='SP')
    
    # Para teste com dados simulados:
    df = create_sample_sinapi_data()
    print(df.head(20))
    print(f"\nTotal: {len(df)} registros")
    print(f"UFs: {df['regiao'].nunique()}")
    print(f"Materiais: {df['material'].nunique()}")
