"""
Cliente para dados do Novo CAGED - Cadastro Geral de Empregados e Desempregados.

FONTE OFICIAL: Ministério do Trabalho e Emprego (MTE)
- Portal: https://www.gov.br/trabalho-e-emprego/pt-br/assuntos/estatisticas-trabalho/novo-caged
- FTP: ftp://ftp.mtps.gov.br/pdet/microdados/NOVO%20CAGED/

ESTRUTURA DOS DADOS:
- Microdados mensais em formato TXT (layout fixo) ou CSV
- Contém admissões, desligamentos e saldo por município/setor

SETOR CONSTRUÇÃO CIVIL:
- CNAE 2.0: Seção F (41, 42, 43)
  - 41: Construção de edifícios
  - 42: Obras de infraestrutura
  - 43: Serviços especializados para construção

Autor: Pipeline de Dados
Data: 2026-01-28
"""

import pandas as pd
import requests
import io
from typing import Dict, List, Optional
from datetime import datetime
import structlog

logger = structlog.get_logger(__name__)


class CAGEDClient:
    """
    Cliente para processar dados do Novo CAGED.
    
    O CAGED é a principal fonte de dados de emprego formal no Brasil,
    com dados mensais de admissões e desligamentos por setor econômico.
    """
    
    # CNAEs da Construção Civil (Seção F)
    CNAE_CONSTRUCAO = {
        '41': 'Construção de Edifícios',
        '42': 'Obras de Infraestrutura', 
        '43': 'Serviços Especializados'
    }
    
    # Mapeamento UF código -> sigla
    UF_CODIGO = {
        11: 'RO', 12: 'AC', 13: 'AM', 14: 'RR', 15: 'PA', 16: 'AP', 17: 'TO',
        21: 'MA', 22: 'PI', 23: 'CE', 24: 'RN', 25: 'PB', 26: 'PE', 27: 'AL',
        28: 'SE', 29: 'BA', 31: 'MG', 32: 'ES', 33: 'RJ', 35: 'SP', 41: 'PR',
        42: 'SC', 43: 'RS', 50: 'MS', 51: 'MT', 52: 'GO', 53: 'DF'
    }
    
    # Layout das colunas do arquivo CAGED (posições principais)
    COLUNAS_CAGED = [
        'competencia',      # AAAAMM
        'uf',               # Código UF
        'municipio',        # Código IBGE município
        'secao_cnae',       # Seção CNAE (letra)
        'subclasse_cnae',   # CNAE 2.0 completo
        'admitidos',        # Quantidade admitidos
        'desligados',       # Quantidade desligados
        'saldo',            # Saldo (adm - desl)
        'salario_medio',    # Salário médio admitidos
        'grau_instrucao',   # Escolaridade
        'idade',            # Faixa etária
        'sexo',             # 1=Masc, 2=Fem
        'tipo_estabelec',   # Tipo estabelecimento
        'tipo_moviment',    # Tipo de movimentação
        'fonte_info'        # Fonte (declaração)
    ]
    
    def __init__(self):
        self.base_url = "ftp://ftp.mtps.gov.br/pdet/microdados/NOVO%20CAGED/"
    
    def process_caged_file(
        self, 
        filepath: str,
        filtrar_construcao: bool = True,
        agrupar_por_uf: bool = True
    ) -> pd.DataFrame:
        """
        Processa arquivo do CAGED (CSV ou TXT).
        
        O usuário baixa o arquivo do portal do MTE e este método
        faz o parsing e transformação para o schema do DW.
        
        Args:
            filepath: Caminho do arquivo CAGED baixado
            filtrar_construcao: Se True, filtra apenas setor construção
            agrupar_por_uf: Se True, agrupa dados por UF
            
        Returns:
            DataFrame no schema fact_emprego
        """
        logger.info("Processando arquivo CAGED", filepath=filepath)
        
        # Detecta formato pelo conteúdo
        df = self._read_caged_file(filepath)
        
        if df.empty:
            logger.warning("Arquivo CAGED vazio ou formato não reconhecido")
            return pd.DataFrame()
        
        # Padroniza nomes de colunas
        df = self._normalize_columns(df)
        
        # Filtra construção civil se solicitado
        if filtrar_construcao:
            df = self._filter_construcao(df)
        
        # Agrupa e transforma para schema
        result = self._transform_to_schema(df, agrupar_por_uf)
        
        logger.info(f"CAGED processado: {len(result)} registros")
        return result
    
    def _read_caged_file(self, filepath: str) -> pd.DataFrame:
        """Lê arquivo CAGED detectando formato automaticamente."""
        try:
            # Tenta CSV primeiro (formato mais comum nos dados recentes)
            if filepath.endswith('.csv'):
                df = pd.read_csv(filepath, sep=';', encoding='latin-1', low_memory=False)
            elif filepath.endswith('.txt'):
                # TXT pode ser delimitado ou posição fixa
                df = pd.read_csv(filepath, sep=';', encoding='latin-1', low_memory=False)
            else:
                # Tenta detectar
                df = pd.read_csv(filepath, sep=';', encoding='latin-1', low_memory=False)
            
            return df
            
        except Exception as e:
            logger.error(f"Erro ao ler arquivo CAGED: {e}")
            return pd.DataFrame()
    
    def _normalize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normaliza nomes de colunas para padrão."""
        # Mapeamento de possíveis nomes de colunas
        col_map = {
            # Competência/Data
            'competência': 'competencia',
            'competenciamov': 'competencia',
            'competênciadec': 'competencia',
            'ano': 'ano',
            'mes': 'mes',
            
            # Localização
            'uf': 'uf',
            'sigla_uf': 'uf',
            'município': 'municipio',
            'municipio': 'municipio',
            'codigomunicipio': 'municipio',
            
            # CNAE
            'seção': 'secao_cnae',
            'secao': 'secao_cnae',
            'subclasse': 'subclasse_cnae',
            'cnaes20subclasse': 'subclasse_cnae',
            
            # Movimento
            'saldomovimentação': 'saldo',
            'saldo': 'saldo',
            'admissões': 'admitidos',
            'admitidos': 'admitidos',
            'desligamentos': 'desligados',
            'desligados': 'desligados',
            
            # Salário
            'saláriomédio': 'salario_medio',
            'salariomedio': 'salario_medio',
            'valorsaláriofixo': 'salario_medio',
        }
        
        # Normaliza nomes (lowercase, remove espaços)
        df.columns = df.columns.str.lower().str.strip().str.replace(' ', '')
        
        # Aplica mapeamento
        df = df.rename(columns=col_map)
        
        return df
    
    def _filter_construcao(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filtra apenas registros da construção civil."""
        # Verifica coluna disponível
        if 'secao_cnae' in df.columns:
            # Seção F = Construção
            return df[df['secao_cnae'].astype(str).str.upper() == 'F']
        
        elif 'subclasse_cnae' in df.columns:
            # CNAE começa com 41, 42 ou 43
            df['cnae_div'] = df['subclasse_cnae'].astype(str).str[:2]
            return df[df['cnae_div'].isin(['41', '42', '43'])]
        
        else:
            logger.warning("Coluna CNAE não encontrada, retornando todos")
            return df
    
    def _transform_to_schema(self, df: pd.DataFrame, agrupar_por_uf: bool) -> pd.DataFrame:
        """Transforma para schema fact_emprego."""
        
        # Identifica coluna de UF
        if 'uf' not in df.columns and 'municipio' in df.columns:
            # Extrai UF do código do município (2 primeiros dígitos)
            df['uf_cod'] = df['municipio'].astype(str).str[:2].astype(int)
            df['uf'] = df['uf_cod'].map(self.UF_CODIGO)
        
        # Identifica data de referência
        if 'competencia' in df.columns:
            # Formato AAAAMM
            df['competencia'] = df['competencia'].astype(str)
            df['ano'] = df['competencia'].str[:4].astype(int)
            df['mes'] = df['competencia'].str[4:6].astype(int)
            df['data_referencia'] = pd.to_datetime(
                df['ano'].astype(str) + '-' + df['mes'].astype(str).str.zfill(2) + '-01'
            )
        elif 'ano' in df.columns and 'mes' in df.columns:
            df['data_referencia'] = pd.to_datetime(
                df['ano'].astype(str) + '-' + df['mes'].astype(str).str.zfill(2) + '-01'
            )
        
        # Agrupamento
        if agrupar_por_uf:
            agg_cols = {
                'admitidos': 'sum',
                'desligados': 'sum',
                'saldo': 'sum',
                'salario_medio': 'mean'
            }
            
            # Filtra colunas existentes
            agg_cols = {k: v for k, v in agg_cols.items() if k in df.columns}
            
            result = df.groupby(['uf', 'data_referencia']).agg(agg_cols).reset_index()
        else:
            result = df
        
        # Monta schema final
        output = pd.DataFrame()
        output['id_fato'] = range(1, len(result) + 1)
        output['fonte'] = 'CAGED'
        output['uf'] = result['uf'] if 'uf' in result.columns else None
        output['data_referencia'] = result['data_referencia'].dt.strftime('%Y-%m-%d')
        
        # Campos adicionais (se existirem)
        if 'saldo' in result.columns:
            output['saldo_admissoes'] = result['saldo']
        if 'salario_medio' in result.columns:
            output['salario_medio'] = result['salario_medio'].round(2)
        if 'admitidos' in result.columns:
            output['admitidos'] = result['admitidos']
        if 'desligados' in result.columns:
            output['desligados'] = result['desligados']
        
        return output


def create_sample_caged_data() -> pd.DataFrame:
    """
    Cria dados de exemplo do CAGED para teste.
    
    Em produção, usar dados reais do portal MTE.
    """
    import numpy as np
    
    # Dados simulados para construção civil
    ufs = ['SP', 'RJ', 'MG', 'RS', 'PR', 'SC', 'BA', 'PE', 'CE', 'GO']
    meses = pd.date_range('2024-01-01', '2025-12-01', freq='MS')
    
    records = []
    for uf in ufs:
        for mes in meses:
            # Variação sazonal (mais contratações no 1º semestre)
            base_saldo = np.random.randint(100, 500) * (1 + 0.2 * np.sin(mes.month * np.pi / 6))
            
            records.append({
                'id_fato': len(records) + 1,
                'fonte': 'CAGED',
                'uf': uf,
                'data_referencia': mes.strftime('%Y-%m-%d'),
                'saldo_admissoes': int(base_saldo),
                'salario_medio': round(2500 + np.random.uniform(-200, 500), 2)
            })
    
    return pd.DataFrame(records)


if __name__ == "__main__":
    # Exemplo de uso
    client = CAGEDClient()
    
    # Para processar arquivo real:
    # df = client.process_caged_file("caminho/para/CAGEDMOV202412.txt")
    
    # Para teste com dados simulados:
    df = create_sample_caged_data()
    print(df.head(20))
    print(f"\nTotal: {len(df)} registros")
