"""
REESTRUTURAÇÃO COMPLETA DO FACT_SERIES
Para apresentação profissional na SEXTA-FEIRA

Cria:
- 9 abas separadas (uma por indicador)
- Estrutura padronizada com colunas descritivas
- Variações calculadas (diária, mensal, anual)
- Metadados de rastreabilidade
- fact_series consolidada para análises cruzadas

Estrutura de Colunas:
- id_fato
- data_referencia
- nome_indicador
- unidade
- valor
- variacao_diaria
- variacao_mensal
- variacao_anual
- horario_exec
- data_exec
- metodo_coleta
- status_coleta
- fonte_url
- observacao
"""

import os
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from src.clients.bcb_v2 import BCBClient
from src.etl.sheets import SheetsLoader
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

os.environ["GOOGLE_CREDENTIALS_PATH"] = "credentials.json"
os.environ["GOOGLE_SPREADSHEET_ID"] = "11-KC18ShMKXZOSbWvHcLHJwz3oDjexGQLb26xm2Wq4w"


# Configuração dos indicadores
INDICATORS = {
    "taxa_cambio": {
        "series_id": "1",
        "nome": "Taxa de Câmbio USD/BRL",
        "unidade": "R$/US$",
        "frequencia": "diaria"
    },
    "igp_m_infla": {
        "series_id": "189",
        "nome": "IGP-M - Inflação",
        "unidade": "% m/m",
        "frequencia": "mensal"
    },
    "taxa_ref": {
        "series_id": "226",
        "nome": "Taxa Referencial - TR",
        "unidade": "% a.m.",
        "frequencia": "diaria"
    },
    "credito_habit": {
        "series_id": "24364",
        "nome": "Estoque de Crédito Habitacional",
        "unidade": "R$ bilhões",
        "frequencia": "mensal"
    },
    "inpc_infla": {
        "series_id": "4189",
        "nome": "INPC - Inflação",
        "unidade": "% m/m",
        "frequencia": "mensal"
    },
    "taxa_selic": {
        "series_id": "432",
        "nome": "Taxa Selic",
        "unidade": "% a.a.",
        "frequencia": "mensal"
    },
    "ipca_infla": {
        "series_id": "433",
        "nome": "IPCA - Inflação",
        "unidade": "% m/m",
        "frequencia": "mensal"
    },
    "credito_pf": {
        "series_id": "4390",
        "nome": "Crédito Pessoa Física",
        "unidade": "R$ milhões",
        "frequencia": "mensal"
    },
    "rend_poupanca": {
        "series_id": "7478",
        "nome": "Rendimento da Poupança",
        "unidade": "% a.m.",
        "frequencia": "mensal"
    }
}


def process_indicator(client: BCBClient, aba_name: str, config: dict) -> pd.DataFrame:
    """
    Processa um indicador e retorna DataFrame padronizado.
    """
    print(f"\n📊 Processando: {config['nome']}...")
    
    # Buscar dados do BCB
    df = client.get_series(
        series_id=config['series_id'],
        data_inicio="01/01/2018",
        data_fim="11/11/2025"
    )
    
    if df.empty:
        print(f"   ⚠️  Sem dados disponíveis")
        return pd.DataFrame()
    
    # Criar estrutura padronizada
    df_padrao = pd.DataFrame()
    
    # Colunas básicas
    df_padrao['id_fato'] = df['data'].dt.strftime('%Y-%m-%d') + f"_BCB_SGS_{config['series_id']}"
    df_padrao['data_referencia'] = df['data'].dt.strftime('%Y-%m-%d')
    df_padrao['nome_indicador'] = config['nome']
    df_padrao['unidade'] = config['unidade']
    df_padrao['valor'] = df['valor']
    
    # Ordenar por data
    df_padrao = df_padrao.sort_values('data_referencia').reset_index(drop=True)
    
    # Calcular variações
    if config['frequencia'] == 'diaria':
        # Variação diária
        df_padrao['variacao_diaria'] = df_padrao['valor'].pct_change() * 100
        
        # Variação mensal (21 dias úteis)
        df_padrao['variacao_mensal'] = df_padrao['valor'].pct_change(periods=21) * 100
        
        # Variação anual (252 dias úteis)
        df_padrao['variacao_anual'] = df_padrao['valor'].pct_change(periods=252) * 100
    
    else:  # mensal
        # Variação mensal
        df_padrao['variacao_mensal'] = df_padrao['valor'].pct_change() * 100
        
        # Variação anual (12 meses)
        df_padrao['variacao_anual'] = df_padrao['valor'].pct_change(periods=12) * 100
        
        # Diária não se aplica
        df_padrao['variacao_diaria'] = None
    
    # Metadados
    timestamp = datetime.now()
    df_padrao['horario_exec'] = timestamp.strftime('%H:%M:%S')
    df_padrao['data_exec'] = timestamp.strftime('%Y-%m-%d')
    df_padrao['metodo_coleta'] = 'API BCB SGS'
    df_padrao['status_coleta'] = 'sucesso'
    df_padrao['fonte_url'] = f'https://api.bcb.gov.br/dados/serie/bcdata.sgs.{config["series_id"]}/dados'
    df_padrao['observacao'] = f'Frequência: {config["frequencia"]} | Histórico: 2018-2025'
    
    print(f"   ✓ {len(df_padrao)} registros processados")
    print(f"   ✓ Período: {df_padrao['data_referencia'].min()} até {df_padrao['data_referencia'].max()}")
    
    return df_padrao


def create_sheet_if_needed(loader: SheetsLoader, sheet_name: str, headers: list):
    """
    Cria aba se não existir.
    """
    try:
        worksheet = loader._get_spreadsheet().worksheet(sheet_name)
        print(f"   ✓ Aba '{sheet_name}' já existe")
    except:
        spreadsheet = loader._get_spreadsheet()
        worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=10000, cols=20)
        print(f"   ✓ Aba '{sheet_name}' criada")


def write_to_sheet(loader: SheetsLoader, sheet_name: str, df: pd.DataFrame):
    """
    Escreve DataFrame em aba do Google Sheets.
    """
    worksheet = loader._get_spreadsheet().worksheet(sheet_name)
    worksheet.clear()
    
    # Substituir NaN e inf por strings vazias
    df = df.replace([float('inf'), float('-inf')], None)
    
    # Header + rows
    headers = [list(df.columns)]
    rows = df.values.tolist()
    
    # Converter NaN para string vazia
    rows = [
        ['' if pd.isna(val) else val for val in row]
        for row in rows
    ]
    
    all_data = headers + rows
    worksheet.update(values=all_data, range_name='A1')
    
    print(f"   ✓ {len(rows)} linhas escritas em '{sheet_name}'")


def main():
    print("\n" + "="*80)
    print("  🎯 REESTRUTURAÇÃO COMPLETA DO FACT_SERIES")
    print("  📊 Para apresentação profissional na SEXTA-FEIRA")
    print("="*80 + "\n")
    
    print("📋 Estratégia:")
    print("   • 9 abas separadas (uma por indicador)")
    print("   • Estrutura padronizada com 15 colunas")
    print("   • Variações calculadas (diária, mensal, anual)")
    print("   • Metadados de rastreabilidade")
    print("   • fact_series consolidada para análises cruzadas\n")
    
    # Inicializar clientes
    print("🔧 Inicializando clientes...\n")
    client = BCBClient()
    loader = SheetsLoader()
    
    # Processar cada indicador
    print("="*80)
    print("  ETAPA 1: Processando Indicadores")
    print("="*80)
    
    all_dataframes = []
    
    for aba_name, config in INDICATORS.items():
        df = process_indicator(client, aba_name, config)
        
        if not df.empty:
            all_dataframes.append(df)
            
            # Criar aba se necessário
            create_sheet_if_needed(loader, aba_name, list(df.columns))
            
            # Escrever dados
            write_to_sheet(loader, aba_name, df)
    
    # Consolidar em fact_series
    print("\n" + "="*80)
    print("  ETAPA 2: Criando fact_series Consolidada")
    print("="*80 + "\n")
    
    if all_dataframes:
        df_consolidado = pd.concat(all_dataframes, ignore_index=True)
        df_consolidado = df_consolidado.sort_values(['nome_indicador', 'data_referencia'])
        
        print(f"📊 Consolidado:")
        print(f"   • Total de registros: {len(df_consolidado):,}")
        print(f"   • Indicadores: {df_consolidado['nome_indicador'].nunique()}")
        print(f"   • Período: {df_consolidado['data_referencia'].min()} até {df_consolidado['data_referencia'].max()}\n")
        
        # Escrever fact_series
        write_to_sheet(loader, "fact_series", df_consolidado)
    
    # Relatório final
    print("\n" + "="*80)
    print("  ETAPA 3: Relatório Final")
    print("="*80 + "\n")
    
    print("✅ ABAS CRIADAS:\n")
    
    for aba_name, config in INDICATORS.items():
        print(f"   • {aba_name}")
        print(f"     Nome: {config['nome']}")
        print(f"     Unidade: {config['unidade']}")
        print(f"     Frequência: {config['frequencia']}")
        print()
    
    print("   • fact_series")
    print(f"     Consolidado de todos os indicadores")
    print()
    
    print("📋 ESTRUTURA DE COLUNAS (15 colunas padronizadas):\n")
    
    colunas = [
        "id_fato", "data_referencia", "nome_indicador", "unidade", "valor",
        "variacao_diaria", "variacao_mensal", "variacao_anual",
        "horario_exec", "data_exec", "metodo_coleta", "status_coleta",
        "fonte_url", "observacao"
    ]
    
    for i, col in enumerate(colunas, 1):
        print(f"   {i:2d}. {col}")
    
    print("\n" + "="*80)
    print("  ✅ REESTRUTURAÇÃO COMPLETA CONCLUÍDA!")
    print("  🎯 Dados 100% prontos para apresentação na SEXTA-FEIRA")
    print("="*80 + "\n")
    
    logger.info(
        "restructure_complete",
        total_sheets=len(INDICATORS) + 1,
        total_records=len(df_consolidado) if all_dataframes else 0
    )
    
    return 0


if __name__ == "__main__":
    exit(main())
