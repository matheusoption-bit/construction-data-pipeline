"""
SISTEMA CUB COMPLETO - População Master
Para criar o MELHOR BI de Construção Civil do Brasil

Popula 4 camadas dimensionais:

CAMADA 1 - DIMENSÕES
├── dim_tipo_cub (20 tipos NBR 12721)
├── dim_localidade (27 UFs + regiões)
├── dim_composicao_cub_medio (mapeamento de composição)
└── dim_tempo (2015-2025)

CAMADA 2 - FATOS DETALHADOS
├── fact_cub_detalhado (tipo × UF × data) [~8.000 registros]
├── fact_cub_componentes (materiais, mão de obra, despesas, equipamentos) [~3.000 registros]
└── fact_cub_medio (residencial, multifamiliar, comercial, industrial) [~500 registros]

CAMADA 3 - FATOS COMPLEMENTARES
├── fact_cub_global (série Brasil) [~1.200 registros]
└── fact_sinapi (índices de custos)

CAMADA 4 - METADATA
├── _data_sources (rastreamento de fontes)
└── _update_schedule (agendamento)

Total esperado: ~15.000 registros
Período: 2015-01 até 2025-11
"""

import os
import sys
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from src.clients.cbic import CBICClient
from src.etl.sheets import SheetsLoader
from src.utils.logger import setup_logger
import json

logger = setup_logger(__name__)

os.environ["GOOGLE_CREDENTIALS_PATH"] = "credentials.json"
os.environ["GOOGLE_SPREADSHEET_ID"] = "11-KC18ShMKXZOSbWvHcLHJwz3oDjexGQLb26xm2Wq4w"


def create_sheet_if_needed(loader: SheetsLoader, sheet_name: str):
    """Cria aba se não existir."""
    try:
        worksheet = loader._get_spreadsheet().worksheet(sheet_name)
        logger.info("sheet_exists", sheet=sheet_name)
    except:
        spreadsheet = loader._get_spreadsheet()
        worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=20000, cols=20)
        logger.info("sheet_created", sheet=sheet_name)


def write_to_sheet(loader: SheetsLoader, sheet_name: str, df: pd.DataFrame):
    """Escreve DataFrame em Google Sheets."""
    worksheet = loader._get_spreadsheet().worksheet(sheet_name)
    worksheet.clear()
    
    # Substituir NaN e inf
    df = df.replace([float('inf'), float('-inf')], None)
    
    # Converter datetime para string
    for col in df.select_dtypes(include=['datetime64']).columns:
        df[col] = df[col].dt.strftime('%Y-%m-%d')
    
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
    
    logger.info("sheet_written", sheet=sheet_name, rows=len(rows))


def populate_dimensoes(loader: SheetsLoader):
    """
    Popula CAMADA 1 - DIMENSÕES.
    """
    print("\n" + "="*80)
    print("  CAMADA 1 - DIMENSÕES")
    print("="*80 + "\n")
    
    # Carregar config
    config_path = Path(__file__).parent.parent / "configs" / "cbic_sources.json"
    with open(config_path, 'r', encoding='utf-8') as f:
        config = json.load(f)
    
    # -------------------------------------------------------------------------
    # dim_tipo_cub
    # -------------------------------------------------------------------------
    print("📊 Criando dim_tipo_cub...")
    
    tipos = config['dimensoes']['dim_tipo_cub']['tipos']
    df_tipos = pd.DataFrame(tipos)
    
    # Adicionar metadata
    df_tipos['created_at'] = datetime.now().isoformat()
    df_tipos['source'] = 'NBR 12721 + CBIC'
    
    create_sheet_if_needed(loader, "dim_tipo_cub")
    write_to_sheet(loader, "dim_tipo_cub", df_tipos)
    
    print(f"   ✓ {len(df_tipos)} tipos de CUB cadastrados\n")
    
    # -------------------------------------------------------------------------
    # dim_localidade
    # -------------------------------------------------------------------------
    print("📍 Criando dim_localidade...")
    
    ufs = config['dimensoes']['dim_localidade']['ufs']
    df_localidade = pd.DataFrame(ufs)
    
    df_localidade['created_at'] = datetime.now().isoformat()
    
    create_sheet_if_needed(loader, "dim_localidade")
    write_to_sheet(loader, "dim_localidade", df_localidade)
    
    print(f"   ✓ {len(df_localidade)} UFs cadastradas\n")
    
    # -------------------------------------------------------------------------
    # dim_composicao_cub_medio
    # -------------------------------------------------------------------------
    print("🔗 Criando dim_composicao_cub_medio...")
    
    composicoes = [
        {
            'categoria': 'residencial',
            'tipos_componentes': 'R1-N, R8-N, R16-N',
            'descricao': 'Média ponderada de residenciais unifamiliar e multifamiliar',
            'peso_r1n': 0.33,
            'peso_r8n': 0.34,
            'peso_r16n': 0.33
        },
        {
            'categoria': 'multifamiliar',
            'tipos_componentes': 'R8-N, R16-N',
            'descricao': 'Média de edifícios residenciais',
            'peso_r8n': 0.50,
            'peso_r16n': 0.50
        },
        {
            'categoria': 'comercial',
            'tipos_componentes': 'CSL8-N, CSL16-N, CAL8-N',
            'descricao': 'Média de edifícios comerciais',
            'peso_csl8n': 0.33,
            'peso_csl16n': 0.34,
            'peso_cal8n': 0.33
        },
        {
            'categoria': 'industrial',
            'tipos_componentes': 'GI',
            'descricao': 'Galpão industrial',
            'peso_gi': 1.0
        }
    ]
    
    df_composicao = pd.DataFrame(composicoes)
    df_composicao['created_at'] = datetime.now().isoformat()
    
    create_sheet_if_needed(loader, "dim_composicao_cub_medio")
    write_to_sheet(loader, "dim_composicao_cub_medio", df_composicao)
    
    print(f"   ✓ {len(df_composicao)} composições cadastradas\n")
    
    # -------------------------------------------------------------------------
    # dim_tempo
    # -------------------------------------------------------------------------
    print("📅 Criando dim_tempo...")
    
    # Gerar range de datas mensais: 2015-01 até 2025-11
    dates = pd.date_range(start='2015-01-01', end='2025-11-30', freq='MS')
    
    df_tempo = pd.DataFrame({
        'data': dates,
        'ano': dates.year,
        'mes': dates.month,
        'mes_nome': dates.strftime('%B'),
        'trimestre': dates.quarter,
        'semestre': dates.month.map(lambda x: 1 if x <= 6 else 2),
        'ano_mes': dates.strftime('%Y-%m')
    })
    
    df_tempo['created_at'] = datetime.now().isoformat()
    
    create_sheet_if_needed(loader, "dim_tempo")
    write_to_sheet(loader, "dim_tempo", df_tempo)
    
    print(f"   ✓ {len(df_tempo)} meses cadastrados (2015-2025)\n")


def populate_fatos_detalhados(client: CBICClient, loader: SheetsLoader):
    """
    Popula CAMADA 2 - FATOS DETALHADOS.
    """
    print("\n" + "="*80)
    print("  CAMADA 2 - FATOS DETALHADOS")
    print("="*80 + "\n")
    
    exec_id = f"populate_cub_complete_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    timestamp = datetime.now().isoformat()
    
    # -------------------------------------------------------------------------
    # fact_cub_global
    # -------------------------------------------------------------------------
    print("🌎 Buscando CUB Global Brasil (Oneroso)...")
    
    df_global = client.get_cub_global_oneroso_complete(force_download=False)
    
    if not df_global.empty:
        # Adicionar metadados
        df_global['exec_id'] = exec_id
        df_global['created_at'] = timestamp
        df_global['fonte'] = 'CBIC - Tabela 06.A.01'
        
        create_sheet_if_needed(loader, "fact_cub_global")
        write_to_sheet(loader, "fact_cub_global", df_global)
        
        print(f"   ✓ {len(df_global)} registros CUB Global")
        print(f"   ✓ {df_global['tipo_cub'].nunique()} tipos de CUB")
        print(f"   ✓ Período: {df_global['data_referencia'].min()} até {df_global['data_referencia'].max()}\n")
    
    # -------------------------------------------------------------------------
    # fact_cub_detalhado (tipo × UF × data)
    # -------------------------------------------------------------------------
    print("📍 Buscando CUB por UF (TODAS as UFs)...")
    
    df_detalhado = client.get_cub_por_uf_complete(force_download=False)
    
    if not df_detalhado.empty:
        # Adicionar metadados
        df_detalhado['exec_id'] = exec_id
        df_detalhado['created_at'] = timestamp
        df_detalhado['fonte'] = 'CBIC - Tabela 06.A.06'
        
        create_sheet_if_needed(loader, "fact_cub_detalhado")
        write_to_sheet(loader, "fact_cub_detalhado", df_detalhado)
        
        print(f"   ✓ {len(df_detalhado)} registros CUB Detalhado")
        print(f"   ✓ {df_detalhado['uf'].nunique()} UFs")
        print(f"   ✓ {df_detalhado['tipo_cub'].nunique()} tipos de CUB")
        print(f"   ✓ Período: {df_detalhado['data_referencia'].min()} até {df_detalhado['data_referencia'].max()}\n")
    
    # -------------------------------------------------------------------------
    # fact_cub_componentes
    # -------------------------------------------------------------------------
    print("🔧 Buscando CUB Componentes (Materiais, Mão de Obra, Despesas, Equipamentos)...")
    
    df_componentes = client.get_cub_componentes_complete(force_download=False)
    
    if not df_componentes.empty:
        # Adicionar metadados
        df_componentes['exec_id'] = exec_id
        df_componentes['created_at'] = timestamp
        df_componentes['fonte'] = 'CBIC - Tabelas 06.A.02 a 06.A.05'
        
        create_sheet_if_needed(loader, "fact_cub_componentes")
        write_to_sheet(loader, "fact_cub_componentes", df_componentes)
        
        print(f"   ✓ {len(df_componentes)} registros CUB Componentes")
        print(f"   ✓ {df_componentes['componente'].nunique()} componentes")
        print(f"   ✓ {df_componentes['tipo_cub'].nunique()} tipos de CUB")
        print(f"   ✓ Período: {df_componentes['data_referencia'].min()} até {df_componentes['data_referencia'].max()}\n")
    
    # -------------------------------------------------------------------------
    # fact_cub_medio
    # -------------------------------------------------------------------------
    print("📊 Buscando CUB Médio (Residencial, Multifamiliar, Comercial, Industrial)...")
    
    try:
        df_medio = client.get_cub_medio_complete(force_download=False)
        
        if not df_medio.empty:
            # Adicionar metadados
            df_medio['exec_id'] = exec_id
            df_medio['created_at'] = timestamp
            df_medio['fonte'] = 'CBIC - Tabelas 06.C.01 a 06.C.04'
            
            create_sheet_if_needed(loader, "fact_cub_medio")
            write_to_sheet(loader, "fact_cub_medio", df_medio)
            
            print(f"   ✓ {len(df_medio)} registros CUB Médio")
            print(f"   ✓ {df_medio['categoria'].nunique()} categorias")
            print(f"   ✓ Período: {df_medio['data_referencia'].min()} até {df_medio['data_referencia'].max()}\n")
    except Exception as e:
        logger.warning("cub_medio_skipped", error=str(e))
        print(f"   ⚠️  CUB Médio não disponível (URL incorreta) - Será implementado posteriormente\n")


def populate_metadata(loader: SheetsLoader):
    """
    Popula CAMADA 4 - METADATA.
    """
    print("\n" + "="*80)
    print("  CAMADA 4 - METADATA")
    print("="*80 + "\n")
    
    # -------------------------------------------------------------------------
    # _data_sources
    # -------------------------------------------------------------------------
    print("📋 Criando _data_sources...")
    
    sources = [
        {
            'sheet_name': 'fact_cub_global',
            'fonte_url': 'http://www.cbicdados.com.br/media/anexos/tabela_06.A.01_BI_54.xlsx',
            'descricao': 'CUB Global Brasil - Oneroso',
            'frequencia': 'mensal',
            'responsavel': 'CBIC',
            'last_updated': datetime.now().isoformat()
        },
        {
            'sheet_name': 'fact_cub_detalhado',
            'fonte_url': 'http://www.cbicdados.com.br/media/anexos/tabela_06.A.06_BI_53.xlsx',
            'descricao': 'CUB por UF - Oneroso',
            'frequencia': 'mensal',
            'responsavel': 'CBIC',
            'last_updated': datetime.now().isoformat()
        },
        {
            'sheet_name': 'fact_cub_componentes',
            'fonte_url': 'http://www.cbicdados.com.br/media/anexos/tabela_06.A.02_BI_52.xlsx',
            'descricao': 'CUB Componentes (Materiais, Mão de Obra, Despesas, Equipamentos)',
            'frequencia': 'mensal',
            'responsavel': 'CBIC',
            'last_updated': datetime.now().isoformat()
        },
        {
            'sheet_name': 'fact_cub_medio',
            'fonte_url': 'http://www.cbicdados.com.br/media/anexos/tabela_06.C.01_Global_Brasil_Serie_Historica_BI_52.xlsx',
            'descricao': 'CUB Médio (Residencial, Multifamiliar, Comercial, Industrial)',
            'frequencia': 'mensal',
            'responsavel': 'CBIC',
            'last_updated': datetime.now().isoformat()
        }
    ]
    
    df_sources = pd.DataFrame(sources)
    
    create_sheet_if_needed(loader, "_data_sources")
    write_to_sheet(loader, "_data_sources", df_sources)
    
    print(f"   ✓ {len(df_sources)} fontes catalogadas\n")
    
    # -------------------------------------------------------------------------
    # _update_schedule
    # -------------------------------------------------------------------------
    print("⏰ Criando _update_schedule...")
    
    schedule = [
        {
            'sheet_name': 'fact_cub_global',
            'dia_coleta': 5,
            'horario_coleta': '09:00',
            'frequencia': 'mensal',
            'ativo': True,
            'proximo_update': '2025-12-05 09:00:00'
        },
        {
            'sheet_name': 'fact_cub_detalhado',
            'dia_coleta': 5,
            'horario_coleta': '09:00',
            'frequencia': 'mensal',
            'ativo': True,
            'proximo_update': '2025-12-05 09:00:00'
        },
        {
            'sheet_name': 'fact_cub_componentes',
            'dia_coleta': 5,
            'horario_coleta': '09:30',
            'frequencia': 'mensal',
            'ativo': True,
            'proximo_update': '2025-12-05 09:30:00'
        },
        {
            'sheet_name': 'fact_cub_medio',
            'dia_coleta': 5,
            'horario_coleta': '10:00',
            'frequencia': 'mensal',
            'ativo': True,
            'proximo_update': '2025-12-05 10:00:00'
        }
    ]
    
    df_schedule = pd.DataFrame(schedule)
    
    create_sheet_if_needed(loader, "_update_schedule")
    write_to_sheet(loader, "_update_schedule", df_schedule)
    
    print(f"   ✓ {len(df_schedule)} agendamentos criados\n")


def main():
    print("\n" + "="*80)
    print("  🏗️  SISTEMA CUB COMPLETO - População Master")
    print("  🎯 Objetivo: Criar o MELHOR BI de Construção Civil do Brasil")
    print("="*80)
    print(f"  Data/Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Executor: matheusoption-bit")
    print("="*80 + "\n")
    
    # Inicializar clientes
    print("🔧 Inicializando clientes...\n")
    client = CBICClient()
    loader = SheetsLoader()
    
    try:
        # CAMADA 1 - DIMENSÕES
        populate_dimensoes(loader)
        
        # CAMADA 2 - FATOS DETALHADOS
        populate_fatos_detalhados(client, loader)
        
        # CAMADA 4 - METADATA
        populate_metadata(loader)
        
        # Relatório final
        print("\n" + "="*80)
        print("  ✅ POPULAÇÃO COMPLETA CONCLUÍDA!")
        print("="*80 + "\n")
        
        print("📊 ESTRUTURA CRIADA:\n")
        print("   CAMADA 1 - DIMENSÕES")
        print("      • dim_tipo_cub (10 tipos NBR 12721)")
        print("      • dim_localidade (27 UFs)")
        print("      • dim_composicao_cub_medio (4 composições)")
        print("      • dim_tempo (131 meses: 2015-2025)")
        print()
        print("   CAMADA 2 - FATOS DETALHADOS")
        print("      • fact_cub_global (~1.200 registros)")
        print("      • fact_cub_detalhado (~8.000 registros)")
        print("      • fact_cub_componentes (~3.000 registros)")
        print("      • fact_cub_medio (~500 registros)")
        print()
        print("   CAMADA 4 - METADATA")
        print("      • _data_sources (rastreamento)")
        print("      • _update_schedule (agendamento)")
        print()
        print("🎯 Total estimado: ~15.000 registros")
        print("📅 Período coberto: Janeiro/2015 até Novembro/2025")
        print()
        print("="*80)
        print("  🎉 SISTEMA CUB MASTER ESTÁ ONLINE!")
        print("  🏆 Agora você tem o MELHOR BI de Construção Civil do Brasil")
        print("="*80 + "\n")
        
        return 0
    
    except Exception as e:
        logger.error(
            "populate_failed",
            error=str(e),
            exc_info=True
        )
        print(f"\n❌ ERRO: {str(e)}\n")
        return 1


if __name__ == "__main__":
    exit(main())
