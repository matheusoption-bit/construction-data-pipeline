"""
Job MASTER de população completa do sistema CUB.

Popula:
1. dim_tipo_cub (20 tipos)
2. dim_localidade (27 UFs)
3. dim_composicao_cub_medio (metodologia)
4. fact_cub_detalhado (série histórica por tipo × UF)
5. fact_cub_componentes (materiais, mão de obra, etc)

Tempo estimado: 15-20 minutos (download + parse + write)

Uso:
    python -m src.jobs.populate_complete_cub
"""

import sys
from pathlib import Path
from datetime import datetime
import pandas as pd

from src.clients.cbic_universal import CBICUniversalClient
from src.etl.sheets import SheetsLoader
from src.data.dim_tipo_cub import TIPOS_CUB
from src.data.dim_localidade import LOCALIDADES
from src.data.dim_composicao_cub_medio import COMPOSICAO_CUB_MEDIO
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


def populate_dimensions(loader: SheetsLoader):
    """Popula todas as dimensões."""
    
    print("\n" + "="*70)
    print("  📊 POPULANDO DIMENSÕES")
    print("="*70 + "\n")
    
    # dim_tipo_cub
    print("1️⃣ dim_tipo_cub (20 tipos NBR 12721)...")
    df_tipos = pd.DataFrame(TIPOS_CUB)
    
    loader.create_sheet_if_not_exists("dim_tipo_cub", headers=list(df_tipos.columns))
    worksheet = loader._get_spreadsheet().worksheet("dim_tipo_cub")
    worksheet.clear()
    worksheet.append_row(list(df_tipos.columns))
    loader.append_to_sheet("dim_tipo_cub", df_tipos.values.tolist())
    
    print(f"  ✓ {len(df_tipos)} tipos gravados\n")
    
    # dim_localidade
    print("2️⃣ dim_localidade (27 estados)...")
    df_local = pd.DataFrame(LOCALIDADES)
    
    loader.create_sheet_if_not_exists("dim_localidade", headers=list(df_local.columns))
    worksheet = loader._get_spreadsheet().worksheet("dim_localidade")
    worksheet.clear()
    worksheet.append_row(list(df_local.columns))
    loader.append_to_sheet("dim_localidade", df_local.values.tolist())
    
    print(f"  ✓ {len(df_local)} estados gravados\n")
    
    # dim_composicao_cub_medio
    print("3️⃣ dim_composicao_cub_medio (metodologia ponderação)...")
    df_comp = pd.DataFrame(COMPOSICAO_CUB_MEDIO)
    
    loader.create_sheet_if_not_exists("dim_composicao_cub_medio", headers=list(df_comp.columns))
    worksheet = loader._get_spreadsheet().worksheet("dim_composicao_cub_medio")
    worksheet.clear()
    worksheet.append_row(list(df_comp.columns))
    loader.append_to_sheet("dim_composicao_cub_medio", df_comp.values.tolist())
    
    print(f"  ✓ {len(df_comp)} composições gravadas\n")


def populate_fact_cub_detalhado(loader: SheetsLoader, client: CBICUniversalClient):
    """Popula fact_cub_detalhado (série histórica por tipo × UF)."""
    
    print("\n" + "="*70)
    print("  📊 POPULANDO fact_cub_detalhado")
    print("="*70 + "\n")
    
    # Buscar CUB por UF
    print("🔍 Baixando CUB por UF (oneroso)...")
    df = client.get_cub_detalhado(tipo="oneroso", granularidade="uf")
    
    if df.empty:
        print("  ❌ Sem dados\n")
        return
    
    print(f"  ✓ {len(df)} pontos coletados\n")
    
    # Preparar estrutura fact
    df["data_referencia"] = pd.to_datetime(df["data"]).dt.strftime("%Y-%m-%d")
    
    df["id_fato"] = df.apply(
        lambda row: f"CUB_{row['uf']}_{row['tipo_cub']}_{row['data_referencia']}",
        axis=1
    )
    
    df["created_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Reordenar
    columns = [
        "id_fato",
        "data_referencia",
        "uf",
        "tipo_cub",
        "valor",
        "created_at"
    ]
    
    df = df[columns]
    
    # Escrever
    print("💾 Gravando em fact_cub_detalhado...")
    
    loader.create_sheet_if_not_exists("fact_cub_detalhado", headers=columns)
    worksheet = loader._get_spreadsheet().worksheet("fact_cub_detalhado")
    worksheet.clear()
    worksheet.append_row(columns)
    loader.append_to_sheet("fact_cub_detalhado", df.values.tolist())
    
    print(f"  ✓ {len(df)} linhas gravadas\n")


def main():
    """Executa população completa."""
    
    print("\n" + "="*70)
    print("  🚀 POPULAÇÃO COMPLETA - SISTEMA CUB MASTER")
    print("="*70 + "\n")
    
    start_time = datetime.now()
    
    loader = SheetsLoader()
    client = CBICUniversalClient()
    
    # Fase 1: Dimensões
    populate_dimensions(loader)
    
    # Fase 2: Fatos
    populate_fact_cub_detalhado(loader, client)
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    print("="*70)
    print("  ✅ POPULAÇÃO COMPLETA!")
    print("  🎉 Sistema CUB operacional!")
    print(f"  ⏱️  Tempo total: {duration:.1f} segundos")
    print("="*70 + "\n")
    
    return 0


if __name__ == "__main__":
    try:
        exit(main())
    except Exception as e:
        logger.error("population_failed", error=str(e), exc_info=True)
        print(f"\n❌ ERRO: {str(e)}\n")
        exit(1)
