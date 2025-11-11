"""
Limpeza COMPLETA de fact_series.

Remove:
1. Datas futuras
2. Valores vazios/zeros
3. Duplicatas
4. Dados suspeitos
"""

import os
import sys
from datetime import datetime
from pathlib import Path

# Adicionar path do projeto
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from src.etl.sheets import SheetsLoader
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

# Configurar env vars
os.environ["GOOGLE_CREDENTIALS_PATH"] = "credentials.json"
os.environ["GOOGLE_SPREADSHEET_ID"] = "11-KC18ShMKXZOSbWvHcLHJwz3oDjexGQLb26xm2Wq4w"


def main():
    print("\n" + "="*70)
    print("  🧹 LIMPEZA COMPLETA - fact_series")
    print("="*70 + "\n")
    
    loader = SheetsLoader()
    
    # Ler dados
    print("📖 Lendo fact_series...")
    data = loader.read_sheet("fact_series")
    
    if len(data) <= 1:
        print("✓ Aba vazia ou só header\n")
        return 0
    
    df = pd.DataFrame(data[1:], columns=data[0])
    
    print(f"✓ {len(df)} linhas carregadas\n")
    
    # Limpeza 1: Datas futuras
    print("🔍 Removendo datas futuras...")
    
    df['data_referencia'] = pd.to_datetime(df['data_referencia'], errors='coerce')
    hoje = datetime.now()
    
    df_futuro = df[df['data_referencia'] > hoje]
    print(f"  ⚠️  {len(df_futuro)} linhas com datas futuras removidas")
    
    df = df[df['data_referencia'] <= hoje]
    
    # Limpeza 2: Valores vazios
    print("\n🔍 Removendo valores vazios...")
    
    df['valor'] = pd.to_numeric(df['valor'], errors='coerce')
    
    df_vazio = df[df['valor'].isna()]
    print(f"  ⚠️  {len(df_vazio)} linhas com valores vazios removidas")
    
    df = df[df['valor'].notna()]
    
    # Limpeza 3: Valores absurdos
    print("\n🔍 Removendo valores absurdos...")
    
    # Detectar outliers extremos por série
    outliers_total = 0
    for series_id in df['series_id'].unique():
        df_series = df[df['series_id'] == series_id]
        
        Q1 = df_series['valor'].quantile(0.25)
        Q3 = df_series['valor'].quantile(0.75)
        IQR = Q3 - Q1
        
        # Outliers: fora de 10*IQR (muito generoso)
        lower = Q1 - 10 * IQR
        upper = Q3 + 10 * IQR
        
        outliers = df_series[
            (df_series['valor'] < lower) | 
            (df_series['valor'] > upper)
        ]
        
        if not outliers.empty:
            print(f"  ⚠️  {series_id}: {len(outliers)} outliers removidos")
            df = df[~df.index.isin(outliers.index)]
            outliers_total += len(outliers)
    
    if outliers_total == 0:
        print(f"  ✓ Nenhum outlier detectado")
    
    # Limpeza 4: Duplicatas
    print("\n🔍 Removendo duplicatas...")
    
    df_dup = df[df.duplicated(subset=['id_fato'], keep=False)]
    print(f"  ⚠️  {len(df_dup)} duplicatas encontradas")
    
    # Manter mais recente (por created_at)
    df['created_at_dt'] = pd.to_datetime(df['created_at'], errors='coerce')
    df = df.sort_values('created_at_dt').drop_duplicates(
        subset=['id_fato'],
        keep='last'
    )
    df = df.drop('created_at_dt', axis=1)
    
    # Ordenar por data
    df = df.sort_values(['series_id', 'data_referencia'])
    
    print(f"\n✓ Limpeza concluída: {len(df)} linhas restantes\n")
    
    # Reescrever
    print("💾 Reescrevendo fact_series...")
    
    worksheet = loader._get_spreadsheet().worksheet("fact_series")
    worksheet.clear()
    
    # Converter data_referencia de volta para string no formato correto
    df['data_referencia'] = df['data_referencia'].dt.strftime('%Y-%m-%d')
    
    # Header + Dados em uma operação
    headers = [list(df.columns)]
    rows = df.values.tolist()
    
    # Converter NaN para string vazia
    rows = [
        ['' if pd.isna(val) else val for val in row]
        for row in rows
    ]
    
    all_data = headers + rows
    worksheet.update(values=all_data, range_name='A1')
    
    print(f"✓ {len(rows)} linhas escritas\n")
    
    logger.info(
        "fact_series_cleaned_complete",
        original_rows=len(data) - 1,
        final_rows=len(df),
        removed_rows=len(data) - 1 - len(df)
    )
    
    print("="*70)
    print("  ✅ LIMPEZA CONCLUÍDA!")
    print("="*70 + "\n")
    
    return 0


if __name__ == "__main__":
    exit(main())
