"""
Verifica dados das tabelas dimensionais.
"""
import os
import sys
import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.etl.sheets import SheetsLoader

# Configurar env vars
os.environ["GOOGLE_CREDENTIALS_PATH"] = "credentials.json"
os.environ["GOOGLE_SPREADSHEET_ID"] = "11-KC18ShMKXZOSbWvHcLHJwz3oDjexGQLb26xm2Wq4w"

# Tabelas para verificar
tables = [
    "dim_series",
    "dim_topografia",
    "dim_metodo",
    "dim_projetos"
]

loader = SheetsLoader()

print("=" * 70)
print("  VERIFICACAO DE TABELAS DIMENSIONAIS")
print("=" * 70)

total_rows = 0

for table in tables:
    print(f"\n[{table}]")
    
    try:
        data = loader.read_sheet(table)
        
        if len(data) <= 1:
            print(f"  Vazia (apenas header)")
            continue
        
        df = pd.DataFrame(data[1:], columns=data[0])
        
        print(f"  Linhas: {len(df)}")
        print(f"  Colunas: {', '.join(df.columns)}")
        
        # Mostrar primeiras 3 linhas
        print(f"\n  Primeiras 3 linhas:")
        for i, row in df.head(3).iterrows():
            print(f"    [{i+1}] {dict(row)}")
        
        total_rows += len(df)
        
    except Exception as e:
        print(f"  ERRO: {str(e)}")

print("\n" + "=" * 70)
print(f"TOTAL: {total_rows} linhas em {len(tables)} tabelas")
print("=" * 70)
