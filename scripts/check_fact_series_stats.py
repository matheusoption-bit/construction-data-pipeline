"""
Verifica estatísticas da aba fact_series.
"""
import os
import sys
import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.etl.sheets import SheetsLoader

# Configurar env vars
os.environ["GOOGLE_CREDENTIALS_PATH"] = "credentials.json"
os.environ["GOOGLE_SPREADSHEET_ID"] = "11-KC18ShMKXZOSbWvHcLHJwz3oDjexGQLb26xm2Wq4w"

# Carregar dados
loader = SheetsLoader()
data = loader.read_sheet("fact_series")

if len(data) <= 1:
    print("Aba vazia ou só tem header")
    sys.exit(0)

# Converter para DataFrame
df = pd.DataFrame(data[1:], columns=data[0])

print("=" * 70)
print("  ESTATISTICAS - fact_series")
print("=" * 70)
print()
print(f"Total de linhas:        {len(df)}")
print(f"Total de colunas:       {len(df.columns)}")
print()

# Estatísticas por série
print("Linhas por serie:")
series_counts = df.groupby('series_id').size().sort_values(ascending=False)
for series_id, count in series_counts.items():
    print(f"  {series_id:20s} {count:4d} linhas")

print()

# Verificar duplicatas
duplicates = df[df.duplicated(subset=['id_fato'], keep=False)]
print(f"Duplicatas:             {len(duplicates)} linhas")

if not duplicates.empty:
    print()
    print("  Duplicatas por serie:")
    dup_by_series = duplicates.groupby('series_id').size()
    for series_id, count in dup_by_series.items():
        print(f"    {series_id:20s} {count:4d} duplicatas")

print()

# Datas mais antigas e mais recentes
if 'data_referencia' in df.columns:
    print(f"Data mais antiga:       {df['data_referencia'].min()}")
    print(f"Data mais recente:      {df['data_referencia'].max()}")

print()

# Timestamps de criação
if 'created_at' in df.columns:
    print(f"Primeira ingestao:      {df['created_at'].min()}")
    print(f"Ultima ingestao:        {df['created_at'].max()}")

print()
print("=" * 70)
