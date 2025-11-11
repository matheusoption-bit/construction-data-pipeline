"""
Verifica dados na aba fact_cub_historico.
"""
import os
import pandas as pd
from src.etl.sheets import SheetsLoader

# Configurar env vars
os.environ["GOOGLE_CREDENTIALS_PATH"] = "credentials.json"
os.environ["GOOGLE_SPREADSHEET_ID"] = "11-KC18ShMKXZOSbWvHcLHJwz3oDjexGQLb26xm2Wq4w"

# Carregar dados
loader = SheetsLoader()
data = loader.read_sheet("fact_cub_historico")

# Converter para DataFrame
df = pd.DataFrame(data[1:], columns=data[0])  # Primeira linha é header

print("=" * 70)
print("  DADOS NA ABA fact_cub_historico")
print("=" * 70)
print(f"Total de linhas (sem header): {len(df)}")
print(f"Colunas: {list(df.columns)}")
print()
print("Primeiras 5 linhas:")
print(df.head().to_string())
print()
print("Ultimas 5 linhas:")
print(df.tail().to_string())
print()
print("=" * 70)
print(f"Periodo: {df['data_referencia'].min()} ate {df['data_referencia'].max()}")
print(f"UF: {df['uf'].unique()}")
print(f"Tipos CUB: {df['tipo_cub'].unique()}")
print(f"Valor min: R$ {pd.to_numeric(df['custo_m2'], errors='coerce').min():.2f}")
print(f"Valor max: R$ {pd.to_numeric(df['custo_m2'], errors='coerce').max():.2f}")
print(f"Valor medio: R$ {pd.to_numeric(df['custo_m2'], errors='coerce').mean():.2f}")
print("=" * 70)
