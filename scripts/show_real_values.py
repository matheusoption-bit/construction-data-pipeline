"""
Ver valores reais (não só '...') na sheet SC.
"""
import pandas as pd
from pathlib import Path

filepath = Path("data/cache/cbic/tabela_06.A.06_BI_53.xlsx")

print("=" * 70)
print("  VALORES REAIS - SHEET SC")
print("=" * 70)

# Ler com skiprows=7
df = pd.read_excel(filepath, sheet_name="SC", skiprows=7)

print(f"\nShape: {df.shape}")
print(f"Colunas: {list(df.columns)}")

# Configurar pandas para mostrar valores completos
pd.set_option('display.max_colwidth', None)
pd.set_option('display.width', None)

print("\n" + "=" * 70)
print("  PRIMEIRAS 30 LINHAS (SEM TRUNCAR)")
print("=" * 70)

# Mostrar linha por linha
for i in range(min(30, len(df))):
    row = df.iloc[i]
    print(f"\n[Linha {i}]")
    for col_idx, (col_name, value) in enumerate(row.items()):
        print(f"  [{col_idx}] {col_name!r:30s} = {value!r}")

print("\n" + "=" * 70)
print("  ANÁLISE: QUAL COLUNA TEM OS VALORES R$/m²?")
print("=" * 70)

# Tentar converter cada coluna para float
for col in df.columns:
    print(f"\nColuna: {col!r}")
    print(f"  Tipo: {df[col].dtype}")
    print(f"  Valores únicos (10 primeiros): {df[col].dropna().unique()[:10].tolist()}")
    
    # Tentar conversão numérica
    try:
        numeric = pd.to_numeric(df[col], errors='coerce')
        valid = numeric.dropna()
        if len(valid) > 0:
            print(f"  ✓ NUMÉRICA! Min={valid.min():.2f}, Max={valid.max():.2f}, Média={valid.mean():.2f}")
    except:
        print(f"  ✗ Não numérica")
