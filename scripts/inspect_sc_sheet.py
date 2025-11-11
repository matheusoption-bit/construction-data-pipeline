"""
Inspeção detalhada da sheet SC do arquivo CBIC.
"""
import pandas as pd
from pathlib import Path

# Arquivo baixado
filepath = Path("data/cache/cbic/tabela_06.A.06_BI_53.xlsx")

print("=" * 70)
print("  📊 INSPEÇÃO DETALHADA - SHEET SC")
print("=" * 70)

# Ler sheet SC com diferentes skiprows
for skiprows in [0, 5, 6, 7, 8]:
    try:
        df = pd.read_excel(filepath, sheet_name="SC", skiprows=skiprows)
        
        print(f"\n{'='*70}")
        print(f"  SKIPROWS = {skiprows}")
        print(f"{'='*70}")
        print(f"Shape: {df.shape}")
        print(f"\nPrimeiras 5 colunas:")
        for i, col in enumerate(df.columns[:5]):
            print(f"  [{i}] {repr(col)}")
        
        print(f"\nPrimeiras 10 linhas da coluna 0:")
        for i, val in enumerate(df.iloc[:10, 0]):
            print(f"  [{i}] {repr(val)}")
        
        print(f"\nPrimeiras 10 linhas da coluna 1 (se existir):")
        if len(df.columns) > 1:
            for i, val in enumerate(df.iloc[:10, 1]):
                print(f"  [{i}] {repr(val)}")
        
        print(f"\nAmostra dos dados (5 primeiras linhas):")
        print(df.head())
        
    except Exception as e:
        print(f"\nSKIPROWS = {skiprows}: ERRO - {e}")

print("\n" + "=" * 70)
print("  🔍 ANÁLISE COMPLETA")
print("=" * 70)

# Ler com skiprows=7 (parece ser o melhor)
df = pd.read_excel(filepath, sheet_name="SC", skiprows=7)

print(f"\nShape final: {df.shape}")
print(f"\nTodas as colunas:")
for i, col in enumerate(df.columns):
    print(f"  [{i}] {repr(col)}")

print(f"\nTipos de dados:")
print(df.dtypes)

print(f"\nPrimeiras 20 linhas completas:")
print(df.head(20).to_string())
