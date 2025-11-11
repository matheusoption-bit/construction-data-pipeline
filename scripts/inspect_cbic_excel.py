"""
Script para inspecionar estrutura de arquivos Excel da CBIC.

Mostra primeiras linhas, colunas e estrutura do arquivo.

Uso:
    python scripts/inspect_cbic_excel.py
"""

from pathlib import Path
import pandas as pd


def inspect_excel(filepath: Path, max_rows: int = 20):
    """Inspeciona arquivo Excel mostrando estrutura."""
    
    print(f"\n{'='*70}")
    print(f"  📄 Inspecionando: {filepath.name}")
    print(f"{'='*70}\n")
    
    # Tentar diferentes skiprows
    for skiprows in range(0, 10):
        print(f"\n--- skiprows={skiprows} ---")
        
        try:
            df = pd.read_excel(filepath, skiprows=skiprows, nrows=max_rows)
            
            print(f"\nShape: {df.shape} (linhas x colunas)")
            print(f"\nColunas ({len(df.columns)}):")
            for i, col in enumerate(df.columns):
                print(f"  [{i}] {col}")
            
            print(f"\nPrimeiras 5 linhas:")
            print(df.head().to_string())
            
            # Procurar por "SC" nas colunas
            if "SC" in df.columns:
                print(f"\n✅ Coluna 'SC' encontrada no skiprows={skiprows}")
                print(f"\nDados da coluna SC:")
                print(df["SC"].head(10))
                break
            
        except Exception as e:
            print(f"Erro: {e}")
    
    print(f"\n{'='*70}\n")


def main():
    """Inspeciona arquivo CUB baixado."""
    
    filepath = Path("data/cache/cbic/tabela_06.A.06_BI_53.xlsx")
    
    if not filepath.exists():
        print(f"❌ Arquivo não encontrado: {filepath}")
        print("Execute primeiro: python scripts/test_cbic_client.py")
        return
    
    inspect_excel(filepath)


if __name__ == "__main__":
    main()
