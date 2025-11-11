"""
Script para listar todas as sheets/abas de um arquivo Excel.

Uso:
    python scripts/list_excel_sheets.py
"""

from pathlib import Path
import pandas as pd


def list_sheets(filepath: Path):
    """Lista todas as sheets do Excel."""
    
    print(f"\n{'='*70}")
    print(f"  📋 Sheets no arquivo: {filepath.name}")
    print(f"{'='*70}\n")
    
    try:
        excel_file = pd.ExcelFile(filepath)
        
        print(f"Total de sheets: {len(excel_file.sheet_names)}\n")
        
        for i, sheet_name in enumerate(excel_file.sheet_names, 1):
            print(f"  [{i}] {sheet_name}")
            
            # Ler primeiras linhas
            try:
                df = pd.read_excel(filepath, sheet_name=sheet_name, nrows=5)
                print(f"      Shape: {df.shape}")
                print(f"      Colunas: {list(df.columns[:5])}...\n")
            except Exception as e:
                print(f"      Erro ao ler: {e}\n")
        
    except Exception as e:
        print(f"❌ Erro: {e}")
    
    print(f"{'='*70}\n")


def main():
    """Lista sheets do arquivo CUB."""
    
    filepath = Path("data/cache/cbic/tabela_06.A.06_BI_53.xlsx")
    
    if not filepath.exists():
        print(f"❌ Arquivo não encontrado: {filepath}")
        return
    
    list_sheets(filepath)


if __name__ == "__main__":
    main()
