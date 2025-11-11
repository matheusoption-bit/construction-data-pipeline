"""
Script para limpar duplicatas em fact_series.

Mantém apenas a linha mais recente (por created_at) para cada id_fato.

Uso:
    python scripts/clean_fact_series_duplicates.py
    python scripts/clean_fact_series_duplicates.py --dry-run
"""

import argparse
import sys
import os
from datetime import datetime
import pandas as pd

# Adicionar path do projeto
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.etl.sheets import SheetsLoader
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


def main(dry_run: bool = False) -> int:
    """
    Remove duplicatas de fact_series.
    
    Args:
        dry_run: Se True, apenas mostra o que seria removido
    
    Returns:
        0 se sucesso, 1 se erro
    """
    print("\n" + "=" * 70)
    print("  LIMPEZA DE DUPLICATAS - fact_series")
    print("=" * 70 + "\n")
    
    if dry_run:
        print("  MODO DRY-RUN (nenhuma mudanca sera feita)\n")
    
    # Carregar dados
    print("Lendo dados de fact_series...")
    
    try:
        loader = SheetsLoader()
        data = loader.read_sheet("fact_series")
    except Exception as e:
        logger.error("failed_to_read_sheet", error=str(e))
        print(f"ERRO ao ler fact_series: {str(e)}\n")
        return 1
    
    if len(data) <= 1:
        print("Aba vazia ou so tem header\n")
        return 0
    
    # Converter para DataFrame
    df = pd.DataFrame(data[1:], columns=data[0])
    
    print(f"  {len(df)} linhas carregadas\n")
    
    # Identificar duplicatas
    print("Analisando duplicatas...\n")
    
    duplicates = df[df.duplicated(subset=['id_fato'], keep=False)]
    
    if duplicates.empty:
        print("  Nenhuma duplicata encontrada!\n")
        return 0
    
    # Contar duplicatas por série
    dup_by_series = duplicates.groupby('series_id').size().sort_values(ascending=False)
    
    print(f"  {len(duplicates)} linhas duplicadas encontradas:\n")
    
    for series_id, count in dup_by_series.items():
        print(f"  - {series_id}: {count} duplicatas")
    
    print()
    
    # Mostrar exemplos
    print("Exemplos de duplicatas (primeiras 10):\n")
    
    sample = duplicates.sort_values(['id_fato', 'created_at']).head(10)
    
    if not sample.empty:
        # Selecionar colunas relevantes
        cols_to_show = ['id_fato', 'series_id', 'data_referencia', 'created_at']
        cols_available = [col for col in cols_to_show if col in sample.columns]
        
        print(sample[cols_available].to_string(index=False))
    print()
    
    if dry_run:
        print("=" * 70)
        print("  DRY-RUN: Nenhuma mudanca foi feita")
        print("  Execute sem --dry-run para aplicar limpeza")
        print("=" * 70 + "\n")
        return 0
    
    # Remover duplicatas (manter mais recente)
    print("Removendo duplicatas (mantendo linha mais recente)...\n")
    
    # Usar método deduplicate_fact_series do SheetsLoader
    df_clean, removed_count = loader.deduplicate_fact_series(df, keep='last')
    
    print(f"  {removed_count} duplicatas removidas")
    print(f"  {len(df_clean)} linhas unicas restantes\n")
    
    # Reescrever aba
    print("Reescrevendo fact_series...\n")
    
    try:
        worksheet = loader._get_spreadsheet().worksheet("fact_series")
        worksheet.clear()
        
        # Preparar dados (header + rows)
        headers = [list(df_clean.columns)]
        rows = df_clean.values.tolist()
        
        # Converter NaN para string vazia
        rows = [
            ['' if pd.isna(val) else val for val in row]
            for row in rows
        ]
        
        # Escrever tudo de uma vez
        all_data = headers + rows
        worksheet.update(values=all_data, range_name='A1')
        
        print(f"  Aba reescrita com {len(df_clean)} linhas\n")
        
    except Exception as e:
        logger.error("failed_to_write_sheet", error=str(e))
        print(f"ERRO ao escrever fact_series: {str(e)}\n")
        return 1
    
    print("=" * 70)
    print("  LIMPEZA CONCLUIDA COM SUCESSO!")
    print("=" * 70 + "\n")
    
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Limpa duplicatas de fact_series")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Apenas mostra o que seria removido, sem fazer mudancas"
    )
    
    args = parser.parse_args()
    
    try:
        exit_code = main(dry_run=args.dry_run)
        sys.exit(exit_code)
    except Exception as e:
        logger.error("cleanup_failed", error=str(e), exc_info=True)
        print(f"\nERRO: {str(e)}\n")
        sys.exit(1)
