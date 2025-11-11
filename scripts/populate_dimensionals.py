"""
Script para popular tabelas dimensionais com dados iniciais.

Popula:
- dim_series (10 séries BCB)
- dim_topografia (10 tipos de terreno)
- dim_metodo (8 métodos construtivos)
- dim_projetos (12 tipos de projetos)

Uso:
    python scripts/populate_dimensionals.py
    python scripts/populate_dimensionals.py --table dim_series
"""

import argparse
import sys
import os
from pathlib import Path
import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.etl.sheets import SheetsLoader
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


def populate_table(loader: SheetsLoader, table_name: str, csv_file: Path) -> int:
    """
    Popula uma tabela dimensional a partir de CSV.
    
    Args:
        loader: SheetsLoader instance
        table_name: Nome da aba (ex: "dim_series")
        csv_file: Path do arquivo CSV
    
    Returns:
        Número de linhas inseridas
    """
    print(f"\n{'='*70}")
    print(f"  Populando {table_name}")
    print(f"{'='*70}\n")
    
    # Ler CSV
    print(f"Lendo {csv_file.name}...")
    
    if not csv_file.exists():
        print(f"  ERRO: Arquivo nao encontrado: {csv_file}")
        return 0
    
    df = pd.read_csv(csv_file)
    
    print(f"  {len(df)} linhas carregadas")
    print(f"  Colunas: {', '.join(df.columns)}\n")
    
    # Criar aba se não existir
    print(f"Criando aba {table_name}...")
    headers = list(df.columns)
    loader.create_sheet_if_not_exists(table_name, headers=headers)
    print(f"  OK\n")
    
    # Verificar se já tem dados
    print(f"Verificando dados existentes...")
    existing_data = loader.read_sheet(table_name)
    
    if len(existing_data) > 1:
        print(f"  Aba ja tem {len(existing_data) - 1} linhas")
        print(f"  Pulando (use --force para sobrescrever)\n")
        return 0
    
    print(f"  Aba vazia, prosseguindo...\n")
    
    # Escrever dados
    print(f"Escrevendo dados...")
    rows = df.values.tolist()
    
    # Converter NaN para string vazia
    rows = [
        ['' if pd.isna(val) else val for val in row]
        for row in rows
    ]
    
    loader.append_to_sheet(table_name, rows)
    
    print(f"  {len(rows)} linhas escritas\n")
    print(f"{'='*70}")
    print(f"  {table_name} populado com sucesso!")
    print(f"{'='*70}\n")
    
    return len(rows)


def main(table: str = None, force: bool = False) -> int:
    """
    Popula tabelas dimensionais.
    
    Args:
        table: Nome específico de tabela ou None para todas
        force: Sobrescrever dados existentes
    
    Returns:
        0 se sucesso, 1 se erro
    """
    print("\n" + "="*70)
    print("  POPULANDO TABELAS DIMENSIONAIS")
    print("="*70 + "\n")
    
    # Configurar paths
    configs_dir = Path(__file__).parent.parent / "configs"
    
    # Definir tabelas e arquivos CSV
    tables = {
        "dim_series": configs_dir / "dim_series_initial.csv",
        "dim_topografia": configs_dir / "dim_topografia_initial.csv",
        "dim_metodo": configs_dir / "dim_metodo_initial.csv",
        "dim_projetos": configs_dir / "dim_projetos_initial.csv"
    }
    
    # Filtrar tabela específica se solicitado
    if table:
        if table not in tables:
            print(f"ERRO: Tabela '{table}' nao encontrada")
            print(f"Tabelas disponiveis: {', '.join(tables.keys())}\n")
            return 1
        tables = {table: tables[table]}
    
    # Inicializar loader
    try:
        loader = SheetsLoader()
    except Exception as e:
        logger.error("failed_to_initialize_loader", error=str(e))
        print(f"ERRO ao inicializar SheetsLoader: {str(e)}\n")
        return 1
    
    # Popular tabelas
    total_rows = 0
    
    for table_name, csv_file in tables.items():
        try:
            rows = populate_table(loader, table_name, csv_file)
            total_rows += rows
        except Exception as e:
            logger.error("failed_to_populate_table", table=table_name, error=str(e))
            print(f"\nERRO ao popular {table_name}: {str(e)}\n")
            return 1
    
    print("\n" + "="*70)
    print("  RESUMO")
    print("="*70)
    print(f"Tabelas populadas:   {len(tables)}")
    print(f"Total de linhas:     {total_rows}")
    print("="*70 + "\n")
    
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Popula tabelas dimensionais com dados iniciais"
    )
    parser.add_argument(
        "--table",
        choices=["dim_series", "dim_topografia", "dim_metodo", "dim_projetos"],
        help="Popular apenas uma tabela específica"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Sobrescrever dados existentes"
    )
    
    args = parser.parse_args()
    
    try:
        exit_code = main(table=args.table, force=args.force)
        sys.exit(exit_code)
    except Exception as e:
        logger.error("populate_failed", error=str(e), exc_info=True)
        print(f"\nERRO: {str(e)}\n")
        sys.exit(1)
