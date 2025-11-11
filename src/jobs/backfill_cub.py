"""
Job de backfill de dados CUB históricos.

Busca série histórica completa do CBICdados e popula fact_cub_historico.

Uso:
    python -m src.jobs.backfill_cub
    python -m src.jobs.backfill_cub --uf SC --force
"""

import sys
import argparse
import uuid
from datetime import datetime
from typing import Dict, Any

import pandas as pd

from src.clients.cbic import CBICClient
from src.etl.sheets import SheetsLoader
from src.utils.logger import setup_logger, log_execution

logger = setup_logger(__name__)


@log_execution(logger)
def validate_cub_data(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Valida dados de CUB.
    
    Validações:
    1. Valores > 0
    2. Sem gaps mensais excessivos
    3. Variação MoM razoável (-5% a +10%)
    
    Args:
        df: DataFrame com dados de CUB
    
    Returns:
        Dict com resultado da validação:
        - is_valid: bool
        - issues: List[str]
        - total_rows: int
        - date_range: str
        - value_range: str
    """
    issues = []
    
    # Validação 1: Valores positivos
    invalid_values = df[df['custo_m2'] <= 0]
    if not invalid_values.empty:
        issues.append(f"Valores inválidos: {len(invalid_values)} linhas")
    
    # Validação 2: Gaps mensais
    df_sorted = df.sort_values('data_referencia').copy()
    dates = pd.to_datetime(df_sorted['data_referencia'])
    
    date_diffs = dates.diff().dt.days
    gaps = date_diffs[(date_diffs > 35) | (date_diffs < 25)]  # Aceitar 25-35 dias
    
    if not gaps.empty:
        issues.append(f"Gaps mensais encontrados: {len(gaps)} casos")
    
    # Validação 3: Variação MoM
    df_sorted['variacao_mom'] = df_sorted['custo_m2'].pct_change() * 100
    
    outliers = df_sorted[
        (df_sorted['variacao_mom'] < -5) | 
        (df_sorted['variacao_mom'] > 10)
    ]
    
    if not outliers.empty:
        issues.append(f"Variações MoM anormais: {len(outliers)} casos")
        logger.warning(
            "mom_outliers_found",
            count=len(outliers),
            samples=outliers[['data_referencia', 'custo_m2', 'variacao_mom']].head(3).to_dict('records')
        )
    
    return {
        "is_valid": len(issues) == 0,
        "issues": issues,
        "total_rows": len(df),
        "date_range": f"{dates.min().date()} até {dates.max().date()}",
        "value_range": f"R$ {df['custo_m2'].min():.2f} - R$ {df['custo_m2'].max():.2f}"
    }


@log_execution(logger)
def main(uf: str = "SC", force: bool = False) -> Dict[str, Any]:
    """
    Executa backfill de dados CUB.
    
    Args:
        uf: Estado (padrão: SC)
        force: Forçar reprocessamento (limpar existentes)
    
    Returns:
        Resumo da execução com status, métricas e issues
    """
    logger.info("backfill_cub_started", uf=uf, force=force)
    
    start_time = datetime.now()
    exec_id = f"backfill_cub_{uf}_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{str(uuid.uuid4())[:8]}"
    
    # Inicializar clientes
    try:
        logger.info("initializing_clients")
        
        cbic_client = CBICClient()
        sheets_loader = SheetsLoader()
        
        logger.info("clients_initialized")
    
    except Exception as e:
        logger.error("client_initialization_failed", error=str(e), exc_info=True)
        return {
            "status": "error",
            "error": str(e),
            "exec_id": exec_id
        }
    
    # Buscar dados históricos
    try:
        logger.info("fetching_cub_data", uf=uf)
        
        df_cub = cbic_client.fetch_cub_historical(uf=uf)
        
        if df_cub.empty:
            logger.warning("no_data_returned", uf=uf)
            return {
                "status": "error",
                "error": "Nenhum dado retornado",
                "exec_id": exec_id
            }
        
        logger.info("cub_data_fetched", rows=len(df_cub), uf=uf)
    
    except Exception as e:
        logger.error("fetch_failed", uf=uf, error=str(e), exc_info=True)
        return {
            "status": "error",
            "error": str(e),
            "exec_id": exec_id
        }
    
    # Validar dados
    validation = validate_cub_data(df_cub)
    
    logger.info(
        "validation_completed",
        is_valid=validation['is_valid'],
        issues=validation['issues']
    )
    
    # Preparar dados para escrita
    df_cub['id_fato'] = df_cub.apply(
        lambda row: f"CUB_{row['uf']}_{row['tipo_cub'].replace('-', '')}_{row['data_referencia'][:7]}",
        axis=1
    )
    
    # Reordenar colunas
    columns_order = [
        'id_fato',
        'uf',
        'tipo_cub',
        'data_referencia',
        'custo_m2',
        'fonte_url',
        'checksum_dados',
        'metodo_versao',
        'created_at'
    ]
    
    df_final = df_cub[columns_order]
    
    # Escrever no Google Sheets
    try:
        logger.info("writing_to_sheets", rows=len(df_final), uf=uf)
        
        # Criar aba se não existir
        headers = columns_order
        sheets_loader.create_sheet_if_not_exists("fact_cub_historico", headers=headers)
        
        # Se force=True, limpar dados existentes do estado
        if force:
            logger.info("force_mode_clearing_existing_data", uf=uf)
            # TODO: Implementar limpeza seletiva por UF
            # Por enquanto, apenas adiciona
        
        # Converter para lista de listas
        rows = df_final.values.tolist()
        
        # Escrever dados
        sheets_loader.append_to_sheet("fact_cub_historico", rows)
        
        logger.info("data_written", rows=len(rows), uf=uf)
    
    except Exception as e:
        logger.error("write_failed", uf=uf, error=str(e), exc_info=True)
        return {
            "status": "error",
            "error": str(e),
            "exec_id": exec_id
        }
    
    # Registrar log de execução
    execution_time = (datetime.now() - start_time).total_seconds()
    
    try:
        sheets_loader.write_ingestion_log(
            exec_id=exec_id,
            fonte=f"cbic_cub_{uf}",
            status="success" if validation['is_valid'] else "partial",
            linhas=len(df_final),
            erros=validation['issues'] if not validation['is_valid'] else None
        )
    except Exception as e:
        logger.warning("failed_to_write_log", error=str(e))
    
    # Resumo
    summary = {
        "status": "success" if validation['is_valid'] else "partial",
        "exec_id": exec_id,
        "uf": uf,
        "linhas_inseridas": len(df_final),
        "periodo": validation['date_range'],
        "valores": validation['value_range'],
        "validation_issues": validation['issues'],
        "execution_time": round(execution_time, 2)
    }
    
    logger.info("backfill_completed", **summary)
    
    return summary


def print_summary(summary: Dict[str, Any]) -> None:
    """Imprime resumo formatado da execução."""
    print("\n" + "=" * 70)
    print("  BACKFILL CUB - RESUMO")
    print("=" * 70)
    
    # Se houve erro, mostrar mensagem simplificada
    if summary.get("status") == "error":
        print(f"Status:              ERROR")
        print(f"Erro:                {summary.get('error', 'Desconhecido')}")
        print(f"Exec ID:             {summary.get('exec_id', 'N/A')}")
        print("=" * 70 + "\n")
        return
    
    print(f"Status:              {summary['status'].upper()}")
    print(f"UF:                  {summary['uf']}")
    print(f"Linhas Inseridas:    {summary['linhas_inseridas']}")
    print(f"Periodo:             {summary['periodo']}")
    print(f"Valores:             {summary['valores']}")
    print(f"Tempo de Execucao:   {summary['execution_time']}s")
    
    if summary.get('validation_issues'):
        print("-" * 70)
        print("  Issues de Qualidade:")
        for issue in summary['validation_issues']:
            print(f"  - {issue}")
    
    print("=" * 70 + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill de dados CUB históricos")
    parser.add_argument("--uf", default="SC", help="Estado (padrão: SC)")
    parser.add_argument("--force", action="store_true", help="Forçar reprocessamento")
    
    args = parser.parse_args()
    
    try:
        summary = main(uf=args.uf, force=args.force)
        
        print_summary(summary)
        
        sys.exit(0 if summary['status'] == "success" else 1)
    
    except Exception as e:
        logger.error("backfill_failed", error=str(e), exc_info=True)
        print(f"\nBACKFILL FALHOU: {str(e)}\n")
        sys.exit(1)
