"""
Job de ingestão diária de séries temporais do Banco Central do Brasil.

Este job busca dados de séries econômicas da API do BCB, processa os dados,
executa validações de qualidade e carrega no Google Sheets.

Séries coletadas:
- Selic (432)
- TR (226)
- USD/BRL (1)
- IPCA (433)
- IGP-M (189)
- Poupança (7478)
- INPC (4189)
- Crédito PF (4390)
- Produção Construção (1207)
- Estoque Crédito Habitacional (24364)

Uso:
    python -m src.jobs.daily_bcb
"""

import sys
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Any

import pandas as pd

from src.clients.bcb import BCBClient
from src.etl.sheets import SheetsLoader
from src.etl.quality import run_quality_checks, calculate_mom_yoy
from src.utils.logger import setup_logger, log_execution

# Configurar logger
logger = setup_logger(__name__)

# Mapeamento de séries BCB (nome_serie: codigo_sgs)
SERIES_MAP = {
    'BCB_SGS_432': 432,      # Selic
    'BCB_SGS_226': 226,      # TR
    'BCB_SGS_1': 1,          # USD/BRL
    'BCB_SGS_433': 433,      # IPCA
    'BCB_SGS_189': 189,      # IGP-M
    'BCB_SGS_7478': 7478,    # Poupança
    'BCB_SGS_4189': 4189,    # INPC
    'BCB_SGS_4390': 4390,    # Crédito PF
    'BCB_SGS_1207': 1207,    # Produção Construção
    'BCB_SGS_24364': 24364   # Estoque Crédito Habitacional
}


@log_execution(logger)
def process_series(
    series_id: str,
    series_code: int,
    bcb_client: BCBClient,
    sheets_loader: SheetsLoader,
    exec_id: str,
    months_back: int = 12
) -> Dict[str, Any]:
    """
    Processa uma série temporal individual do BCB.
    
    Args:
        series_id: Identificador da série (ex: 'BCB_SGS_432')
        series_code: Código SGS da série no BCB
        bcb_client: Cliente BCB configurado
        sheets_loader: Loader de Google Sheets configurado
        exec_id: ID da execução para rastreamento
        months_back: Número de meses para buscar (padrão: 12)
    
    Returns:
        Dicionário com resultado do processamento:
        - status: "success", "error" ou "partial"
        - linhas_processadas: Número de linhas processadas
        - quality_flags: Número de flags de qualidade encontrados
        - error: Mensagem de erro (se houver)
    """
    logger.info(
        "processing_series_started",
        series_id=series_id,
        series_code=series_code,
        exec_id=exec_id
    )
    
    try:
        # Calcular datas (últimos N meses)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=months_back * 30)
        
        start_date_str = start_date.strftime("%d/%m/%Y")
        end_date_str = end_date.strftime("%d/%m/%Y")
        
        logger.info(
            "fetching_series_data",
            series_id=series_id,
            start_date=start_date_str,
            end_date=end_date_str
        )
        
        # Buscar dados do BCB
        raw_data = bcb_client.fetch_series(
            series_code,
            start_date=start_date_str,
            end_date=end_date_str
        )
        
        if not raw_data:
            logger.warning(
                "no_data_returned",
                series_id=series_id,
                series_code=series_code
            )
            return {
                "status": "error",
                "linhas_processadas": 0,
                "quality_flags": 0,
                "error": "Nenhum dado retornado pela API"
            }
        
        # Converter para DataFrame
        df = pd.DataFrame(raw_data)
        df.columns = ['data_referencia', 'valor']
        
        logger.info(
            "data_fetched",
            series_id=series_id,
            rows_fetched=len(df)
        )
        
        # Calcular variações MoM e YoY
        df_with_variations = calculate_mom_yoy(df, value_col='valor')
        
        # Renomear coluna para compatibilidade com quality checks
        df_check = df_with_variations.rename(columns={
            'data_referencia': 'date',
            'valor': 'value'
        })
        
        # Executar quality checks
        logger.info(
            "running_quality_checks",
            series_id=series_id
        )
        
        quality_flags = run_quality_checks(df_check, series_id)
        
        logger.info(
            "quality_checks_completed",
            series_id=series_id,
            flags_found=len(quality_flags)
        )
        
        # Escrever dados em fact_series
        logger.info(
            "writing_to_sheets",
            series_id=series_id,
            rows_to_write=len(df_with_variations)
        )
        
        sheets_loader.write_fact_series(
            series_id=series_id,
            data=df_with_variations,
            exec_id=exec_id
        )
        
        # Escrever quality flags se houver
        if quality_flags:
            logger.info(
                "writing_quality_flags",
                series_id=series_id,
                flags_count=len(quality_flags)
            )
            
            # Preparar dados de flags para sheets
            flags_rows = []
            for flag in quality_flags:
                flags_rows.append([
                    flag['series_id'],
                    flag['data_referencia'],
                    flag['tipo_flag'],
                    flag['severidade'],
                    flag['valor_observado'],
                    flag['desvio_padrao'],
                    flag['detalhes']
                ])
            
            # Criar aba de quality flags se não existir
            headers = [
                'series_id',
                'data_referencia',
                'tipo_flag',
                'severidade',
                'valor_observado',
                'desvio_padrao',
                'detalhes'
            ]
            sheets_loader.create_sheet_if_not_exists('_quality_flags', headers=headers)
            
            # Escrever flags
            sheets_loader.append_to_sheet('_quality_flags', flags_rows)
        
        logger.info(
            "series_processed_successfully",
            series_id=series_id,
            rows_written=len(df_with_variations),
            quality_flags=len(quality_flags)
        )
        
        return {
            "status": "success",
            "linhas_processadas": len(df_with_variations),
            "quality_flags": len(quality_flags),
            "error": None
        }
    
    except Exception as e:
        logger.error(
            "series_processing_failed",
            series_id=series_id,
            series_code=series_code,
            error=str(e),
            error_type=type(e).__name__,
            exc_info=True
        )
        
        return {
            "status": "error",
            "linhas_processadas": 0,
            "quality_flags": 0,
            "error": str(e)
        }


@log_execution(logger)
def main() -> Dict[str, Any]:
    """
    Função principal do job de ingestão diária BCB.
    
    Executa o pipeline completo:
    1. Inicializa clientes (BCB, Sheets)
    2. Gera ID de execução único
    3. Processa cada série do SERIES_MAP
    4. Registra logs de execução
    5. Retorna resumo da execução
    
    Returns:
        Dicionário com resumo da execução:
        - status: "success" (todas ok), "partial" (algumas falharam), "error" (todas falharam)
        - exec_id: ID da execução
        - total_series: Total de séries processadas
        - successful_series: Séries processadas com sucesso
        - failed_series: Séries que falharam
        - total_linhas: Total de linhas processadas
        - total_quality_flags: Total de flags de qualidade
        - erros: Lista de erros por série
        - execution_time: Tempo total de execução
    """
    logger.info(
        "daily_bcb_job_started",
        series_count=len(SERIES_MAP)
    )
    
    start_time = datetime.now()
    
    # Gerar ID de execução único
    exec_id = f"daily_bcb_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{str(uuid.uuid4())[:8]}"
    
    logger.info(
        "execution_id_generated",
        exec_id=exec_id
    )
    
    # Inicializar clientes
    try:
        logger.info("initializing_clients")
        
        bcb_client = BCBClient()
        sheets_loader = SheetsLoader()
        
        logger.info("clients_initialized")
    
    except Exception as e:
        logger.error(
            "client_initialization_failed",
            error=str(e),
            error_type=type(e).__name__,
            exc_info=True
        )
        
        return {
            "status": "error",
            "exec_id": exec_id,
            "total_series": 0,
            "successful_series": 0,
            "failed_series": 0,
            "total_linhas": 0,
            "total_quality_flags": 0,
            "erros": [f"Falha ao inicializar clientes: {str(e)}"],
            "execution_time": 0
        }
    
    # Processar cada série
    results = {}
    total_linhas = 0
    total_quality_flags = 0
    erros = []
    
    for series_id, series_code in SERIES_MAP.items():
        logger.info(
            "processing_series",
            series_id=series_id,
            series_code=series_code,
            progress=f"{len(results) + 1}/{len(SERIES_MAP)}"
        )
        
        result = process_series(
            series_id=series_id,
            series_code=series_code,
            bcb_client=bcb_client,
            sheets_loader=sheets_loader,
            exec_id=exec_id
        )
        
        results[series_id] = result
        
        if result["status"] == "success":
            total_linhas += result["linhas_processadas"]
            total_quality_flags += result["quality_flags"]
        else:
            erros.append(f"{series_id}: {result['error']}")
    
    # Calcular estatísticas
    successful_series = sum(1 for r in results.values() if r["status"] == "success")
    failed_series = len(SERIES_MAP) - successful_series
    
    # Determinar status geral
    if failed_series == 0:
        overall_status = "success"
    elif successful_series == 0:
        overall_status = "error"
    else:
        overall_status = "partial"
    
    # Calcular tempo de execução
    execution_time = (datetime.now() - start_time).total_seconds()
    
    # Registrar log de execução no Sheets
    try:
        logger.info(
            "writing_ingestion_log",
            exec_id=exec_id,
            status=overall_status
        )
        
        sheets_loader.write_ingestion_log(
            exec_id=exec_id,
            fonte="bcb_daily",
            status=overall_status,
            linhas=total_linhas,
            erros=erros if erros else None
        )
        
        logger.info("ingestion_log_written")
    
    except Exception as e:
        logger.error(
            "failed_to_write_ingestion_log",
            error=str(e),
            error_type=type(e).__name__
        )
        # Não falhar o job por erro ao escrever log
    
    # Criar resumo
    summary = {
        "status": overall_status,
        "exec_id": exec_id,
        "total_series": len(SERIES_MAP),
        "successful_series": successful_series,
        "failed_series": failed_series,
        "total_linhas": total_linhas,
        "total_quality_flags": total_quality_flags,
        "erros": erros,
        "execution_time": round(execution_time, 2)
    }
    
    logger.info(
        "daily_bcb_job_completed",
        **summary
    )
    
    return summary


def print_summary(summary: Dict[str, Any]) -> None:
    """
    Imprime resumo da execução de forma legível.
    
    Args:
        summary: Dicionário com resumo da execução
    """
    print("\n" + "=" * 70)
    print("RESUMO DA EXECUÇÃO - JOB DAILY BCB")
    print("=" * 70)
    print(f"Execution ID:        {summary['exec_id']}")
    print(f"Status:              {summary['status'].upper()}")
    print(f"Tempo de Execução:   {summary['execution_time']}s")
    print("-" * 70)
    print(f"Total de Séries:     {summary['total_series']}")
    print(f"Séries com Sucesso:  {summary['successful_series']}")
    print(f"Séries com Falha:    {summary['failed_series']}")
    print("-" * 70)
    print(f"Total de Linhas:     {summary['total_linhas']}")
    print(f"Quality Flags:       {summary['total_quality_flags']}")
    
    if summary['erros']:
        print("-" * 70)
        print("ERROS:")
        for erro in summary['erros']:
            print(f"  - {erro}")
    
    print("=" * 70 + "\n")


if __name__ == "__main__":
    """
    Ponto de entrada do job.
    
    Executa main() e sai com código apropriado:
    - 0: Sucesso completo
    - 1: Falha ou sucesso parcial
    """
    try:
        # Executar job
        summary = main()
        
        # Imprimir resumo
        print_summary(summary)
        
        # Determinar exit code
        if summary["status"] == "success":
            logger.info("job_completed_successfully")
            sys.exit(0)
        else:
            logger.warning(
                "job_completed_with_issues",
                status=summary["status"]
            )
            sys.exit(1)
    
    except Exception as e:
        logger.error(
            "job_failed_with_exception",
            error=str(e),
            error_type=type(e).__name__,
            exc_info=True
        )
        
        print(f"\n❌ JOB FALHOU: {str(e)}\n")
        sys.exit(1)
