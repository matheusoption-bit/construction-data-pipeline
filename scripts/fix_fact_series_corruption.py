"""
Script para limpar dados corrompidos de fact_series.

Remove:
1. Valores vazios/nulos
2. Valores zero (suspeitos)
3. Valores absurdos (> 1 milhão ou < -1000)
4. Datas futuras

Uso:
    python scripts/fix_fact_series_corruption.py
    python scripts/fix_fact_series_corruption.py --dry-run
"""

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path

# Adicionar path do projeto
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from src.etl.sheets import SheetsLoader
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

# Configurar env vars
os.environ["GOOGLE_CREDENTIALS_PATH"] = "credentials.json"
os.environ["GOOGLE_SPREADSHEET_ID"] = "11-KC18ShMKXZOSbWvHcLHJwz3oDjexGQLb26xm2Wq4w"


def analyze_corruption(df: pd.DataFrame) -> dict:
    """
    Analisa tipos de corrupção nos dados.
    
    Returns:
        Dict com estatísticas de corrupção
    """
    stats = {
        "total_rows": len(df),
        "valores_vazios": 0,
        "valores_zero": 0,
        "valores_absurdos": 0,
        "datas_futuras": 0
    }
    
    # Converter valor para numérico
    df['valor_num'] = pd.to_numeric(df['valor'], errors='coerce')
    df['data_ref_dt'] = pd.to_datetime(df['data_referencia'], errors='coerce')
    
    hoje = pd.Timestamp.now()
    
    # Contar problemas
    stats["valores_vazios"] = df['valor_num'].isna().sum()
    stats["valores_zero"] = (df['valor_num'] == 0).sum()
    stats["valores_absurdos"] = (
        (df['valor_num'] > 1_000_000) | (df['valor_num'] < -1000)
    ).sum()
    stats["datas_futuras"] = (df['data_ref_dt'] > hoje).sum()
    
    return stats


def clean_corrupted_data(dry_run: bool = False) -> dict:
    """
    Remove dados corrompidos de fact_series.
    
    Args:
        dry_run: Se True, apenas mostra o que seria removido
    
    Returns:
        Dict com resultado da limpeza
    """
    print("\n" + "=" * 80)
    print("  🧹 LIMPEZA DE DADOS CORROMPIDOS - fact_series")
    print("=" * 80 + "\n")
    
    if dry_run:
        print("  ⚠️  MODO DRY-RUN (nenhuma mudança será feita)\n")
    
    # Carregar dados
    print("📋 Carregando dados de fact_series...")
    loader = SheetsLoader()
    df = loader.read_fact_series()
    
    if df.empty:
        print("❌ fact_series está vazio!\n")
        return {"status": "error", "message": "fact_series vazio"}
    
    print(f"   Total de linhas: {len(df)}\n")
    
    # Analisar corrupção
    print("🔍 Analisando corrupção...")
    stats_before = analyze_corruption(df)
    
    print(f"\n📊 ESTATÍSTICAS DE CORRUPÇÃO:")
    print(f"   • Valores vazios/nulos:  {stats_before['valores_vazios']}")
    print(f"   • Valores zero:          {stats_before['valores_zero']}")
    print(f"   • Valores absurdos:      {stats_before['valores_absurdos']}")
    print(f"   • Datas futuras:         {stats_before['datas_futuras']}")
    print(f"   • Total corrompido:      {stats_before['valores_vazios'] + stats_before['valores_zero'] + stats_before['valores_absurdos'] + stats_before['datas_futuras']}")
    
    # Limpar dados
    print("\n🧹 Aplicando filtros de limpeza...")
    
    # Converter colunas
    df['valor_num'] = pd.to_numeric(df['valor'], errors='coerce')
    df['data_ref_dt'] = pd.to_datetime(df['data_referencia'], errors='coerce')
    
    hoje = pd.Timestamp.now()
    
    # Aplicar filtros
    df_clean = df[
        (df['valor_num'].notna()) &          # Não nulo
        (df['valor_num'] != 0) &             # Não zero
        (df['valor_num'] < 1_000_000) &      # Não absurdo (positivo)
        (df['valor_num'] > -1000) &          # Não absurdo (negativo)
        (df['data_ref_dt'] <= hoje)          # Não futuro
    ].copy()
    
    # Remover colunas temporárias
    df_clean = df_clean.drop(columns=['valor_num', 'data_ref_dt'])
    
    # Estatísticas
    removed = len(df) - len(df_clean)
    
    print(f"\n✂️  RESULTADO DA LIMPEZA:")
    print(f"   • Linhas originais:  {len(df)}")
    print(f"   • Linhas removidas:  {removed} ({removed/len(df)*100:.1f}%)")
    print(f"   • Linhas limpas:     {len(df_clean)}")
    
    if dry_run:
        print("\n" + "=" * 80)
        print("  ⚠️  DRY-RUN COMPLETO - Nenhuma mudança foi feita")
        print("  Execute sem --dry-run para aplicar limpeza")
        print("=" * 80 + "\n")
        
        return {
            "status": "dry_run",
            "original_rows": len(df),
            "removed_rows": removed,
            "clean_rows": len(df_clean)
        }
    
    # Reescrever aba
    print("\n💾 Reescrevendo fact_series...")
    
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
        
        print(f"   ✅ Aba reescrita com {len(df_clean)} linhas limpas\n")
        
        logger.info(
            "fact_series_cleaned",
            original_rows=len(df),
            removed_rows=removed,
            clean_rows=len(df_clean),
            removal_percentage=round(removed/len(df)*100, 2)
        )
    
    except Exception as e:
        logger.error("cleanup_failed", error=str(e), exc_info=True)
        print(f"❌ ERRO ao reescrever: {str(e)}\n")
        return {
            "status": "error",
            "error": str(e)
        }
    
    print("=" * 80)
    print("  ✅ LIMPEZA CONCLUÍDA COM SUCESSO!")
    print("=" * 80 + "\n")
    
    return {
        "status": "success",
        "original_rows": len(df),
        "removed_rows": removed,
        "clean_rows": len(df_clean),
        "removal_percentage": round(removed/len(df)*100, 2)
    }


def main():
    """Executa limpeza de dados corrompidos."""
    
    parser = argparse.ArgumentParser(
        description="Limpa dados corrompidos de fact_series"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Apenas mostra o que seria removido, sem fazer mudanças"
    )
    
    args = parser.parse_args()
    
    try:
        result = clean_corrupted_data(dry_run=args.dry_run)
        
        if result["status"] == "success":
            sys.exit(0)
        elif result["status"] == "dry_run":
            sys.exit(0)
        else:
            sys.exit(1)
    
    except Exception as e:
        logger.error("cleanup_script_failed", error=str(e), exc_info=True)
        print(f"\n❌ ERRO: {str(e)}\n")
        sys.exit(1)


if __name__ == "__main__":
    main()
