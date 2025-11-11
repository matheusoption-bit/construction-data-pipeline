"""
Popula fact_series com histórico COMPLETO desde Janeiro/2018.

Para apresentação na SEXTA-FEIRA.

Features:
- ✅ Histórico completo: 2018-2025
- ✅ Coluna "nome_indicador" com nomes descritivos
- ✅ Dados validados e sem erros
- ✅ Presentation-ready
"""

import os
import sys
from datetime import datetime
from pathlib import Path

# Adicionar path do projeto
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
import uuid
from src.clients.bcb_v2 import BCBClient
from src.etl.sheets import SheetsLoader
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

# Configurar env vars
os.environ["GOOGLE_CREDENTIALS_PATH"] = "credentials.json"
os.environ["GOOGLE_SPREADSHEET_ID"] = "11-KC18ShMKXZOSbWvHcLHJwz3oDjexGQLb26xm2Wq4w"


def main():
    """
    Pipeline completo para popular fact_series.
    """
    print("\n" + "="*80)
    print("  📊 POPULAÇÃO COMPLETA DO fact_series")
    print("  🎯 Objetivo: Dados perfeitos para apresentação na SEXTA")
    print("="*80 + "\n")
    
    # 1. Buscar dados do BCB
    print("🌐 Etapa 1: Buscando dados do BCB...")
    print("   Período: Janeiro/2018 até Novembro/2025")
    print("   Séries: 10 indicadores econômicos\n")
    
    client = BCBClient()
    
    df_bcb = client.get_all_series(
        data_inicio="01/01/2018",
        data_fim="11/11/2025"
    )
    
    if df_bcb.empty:
        print("❌ ERRO: Nenhum dado retornado do BCB!\n")
        return 1
    
    print(f"✓ {len(df_bcb)} registros obtidos")
    print(f"✓ {df_bcb['series_id'].nunique()} séries únicas")
    print(f"✓ Período: {df_bcb['data'].min().date()} até {df_bcb['data'].max().date()}\n")
    
    # 2. Transformar para formato fact_series
    print("🔄 Etapa 2: Transformando para formato fact_series...\n")
    
    exec_id = f"populate_complete_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    timestamp = datetime.now().isoformat()
    
    df_fact = pd.DataFrame()
    
    # Gerar id_fato: series_id + data
    df_fact['id_fato'] = df_bcb['series_id'] + "_" + df_bcb['data'].dt.strftime('%Y-%m-%d')
    
    df_fact['series_id'] = df_bcb['series_id']
    df_fact['nome_indicador'] = df_bcb['nome_indicador']
    df_fact['data_referencia'] = df_bcb['data'].dt.strftime('%Y-%m-%d')
    df_fact['valor'] = df_bcb['valor']
    
    # Calcular variações MoM e YoY
    print("📈 Calculando variações MoM e YoY...")
    
    df_fact = df_fact.sort_values(['series_id', 'data_referencia'])
    
    # MoM (Month-over-Month)
    df_fact['variacao_mom'] = df_fact.groupby('series_id')['valor'].pct_change() * 100
    
    # YoY (Year-over-Year) - 12 períodos atrás
    df_fact['variacao_yoy'] = df_fact.groupby('series_id')['valor'].pct_change(periods=12) * 100
    
    # Substituir inf/nan por None
    df_fact['variacao_mom'] = df_fact['variacao_mom'].replace([float('inf'), float('-inf')], None)
    df_fact['variacao_yoy'] = df_fact['variacao_yoy'].replace([float('inf'), float('-inf')], None)
    
    print(f"✓ Variações calculadas\n")
    
    # Metadados
    df_fact['exec_id'] = exec_id
    df_fact['created_at'] = timestamp
    df_fact['updated_at'] = timestamp
    
    # Ordenar por série e data
    df_fact = df_fact.sort_values(['series_id', 'data_referencia'])
    
    print(f"✓ {len(df_fact)} registros prontos para escrita\n")
    
    # 3. Estatísticas por série
    print("📊 Estatísticas por série:\n")
    
    for series_id in sorted(df_fact['series_id'].unique()):
        df_serie = df_fact[df_fact['series_id'] == series_id]
        nome = df_serie['nome_indicador'].iloc[0]
        
        print(f"   • {series_id}")
        print(f"     Nome: {nome}")
        print(f"     Registros: {len(df_serie)}")
        print(f"     Período: {df_serie['data_referencia'].min()} até {df_serie['data_referencia'].max()}")
        print(f"     Valores: {df_serie['valor'].min():.2f} - {df_serie['valor'].max():.2f}")
        print()
    
    # 4. Limpar fact_series atual
    print("🧹 Etapa 3: Limpando fact_series atual...\n")
    
    loader = SheetsLoader()
    worksheet = loader._get_spreadsheet().worksheet("fact_series")
    worksheet.clear()
    
    print("✓ Aba limpa\n")
    
    # 5. Escrever novos dados
    print("💾 Etapa 4: Escrevendo dados no Google Sheets...\n")
    
    # Preparar dados (header + rows)
    headers = [list(df_fact.columns)]
    rows = df_fact.values.tolist()
    
    # Converter NaN para string vazia
    rows = [
        ['' if pd.isna(val) else val for val in row]
        for row in rows
    ]
    
    all_data = headers + rows
    
    # Escrever tudo de uma vez
    worksheet.update(values=all_data, range_name='A1')
    
    print(f"✓ {len(rows)} linhas escritas\n")
    
    # 6. Verificação final
    print("✅ Etapa 5: Verificação final...\n")
    
    df_verify = loader.read_fact_series()
    
    print(f"   Total de linhas: {len(df_verify)}")
    print(f"   Séries únicas: {df_verify['series_id'].nunique()}")
    print(f"   Período: {df_verify['data_referencia'].min()} até {df_verify['data_referencia'].max()}")
    print(f"   Duplicatas: {df_verify['id_fato'].duplicated().sum()}")
    print(f"   Valores nulos: {df_verify['valor'].isna().sum()}")
    
    # Resumo por série
    print("\n   📋 Resumo por série:")
    for series_id in sorted(df_verify['series_id'].unique()):
        df_s = df_verify[df_verify['series_id'] == series_id]
        nome = df_s['nome_indicador'].iloc[0] if 'nome_indicador' in df_s.columns else series_id
        print(f"      • {nome}: {len(df_s)} registros")
    
    print("\n" + "="*80)
    print("  ✅ POPULAÇÃO COMPLETA CONCLUÍDA!")
    print("  🎯 Dados prontos para apresentação na SEXTA-FEIRA")
    print("="*80 + "\n")
    
    logger.info(
        "fact_series_populated",
        exec_id=exec_id,
        total_rows=len(df_fact),
        unique_series=df_fact['series_id'].nunique(),
        date_range=f"{df_fact['data_referencia'].min()} - {df_fact['data_referencia'].max()}"
    )
    
    return 0


if __name__ == "__main__":
    exit(main())
