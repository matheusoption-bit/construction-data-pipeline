"""
Relatório de Qualidade do fact_series.

Verifica:
1. Cobertura temporal
2. Qualidade dos dados
3. Estatísticas por indicador
4. Status presentation-ready
"""

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from src.etl.sheets import SheetsLoader

os.environ["GOOGLE_CREDENTIALS_PATH"] = "credentials.json"
os.environ["GOOGLE_SPREADSHEET_ID"] = "11-KC18ShMKXZOSbWvHcLHJwz3oDjexGQLb26xm2Wq4w"


def main():
    print("\n" + "="*80)
    print("  📊 RELATÓRIO DE QUALIDADE - fact_series")
    print("  🎯 Para apresentação na SEXTA-FEIRA (2025-11-15)")
    print("="*80 + "\n")
    
    loader = SheetsLoader()
    df = loader.read_fact_series()
    
    print(f"📋 VISÃO GERAL\n")
    print(f"   Total de registros: {len(df):,}")
    print(f"   Séries únicas: {df['series_id'].nunique()}")
    print(f"   Período: {df['data_referencia'].min()} até {df['data_referencia'].max()}")
    print(f"   Anos de cobertura: {pd.to_datetime(df['data_referencia']).dt.year.nunique()}")
    
    print(f"\n📊 INDICADORES DISPONÍVEIS\n")
    
    for series_id in sorted(df['series_id'].unique()):
        df_s = df[df['series_id'] == series_id]
        
        nome = df_s['nome_indicador'].iloc[0] if 'nome_indicador' in df_s.columns else series_id
        
        # Calcular densidade temporal
        df_s_dates = pd.to_datetime(df_s['data_referencia'])
        dias_span = (df_s_dates.max() - df_s_dates.min()).days
        densidade = len(df_s) / (dias_span + 1) if dias_span > 0 else 0
        
        freq = "DIÁRIA" if densidade > 0.5 else "MENSAL"
        
        print(f"   ✓ {nome}")
        print(f"      • ID: {series_id}")
        print(f"      • Registros: {len(df_s):,}")
        print(f"      • Período: {df_s['data_referencia'].min()} até {df_s['data_referencia'].max()}")
        print(f"      • Frequência: {freq}")
        print(f"      • Valor min: {df_s['valor'].min():.2f}")
        print(f"      • Valor max: {df_s['valor'].max():.2f}")
        print(f"      • Valor médio: {df_s['valor'].mean():.2f}")
        print()
    
    print(f"📈 QUALIDADE DOS DADOS\n")
    
    # Verificar problemas
    problemas = []
    
    # 1. Duplicatas
    dup_count = df['id_fato'].duplicated().sum()
    if dup_count > 0:
        problemas.append(f"⚠️  {dup_count} duplicatas detectadas")
    else:
        print(f"   ✓ Sem duplicatas")
    
    # 2. Valores nulos
    null_valor = df['valor'].isna().sum()
    if null_valor > 0:
        problemas.append(f"⚠️  {null_valor} valores nulos")
    else:
        print(f"   ✓ Sem valores nulos")
    
    # 3. Datas futuras
    df['data_dt'] = pd.to_datetime(df['data_referencia'])
    futuras = (df['data_dt'] > pd.Timestamp.now()).sum()
    if futuras > 0:
        problemas.append(f"⚠️  {futuras} datas futuras")
    else:
        print(f"   ✓ Sem datas futuras")
    
    # 4. Coluna nome_indicador
    if 'nome_indicador' not in df.columns:
        problemas.append(f"❌ Coluna 'nome_indicador' ausente")
    else:
        null_nome = df['nome_indicador'].isna().sum()
        if null_nome > 0:
            problemas.append(f"⚠️  {null_nome} indicadores sem nome")
        else:
            print(f"   ✓ Todos os indicadores têm nome descritivo")
    
    # 5. Variações MoM/YoY
    if 'variacao_mom' in df.columns:
        print(f"   ✓ Variações MoM calculadas")
    
    if 'variacao_yoy' in df.columns:
        print(f"   ✓ Variações YoY calculadas")
    
    if problemas:
        print("\n   PROBLEMAS DETECTADOS:")
        for p in problemas:
            print(f"   {p}")
    
    print(f"\n🎯 STATUS PRESENTATION-READY\n")
    
    checklist = [
        ("✓" if len(df) > 5000 else "❌", f"Histórico robusto (>5000 registros): {len(df):,}"),
        ("✓" if df['series_id'].nunique() >= 9 else "❌", f"Múltiplos indicadores (>=9): {df['series_id'].nunique()}"),
        ("✓" if 'nome_indicador' in df.columns else "❌", "Nomes descritivos presentes"),
        ("✓" if dup_count == 0 else "❌", f"Sem duplicatas: {dup_count}"),
        ("✓" if null_valor == 0 else "❌", f"Sem valores nulos: {null_valor}"),
        ("✓" if futuras == 0 else "❌", f"Sem datas futuras: {futuras}"),
        ("✓" if 'variacao_mom' in df.columns else "❌", "Variações MoM calculadas"),
        ("✓" if 'variacao_yoy' in df.columns else "❌", "Variações YoY calculadas")
    ]
    
    for status, desc in checklist:
        print(f"   {status} {desc}")
    
    ready = all(status == "✓" for status, _ in checklist[:6])  # Primeiros 6 são críticos
    
    print("\n" + "="*80)
    if ready:
        print("  ✅ DADOS PRONTOS PARA APRESENTAÇÃO!")
        print("  🎯 Você pode apresentar na sexta com confiança")
    else:
        print("  ⚠️  DADOS PRECISAM DE AJUSTES")
        print("  Execute clean_fact_series_complete.py para corrigir problemas")
    print("="*80 + "\n")
    
    return 0 if ready else 1


if __name__ == "__main__":
    exit(main())
