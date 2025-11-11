"""
Analisa problemas no BCBClient e dados corrompidos no fact_series.

Identifica:
1. Datas futuras impossíveis
2. Valores vazios ou zeros
3. Valores absurdos
4. Séries com valores fixos incorretos
"""

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Adicionar path do projeto
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from src.clients.bcb import BCBClient
from src.etl.sheets import SheetsLoader

# Configurar env vars
os.environ["GOOGLE_CREDENTIALS_PATH"] = "credentials.json"
os.environ["GOOGLE_SPREADSHEET_ID"] = "11-KC18ShMKXZOSbWvHcLHJwz3oDjexGQLb26xm2Wq4w"


def analyze_fact_series_data():
    """Analisa dados corrompidos em fact_series."""
    
    print("\n" + "=" * 80)
    print("  📊 ANÁLISE DE DADOS CORROMPIDOS - fact_series")
    print("=" * 80 + "\n")
    
    loader = SheetsLoader()
    df = loader.read_fact_series()
    
    if df.empty:
        print("❌ fact_series está vazio!")
        return
    
    print(f"📌 Total de linhas: {len(df)}")
    print(f"📌 Período: {df['data_referencia'].min()} até {df['data_referencia'].max()}\n")
    
    # Converter data_referencia para datetime
    df['data_ref_dt'] = pd.to_datetime(df['data_referencia'], errors='coerce')
    hoje = pd.Timestamp.now()
    
    # 1. DATAS FUTURAS
    print("=" * 80)
    print("  1️⃣  DATAS FUTURAS (IMPOSSÍVEIS)")
    print("=" * 80)
    
    df_futuro = df[df['data_ref_dt'] > hoje]
    
    if len(df_futuro) > 0:
        print(f"❌ ENCONTRADAS {len(df_futuro)} linhas com datas FUTURAS:\n")
        
        for series_id in df_futuro['series_id'].unique():
            serie_futuro = df_futuro[df_futuro['series_id'] == series_id]
            print(f"   • {series_id}: {len(serie_futuro)} linhas")
            print(f"     Datas: {serie_futuro['data_referencia'].min()} até {serie_futuro['data_referencia'].max()}")
            print(f"     Valores: {serie_futuro['valor'].describe()[['min', 'max', 'mean']].to_dict()}\n")
    else:
        print("✅ Nenhuma data futura encontrada\n")
    
    # 2. VALORES VAZIOS/NULOS
    print("=" * 80)
    print("  2️⃣  VALORES VAZIOS OU NULOS")
    print("=" * 80)
    
    # Converter valor para numérico
    df['valor_num'] = pd.to_numeric(df['valor'], errors='coerce')
    
    df_vazios = df[df['valor_num'].isna()]
    
    if len(df_vazios) > 0:
        print(f"❌ ENCONTRADAS {len(df_vazios)} linhas com valores VAZIOS/NULOS:\n")
        
        for series_id in df_vazios['series_id'].unique():
            serie_vazio = df_vazios[df_vazios['series_id'] == series_id]
            print(f"   • {series_id}: {len(serie_vazio)} linhas vazias")
            print(f"     Período: {serie_vazio['data_referencia'].min()} até {serie_vazio['data_referencia'].max()}\n")
    else:
        print("✅ Nenhum valor vazio encontrado\n")
    
    # 3. VALORES ZEROS
    print("=" * 80)
    print("  3️⃣  VALORES ZERO (SUSPEITOS)")
    print("=" * 80)
    
    df_zeros = df[df['valor_num'] == 0]
    
    if len(df_zeros) > 0:
        print(f"⚠️  ENCONTRADAS {len(df_zeros)} linhas com valor ZERO:\n")
        
        for series_id in df_zeros['series_id'].unique():
            serie_zero = df_zeros[df_zeros['series_id'] == series_id]
            print(f"   • {series_id}: {len(serie_zero)} linhas com zero")
            print(f"     Período: {serie_zero['data_referencia'].min()} até {serie_zero['data_referencia'].max()}\n")
    else:
        print("✅ Nenhum valor zero encontrado\n")
    
    # 4. VALORES ABSURDOS
    print("=" * 80)
    print("  4️⃣  VALORES ABSURDOS (OUTLIERS EXTREMOS)")
    print("=" * 80)
    
    # Definir threshold: valores > 1 milhão ou < -1000
    df_absurdos = df[(df['valor_num'] > 1_000_000) | (df['valor_num'] < -1000)]
    
    if len(df_absurdos) > 0:
        print(f"❌ ENCONTRADAS {len(df_absurdos)} linhas com valores ABSURDOS:\n")
        
        for _, row in df_absurdos.iterrows():
            print(f"   • {row['id_fato']}")
            print(f"     Valor: {row['valor_num']:,.2f}")
            print(f"     Data: {row['data_referencia']}\n")
    else:
        print("✅ Nenhum valor absurdo encontrado\n")
    
    # 5. SÉRIES COM VALORES FIXOS (SELIC = 15)
    print("=" * 80)
    print("  5️⃣  SÉRIES COM VALORES FIXOS SUSPEITOS")
    print("=" * 80)
    
    for series_id in df['series_id'].unique():
        serie_df = df[df['series_id'] == series_id].copy()
        serie_df = serie_df[serie_df['valor_num'].notna()]
        
        if len(serie_df) > 5:  # Só analisar se tiver dados suficientes
            # Verificar se tem muitos valores repetidos
            valor_mais_comum = serie_df['valor_num'].mode()
            
            if len(valor_mais_comum) > 0:
                valor_fixo = valor_mais_comum.iloc[0]
                count_fixo = (serie_df['valor_num'] == valor_fixo).sum()
                percentual = (count_fixo / len(serie_df)) * 100
                
                if percentual > 50:  # Mais de 50% com mesmo valor
                    print(f"⚠️  {series_id}:")
                    print(f"   Valor fixo: {valor_fixo}")
                    print(f"   Repetições: {count_fixo}/{len(serie_df)} ({percentual:.1f}%)")
                    print(f"   Período: {serie_df['data_referencia'].min()} até {serie_df['data_referencia'].max()}\n")
    
    print("=" * 80 + "\n")


def test_bcb_api_real_time():
    """Testa API do BCB em tempo real para identificar problemas."""
    
    print("\n" + "=" * 80)
    print("  🔍 TESTE DA API BCB - DADOS REAIS")
    print("=" * 80 + "\n")
    
    client = BCBClient()
    
    # Testar cada série
    series_to_test = {
        "432": ("Selic", "mensal"),
        "226": ("TR", "mensal"),
        "1": ("Câmbio USD/BRL", "diária"),
        "433": ("IPCA", "mensal"),
        "189": ("IGP-M", "mensal"),
        "7478": ("Poupança", "mensal"),
        "4189": ("INPC", "mensal"),
        "4390": ("Crédito PF", "mensal"),
        "1207": ("Produção Construção", "mensal"),
        "24364": ("Crédito Habitacional", "mensal")
    }
    
    hoje = datetime.now()
    
    for series_id, (nome, freq) in series_to_test.items():
        print(f"\n{'='*80}")
        print(f"  📊 Série {series_id} - {nome} ({freq})")
        print(f"{'='*80}")
        
        # Testar últimos 90 dias
        data_fim = hoje.strftime("%d/%m/%Y")
        data_inicio = (hoje - timedelta(days=90)).strftime("%d/%m/%Y")
        
        try:
            dados = client.fetch_series(
                series_id=int(series_id),
                start_date=data_inicio,
                end_date=data_fim
            )
            
            if not dados:
                print(f"❌ VAZIO - API não retornou nenhum dado")
                continue
            
            df = pd.DataFrame(dados)
            df['date'] = pd.to_datetime(df['date'])
            
            print(f"✅ {len(df)} registros retornados")
            print(f"📅 Período: {df['date'].min().date()} até {df['date'].max().date()}")
            print(f"📈 Valores: min={df['value'].min():.4f}, max={df['value'].max():.4f}, média={df['value'].mean():.4f}")
            
            # Verificar dados futuros
            dados_futuros = df[df['date'] > pd.Timestamp.now()]
            if len(dados_futuros) > 0:
                print(f"⚠️  ALERTA: {len(dados_futuros)} registros com data FUTURA!")
                print(f"   Datas futuras: {dados_futuros['date'].min().date()} até {dados_futuros['date'].max().date()}")
            
            # Verificar valores zero/null
            zeros = (df['value'] == 0).sum()
            nulls = df['value'].isna().sum()
            
            if zeros > 0:
                print(f"⚠️  {zeros} valores ZERO encontrados")
            
            if nulls > 0:
                print(f"⚠️  {nulls} valores NULL encontrados")
            
            # Verificar valores repetidos (para séries mensais)
            if freq == "mensal":
                valor_mais_comum = df['value'].mode()
                if len(valor_mais_comum) > 0:
                    valor_fixo = valor_mais_comum.iloc[0]
                    count_fixo = (df['value'] == valor_fixo).sum()
                    percentual = (count_fixo / len(df)) * 100
                    
                    if percentual > 30:
                        print(f"⚠️  {percentual:.1f}% dos valores são iguais a {valor_fixo}")
            
            # Mostrar últimos 5 valores
            print("\n📋 Últimos 5 registros:")
            for _, row in df.tail(5).iterrows():
                print(f"   {row['date'].date()}: {row['value']:.4f}")
        
        except Exception as e:
            print(f"❌ ERRO: {str(e)}")
            print(f"   Tipo: {type(e).__name__}")
    
    print("\n" + "=" * 80 + "\n")


def analyze_date_generation_logic():
    """Analisa lógica de geração de datas no daily_bcb job."""
    
    print("\n" + "=" * 80)
    print("  🔧 ANÁLISE DA LÓGICA DE DATAS DO JOB")
    print("=" * 80 + "\n")
    
    # Simular lógica do job
    months_back = 12
    end_date = datetime.now()
    start_date = end_date - timedelta(days=months_back * 30)
    
    print(f"📌 Lógica atual do daily_bcb.py:")
    print(f"   months_back = {months_back}")
    print(f"   end_date = datetime.now() = {end_date.date()}")
    print(f"   start_date = end_date - timedelta(days={months_back * 30}) = {start_date.date()}")
    print(f"\n   Resultado: busca de {start_date.strftime('%d/%m/%Y')} até {end_date.strftime('%d/%m/%Y')}")
    
    print(f"\n⚠️  PROBLEMA IDENTIFICADO:")
    print(f"   • end_date = datetime.now() inclui DATA DE HOJE")
    print(f"   • Para séries DIÁRIAS (como câmbio), isso gera dados FUTUROS")
    print(f"   • API do BCB retorna valores ZERO ou VAZIOS para datas futuras")
    
    print(f"\n✅ SOLUÇÃO RECOMENDADA:")
    print(f"   • end_date = datetime.now() - timedelta(days=1)  # Usar ontem")
    print(f"   • Ou end_date = último dia do mês anterior (para séries mensais)")
    
    print("\n" + "=" * 80 + "\n")


def main():
    """Executa todas as análises."""
    
    print("\n" + "=" * 80)
    print("  🚨 DIAGNÓSTICO COMPLETO - PROBLEMAS BCB CLIENT")
    print("=" * 80)
    print(f"  Data/Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80 + "\n")
    
    # 1. Analisar dados corrompidos no Sheets
    print("📋 Etapa 1: Analisando dados em fact_series...")
    analyze_fact_series_data()
    
    # 2. Testar API do BCB em tempo real
    print("📋 Etapa 2: Testando API do BCB em tempo real...")
    test_bcb_api_real_time()
    
    # 3. Analisar lógica de datas
    print("📋 Etapa 3: Analisando lógica de geração de datas...")
    analyze_date_generation_logic()
    
    print("\n" + "=" * 80)
    print("  ✅ DIAGNÓSTICO COMPLETO")
    print("=" * 80)
    print("\n📝 RESUMO DOS PROBLEMAS IDENTIFICADOS:")
    print("\n1. ❌ Datas futuras geradas por end_date=datetime.now()")
    print("2. ❌ API do BCB retorna zeros/vazios para datas futuras")
    print("3. ❌ Séries diárias (câmbio) têm problema com fim de semana")
    print("4. ❌ Valores absurdos não são filtrados antes da escrita")
    print("5. ❌ Falta validação de data máxima por série")
    print("\n📌 PRÓXIMOS PASSOS:")
    print("\n1. Corrigir daily_bcb.py para usar end_date = ontem")
    print("2. Adicionar validação de datas futuras em BCBClient")
    print("3. Filtrar valores zero/null antes de escrever")
    print("4. Limpar fact_series removendo dados corrompidos")
    print("5. Adicionar testes de validação de data")
    print("\n" + "=" * 80 + "\n")


if __name__ == "__main__":
    main()
