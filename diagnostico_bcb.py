#!/usr/bin/env python3
"""
Script para diagnosticar problemas com a API do BCB - Série 1207
"""
import requests
from datetime import datetime, timedelta
import json

def test_bcb_api():
    """Testa a API do BCB para identificar problemas"""
    
    print("=" * 70)
    print("🔍 DIAGNÓSTICO: API BCB - Série 1207 (Produção Construção Civil)")
    print("=" * 70)
    print()
    
    series_id = 1207
    base_url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs"
    
    # Calcular datas
    hoje = datetime.now()
    primeiro_dia_mes_atual = hoje.replace(day=1)
    end_date = primeiro_dia_mes_atual - timedelta(days=1)
    start_date = end_date - timedelta(days=365)  # 12 meses
    
    start_date_str = start_date.strftime("%d/%m/%Y")
    end_date_str = end_date.strftime("%d/%m/%Y")
    
    print(f"📅 Período solicitado:")
    print(f"   Início: {start_date_str}")
    print(f"   Fim: {end_date_str}")
    print()
    
    # Teste 1: Buscar últimos 12 meses
    print("🧪 TESTE 1: Buscar últimos 12 meses")
    print("-" * 70)
    url = f"{base_url}.{series_id}/dados"
    params = {
        "dataInicial": start_date_str,
        "dataFinal": end_date_str
    }
    
    print(f"URL: {url}")
    print(f"Params: {params}")
    
    try:
        response = requests.get(url, params=params, timeout=30)
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Resposta recebida: {len(data)} registros")
            
            if data:
                print(f"\n📊 Primeiros 3 registros:")
                for i, item in enumerate(data[:3], 1):
                    print(f"   {i}. {item}")
                
                if len(data) > 3:
                    print(f"\n📊 Últimos 3 registros:")
                    for i, item in enumerate(data[-3:], 1):
                        print(f"   {i}. {item}")
            else:
                print("⚠️  API retornou lista vazia!")
        else:
            print(f"❌ Erro HTTP: {response.status_code}")
            print(f"Response: {response.text[:500]}")
    except Exception as e:
        print(f"❌ Erro na requisição: {e}")
    
    print()
    
    # Teste 2: Buscar últimos 3 meses
    print("🧪 TESTE 2: Buscar últimos 3 meses")
    print("-" * 70)
    start_date_3m = end_date - timedelta(days=90)
    start_date_3m_str = start_date_3m.strftime("%d/%m/%Y")
    
    params2 = {
        "dataInicial": start_date_3m_str,
        "dataFinal": end_date_str
    }
    
    print(f"Params: {params2}")
    
    try:
        response = requests.get(url, params=params2, timeout=30)
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Resposta recebida: {len(data)} registros")
            
            if data:
                print(f"\n📊 Dados completos:")
                for item in data:
                    print(f"   {item}")
            else:
                print("⚠️  API retornou lista vazia!")
        else:
            print(f"❌ Erro HTTP: {response.status_code}")
    except Exception as e:
        print(f"❌ Erro na requisição: {e}")
    
    print()
    
    # Teste 3: Buscar sem datas (últimos dados disponíveis)
    print("🧪 TESTE 3: Buscar sem especificar datas")
    print("-" * 70)
    
    try:
        response = requests.get(url, timeout=30)
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Resposta recebida: {len(data)} registros")
            
            if data:
                print(f"\n📊 Últimos 5 registros disponíveis:")
                for item in data[-5:]:
                    print(f"   {item}")
            else:
                print("⚠️  API retornou lista vazia!")
        else:
            print(f"❌ Erro HTTP: {response.status_code}")
    except Exception as e:
        print(f"❌ Erro na requisição: {e}")
    
    print()
    
    # Teste 4: Verificar séries similares que funcionam
    print("🧪 TESTE 4: Testar série similar (432 - SELIC)")
    print("-" * 70)
    
    url_selic = f"{base_url}.432/dados"
    params_selic = {
        "dataInicial": start_date_str,
        "dataFinal": end_date_str
    }
    
    try:
        response = requests.get(url_selic, params=params_selic, timeout=30)
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Resposta recebida: {len(data)} registros")
            
            if data:
                print(f"\n📊 Últimos 3 registros:")
                for item in data[-3:]:
                    print(f"   {item}")
            else:
                print("⚠️  API retornou lista vazia!")
        else:
            print(f"❌ Erro HTTP: {response.status_code}")
    except Exception as e:
        print(f"❌ Erro na requisição: {e}")
    
    print()
    
    # Teste 5: Verificar documentação/existência da série
    print("🧪 TESTE 5: Verificar informações da série 1207")
    print("-" * 70)
    print("ℹ️  Série 1207: Produção da indústria da construção civil")
    print("    Fonte: IBGE - Pesquisa Industrial Mensal - Produção Física")
    print("    Periodicidade: Mensal")
    print("    Unidade: Número-índice (base: média de 2012 = 100)")
    print()
    print("🔗 Documentação:")
    print("    https://www3.bcb.gov.br/sgspub/consultarvalores/consultarValoresSeries.do?method=consultarValores")
    
    print()
    print("=" * 70)
    print("📊 RESUMO DO DIAGNÓSTICO")
    print("=" * 70)
    print()
    print("Possíveis causas do problema:")
    print("1. 🔴 Série descontinuada ou sem atualizações recentes")
    print("2. 🟡 Período solicitado não possui dados disponíveis")
    print("3. 🟡 Defasagem na divulgação (dados mensais demoram +30 dias)")
    print("4. 🟢 API funcionando, mas dados não disponíveis no período")
    print()
    print("Recomendações:")
    print("✅ Verificar última data disponível da série")
    print("✅ Aumentar período de busca (ex: 24 meses)")
    print("✅ Usar série alternativa se 1207 estiver descontinuada")
    print("✅ Implementar fallback para quando não há dados")
    print()

if __name__ == "__main__":
    test_bcb_api()
