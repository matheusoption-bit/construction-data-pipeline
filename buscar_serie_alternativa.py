#!/usr/bin/env python3
"""
Script para buscar série alternativa para Construção Civil
"""
import requests
from datetime import datetime, timedelta

def test_construction_series():
    """Testa séries alternativas de Construção Civil"""
    
    print("=" * 70)
    print("🔍 BUSCANDO SÉRIES ALTERNATIVAS PARA CONSTRUÇÃO CIVIL")
    print("=" * 70)
    print()
    
    # Séries alternativas para construção civil
    series_alternatives = {
        1171: "PIM - Construção (Número-índice)",
        1172: "PIM - Construção com ajuste sazonal",
        21863: "PAIC - Produção Construção Civil (Receita nominal)",
        21864: "PAIC - Produção Construção Civil (Receita real)",
        21865: "PAIC - Produção Construção Civil (Pessoal ocupado)",
        28561: "Crédito - Construção Civil (Saldo)",
    }
    
    base_url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs"
    
    hoje = datetime.now()
    primeiro_dia_mes_atual = hoje.replace(day=1)
    end_date = primeiro_dia_mes_atual - timedelta(days=1)
    start_date = end_date - timedelta(days=180)  # 6 meses
    
    start_date_str = start_date.strftime("%d/%m/%Y")
    end_date_str = end_date.strftime("%d/%m/%Y")
    
    print(f"📅 Período de teste: {start_date_str} a {end_date_str}")
    print()
    
    for series_id, description in series_alternatives.items():
        print(f"🧪 Série {series_id}: {description}")
        print("-" * 70)
        
        url = f"{base_url}.{series_id}/dados"
        params = {
            "dataInicial": start_date_str,
            "dataFinal": end_date_str
        }
        
        try:
            response = requests.get(url, params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                
                if data:
                    print(f"✅ {len(data)} registros encontrados")
                    print(f"   Últimos 3 registros:")
                    for item in data[-3:]:
                        print(f"      {item}")
                else:
                    # Testar sem datas
                    response2 = requests.get(url, timeout=30)
                    if response2.status_code == 200:
                        data2 = response2.json()
                        if data2:
                            print(f"⚠️  Sem dados no período, mas série ativa")
                            print(f"   Últimos dados disponíveis:")
                            for item in data2[-3:]:
                                print(f"      {item}")
                        else:
                            print("❌ Série vazia")
                    else:
                        print(f"❌ Erro {response2.status_code}")
            else:
                print(f"❌ Erro HTTP: {response.status_code}")
        except Exception as e:
            print(f"❌ Erro: {e}")
        
        print()
    
    print("=" * 70)
    print("📊 RECOMENDAÇÃO")
    print("=" * 70)
    print()
    print("✅ Melhor alternativa: Série 21863 ou 21864 (PAIC)")
    print("   - Fonte: IBGE - Pesquisa Anual da Indústria da Construção")
    print("   - Periodicidade: Anual")
    print("   - Mais atualizada e confiável")
    print()
    print("🔄 Ação necessária:")
    print("   1. Substituir série 1207 por 21863 ou 21864")
    print("   2. Ou remover série 1207 do mapeamento")
    print("   3. Adicionar tratamento para séries sem dados")
    print()

if __name__ == "__main__":
    test_construction_series()
