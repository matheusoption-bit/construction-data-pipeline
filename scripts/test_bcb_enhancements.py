"""
Script de teste para validar as melhorias no BCBClient.

Testa:
1. Validação de datas para séries diárias (D+1)
2. Validação de datas para séries mensais (fim do mês anterior)
3. Ajuste automático de datas futuras
4. Detecção de respostas vazias
5. Detecção de valores constantes suspeitos
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta

# Adicionar o diretório src ao path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

from clients.bcb import BCBClient
import structlog

# Configurar logging básico
logger = structlog.get_logger()


def test_daily_series_date_validation():
    """Testa se séries diárias usam ontem como data máxima."""
    print("\n" + "="*80)
    print("TESTE 1: Validação de data para série DIÁRIA (Câmbio USD - série 1)")
    print("="*80)
    
    client = BCBClient()
    hoje = datetime.now()
    ontem = hoje - timedelta(days=1)
    
    # Tentar buscar com data futura (hoje) - deve ajustar para ontem
    data_hoje = hoje.strftime("%d/%m/%Y")
    print(f"\n📅 Tentando buscar série 1 (Câmbio) até HOJE: {data_hoje}")
    print(f"   Esperado: Ajuste automático para ONTEM: {ontem.strftime('%d/%m/%Y')}")
    
    data = client.fetch_series(
        series_id=1,  # Câmbio USD (diária)
        start_date=(hoje - timedelta(days=7)).strftime("%d/%m/%Y"),
        end_date=data_hoje  # Data futura para série diária
    )
    
    if data:
        print(f"✅ Série retornou {len(data)} registros")
        print(f"   Última data: {data[-1]['date']}")
        print(f"   Último valor: {data[-1]['value']}")
    else:
        print("⚠️  Série retornou vazia")


def test_monthly_series_date_validation():
    """Testa se séries mensais usam fim do mês anterior como data máxima."""
    print("\n" + "="*80)
    print("TESTE 2: Validação de data para série MENSAL (Selic - série 432)")
    print("="*80)
    
    client = BCBClient()
    hoje = datetime.now()
    primeiro_dia_mes_atual = hoje.replace(day=1)
    ultimo_dia_mes_anterior = primeiro_dia_mes_atual - timedelta(days=1)
    
    # Tentar buscar com data futura (hoje) - deve ajustar para fim do mês anterior
    data_hoje = hoje.strftime("%d/%m/%Y")
    print(f"\n📅 Tentando buscar série 432 (Selic) até HOJE: {data_hoje}")
    print(f"   Esperado: Ajuste automático para FIM DO MÊS ANTERIOR: {ultimo_dia_mes_anterior.strftime('%d/%m/%Y')}")
    
    data = client.fetch_series(
        series_id=432,  # Selic (mensal)
        start_date=(hoje - timedelta(days=90)).strftime("%d/%m/%Y"),
        end_date=data_hoje  # Data futura para série mensal
    )
    
    if data:
        print(f"✅ Série retornou {len(data)} registros")
        print(f"   Última data: {data[-1]['date']}")
        print(f"   Último valor Selic: {data[-1]['value']}%")
    else:
        print("⚠️  Série retornou vazia")


def test_default_dates():
    """Testa se datas padrão são calculadas corretamente quando não fornecidas."""
    print("\n" + "="*80)
    print("TESTE 3: Datas padrão (sem start_date e end_date)")
    print("="*80)
    
    client = BCBClient()
    
    print(f"\n📅 Buscando série 433 (IPCA) SEM especificar datas")
    print(f"   Esperado: start_date = 12 meses atrás, end_date = fim do mês anterior")
    
    data = client.fetch_series(series_id=433)  # IPCA (mensal)
    
    if data:
        print(f"✅ Série retornou {len(data)} registros")
        print(f"   Primeira data: {data[0]['date']}")
        print(f"   Última data: {data[-1]['date']}")
        print(f"   Valores de exemplo:")
        for item in data[-3:]:
            print(f"      {item['date']}: {item['value']}%")
    else:
        print("⚠️  Série retornou vazia")


def test_empty_response_handling():
    """Testa detecção de resposta vazia."""
    print("\n" + "="*80)
    print("TESTE 4: Detecção de resposta vazia")
    print("="*80)
    
    client = BCBClient()
    
    # Série 1207 (Produção CC) tem períodos sem dados - buscar período antigo
    print(f"\n📅 Buscando série 1207 (Produção CC) em período sem dados")
    
    data = client.fetch_series(
        series_id=1207,
        start_date="01/01/2020",  # Período antigo sem dados
        end_date="31/01/2020"
    )
    
    if not data:
        print("✅ Resposta vazia detectada corretamente")
    else:
        print(f"⚠️  Retornou {len(data)} registros")


def test_constant_value_detection():
    """Testa detecção de valores constantes suspeitos."""
    print("\n" + "="*80)
    print("TESTE 5: Detecção de valores constantes (pattern Selic=15.0)")
    print("="*80)
    
    client = BCBClient()
    
    # Buscar período pequeno da Selic para verificar se há valores suspeitos
    print(f"\n📊 Buscando série 432 (Selic) dos últimos 30 dias")
    print(f"   Se houver >10 registros idênticos, deve emitir warning")
    
    hoje = datetime.now()
    data_inicio = (hoje - timedelta(days=30)).strftime("%d/%m/%Y")
    
    data = client.fetch_series(
        series_id=432,
        start_date=data_inicio
    )
    
    if data:
        unique_values = set(item['value'] for item in data)
        print(f"✅ Série retornou {len(data)} registros")
        print(f"   Valores únicos: {len(unique_values)}")
        
        if len(unique_values) == 1:
            print(f"⚠️  DETECTADO: Todos os {len(data)} registros têm valor = {data[0]['value']}")
            print(f"   (Warning deve aparecer nos logs acima)")
        else:
            print(f"✅ Valores variados detectados: {sorted(unique_values)}")
    else:
        print("⚠️  Série retornou vazia")


def test_all_series_types():
    """Testa todas as séries para verificar classificação correta."""
    print("\n" + "="*80)
    print("TESTE 6: Classificação de todas as séries (diárias vs mensais)")
    print("="*80)
    
    client = BCBClient()
    
    series = {
        "DIÁRIAS": [
            (1, "Câmbio USD"),
            (11, "Câmbio USD Compra"),
            (10813, "EUR/BRL"),
            (10814, "GBP/BRL")
        ],
        "MENSAIS": [
            (432, "Selic"),
            (226, "TR"),
            (433, "IPCA"),
            (189, "IGP-M"),
            (7478, "Poupança"),
            (4189, "INPC"),
            (4390, "Crédito PF"),
            (1207, "Produção CC"),
            (24364, "Crédito Habitacional")
        ]
    }
    
    for tipo, lista_series in series.items():
        print(f"\n📊 Testando séries {tipo}:")
        for series_id, nome in lista_series:
            is_daily = client._is_daily_series(series_id)
            resultado = "✅ DIÁRIA" if is_daily else "📅 MENSAL"
            esperado_ok = (tipo == "DIÁRIAS" and is_daily) or (tipo == "MENSAIS" and not is_daily)
            
            status = "✅" if esperado_ok else "❌ ERRO"
            print(f"   {status} Série {series_id:5d} ({nome:25s}): {resultado}")


def main():
    """Executa todos os testes."""
    print("="*80)
    print("TESTES DE VALIDAÇÃO DO BCBClient - Melhorias Anti-Corrupção")
    print("="*80)
    
    try:
        test_all_series_types()
        test_daily_series_date_validation()
        test_monthly_series_date_validation()
        test_default_dates()
        test_empty_response_handling()
        test_constant_value_detection()
        
        print("\n" + "="*80)
        print("✅ TODOS OS TESTES CONCLUÍDOS")
        print("="*80)
        print("\nVerifique os logs acima para confirmar:")
        print("  • Ajustes automáticos de datas futuras")
        print("  • Warnings de respostas vazias")
        print("  • Warnings de valores constantes suspeitos")
        print("  • Classificação correta das séries")
        
    except Exception as e:
        print(f"\n❌ ERRO durante testes: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
