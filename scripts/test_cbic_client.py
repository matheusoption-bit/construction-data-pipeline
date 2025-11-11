"""
Script de teste para validar o CBICClient.

Testa download, cache e parsing de dados CUB.

Uso:
    python scripts/test_cbic_client.py
"""

from pathlib import Path
import sys

# Adicionar src ao path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.clients.cbic import CBICClient


def main():
    """Testa funcionalidades do CBICClient."""
    
    print("\n" + "="*70)
    print("  🧪 Testando CBICClient")
    print("="*70 + "\n")
    
    # Criar cliente
    print("[1/4] Criando cliente CBIC...")
    client = CBICClient()
    print(f"  ✓ Cache dir: {client.cache_dir}\n")
    
    # Testar download
    print("[2/4] Baixando tabela CUB/SC...")
    try:
        filepath = client.download_table("06.A.06", "BI", 53)
        print(f"  ✓ Arquivo baixado: {filepath}")
        print(f"  ✓ Tamanho: {filepath.stat().st_size / 1024:.1f} KB\n")
    except Exception as e:
        print(f"  ❌ Erro: {e}\n")
        return
    
    # Testar parsing
    print("[3/4] Fazendo parsing dos dados...")
    try:
        df = client.parse_cub_by_state(filepath, uf="SC", tipo_cub="R1-N")
        print(f"  ✓ Linhas parseadas: {len(df)}")
        print(f"  ✓ Período: {df['data_referencia'].min()} a {df['data_referencia'].max()}")
        print(f"  ✓ Último valor: R$ {df.iloc[-1]['custo_m2']:.2f}/m²\n")
    except Exception as e:
        print(f"  ❌ Erro: {e}\n")
        return
    
    # Testar método de conveniência
    print("[4/4] Testando fetch_cub_historical()...")
    try:
        df_full = client.fetch_cub_historical("SC")
        print(f"  ✓ Série histórica completa: {len(df_full)} meses")
        print(f"\n  📊 Primeiras 5 linhas:")
        print(df_full.head().to_string(index=False))
        print(f"\n  📊 Últimas 5 linhas:")
        print(df_full.tail().to_string(index=False))
    except Exception as e:
        print(f"  ❌ Erro: {e}\n")
        return
    
    print("\n" + "="*70)
    print("  ✅ Todos os testes passaram!")
    print("="*70 + "\n")


if __name__ == "__main__":
    main()
