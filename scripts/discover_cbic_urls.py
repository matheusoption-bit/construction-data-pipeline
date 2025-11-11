"""
Script para descobrir o padrão exato de URLs do CBICdados.

Testa múltiplas combinações de padrões para encontrar URLs válidas.

Uso:
    python scripts/discover_cbic_urls.py --year 2024 --month 10
"""

import argparse
import requests
from typing import List, Tuple

BASE_URL = "http://www.cbicdados.com.br/media/anexos/"

def generate_test_urls(year: int, month: int) -> List[Tuple[str, str]]:
    """Gera lista de URLs para testar."""
    
    month_names = {
        1: "janeiro", 2: "fevereiro", 3: "marco", 4: "abril",
        5: "maio", 6: "junho", 7: "julho", 8: "agosto",
        9: "setembro", 10: "outubro", 11: "novembro", 12: "dezembro"
    }
    
    month_short = {
        1: "jan", 2: "fev", 3: "mar", 4: "abr",
        5: "mai", 6: "jun", 7: "jul", 8: "ago",
        9: "set", 10: "out", 11: "nov", 12: "dez"
    }
    
    patterns = []
    
    # XLSX patterns
    patterns.extend([
        (f"cub_sc_{year}_{month:02d}.xlsx", "xlsx"),
        (f"cub_{year}{month:02d}_sc.xlsx", "xlsx"),
        (f"cub_sc_{month:02d}_{year}.xlsx", "xlsx"),
        (f"CUB_SC_{year}_{month:02d}.xlsx", "xlsx"),
        (f"boletim_cub_sc_{year}_{month:02d}.xlsx", "xlsx"),
        (f"cub_santa_catarina_{year}_{month:02d}.xlsx", "xlsx"),
        (f"cub_sc_{month_names[month]}_{year}.xlsx", "xlsx"),
        (f"cub_sc_{month_short[month]}_{year}.xlsx", "xlsx"),
        (f"cub_sc_{month_short[month]}{year}.xlsx", "xlsx"),
        (f"cub_{year}_{month:02d}.xlsx", "xlsx"),
    ])
    
    # PDF patterns
    patterns.extend([
        (f"boletim_cub_sc_{month_names[month]}_{year}.pdf", "pdf"),
        (f"cub_sc_{month_names[month]}_{year}.pdf", "pdf"),
        (f"cub_sc_{month_short[month]}{year}.pdf", "pdf"),
        (f"CUB_SC_{month_names[month]}_{year}.pdf", "pdf"),
        (f"boletim_{month_names[month]}_{year}.pdf", "pdf"),
        (f"cub_{year}_{month:02d}.pdf", "pdf"),
    ])
    
    return patterns

def test_url(url: str) -> bool:
    """Testa se URL existe (HEAD request)."""
    try:
        response = requests.head(url, timeout=10, allow_redirects=True)
        return response.status_code == 200
    except:
        return False

def main(year: int, month: int):
    """Descobre padrão de URL para mês específico."""
    
    print(f"\n{'='*70}")
    print(f"  🔍 Descobrindo padrão de URL para {month:02d}/{year}")
    print(f"{'='*70}\n")
    
    patterns = generate_test_urls(year, month)
    
    print(f"Testando {len(patterns)} padrões possíveis...\n")
    
    found_urls = []
    
    for filename, file_type in patterns:
        url = BASE_URL + filename
        print(f"  Testando: {filename}...", end=" ", flush=True)
        
        if test_url(url):
            print("✅ ENCONTRADO!")
            found_urls.append((url, file_type))
        else:
            print("❌")
    
    print(f"\n{'='*70}")
    
    if found_urls:
        print(f"  ✅ {len(found_urls)} URL(s) encontrada(s):")
        for url, file_type in found_urls:
            print(f"     [{file_type.upper()}] {url}")
    else:
        print(f"  ❌ Nenhuma URL encontrada para {month:02d}/{year}")
        print(f"     Tente acessar manualmente:")
        print(f"     http://www.cbicdados.com.br/home/")
    
    print(f"{'='*70}\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Descobre padrão de URLs do CBICdados")
    parser.add_argument("--year", type=int, required=True, help="Ano (ex: 2024)")
    parser.add_argument("--month", type=int, required=True, help="Mês (1-12)")
    
    args = parser.parse_args()
    main(args.year, args.month)
