import requests
import json

print('=== Teste: API SIDRA - Tabela de Área Territorial ===\n')

# Tabela 1301: Área territorial oficial (km²)
# Testando com poucos municípios SC
url = 'https://apisidra.ibge.gov.br/values/t/1301/n6/4200051,4205407/v/all/p/last%201'

print(f'URL: {url}\n')

r = requests.get(url, timeout=30)
data = r.json()

print(f'Total de registros: {len(data)}\n')

if data:
    print('Estrutura (primeiras 3 linhas):')
    for i, row in enumerate(data[:3]):
        print(f'{i}: {row}')
    
    # Parsear dados
    print('\n\nDados parseados:')
    for i, row in enumerate(data):
        if i == 0:  # skip header
            continue
        
        cod = row.get('D1C', '')
        nome = row.get('D1N', '')
        valor = row.get('V', '')
        
        if cod and cod.startswith('42'):
            print(f'{nome}: {valor} km²')
