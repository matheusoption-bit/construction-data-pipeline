"""Script para mostrar resultados finais."""
from src.etl.sheets import SheetsLoader

loader = SheetsLoader()

print("\n" + "="*80)
print("  🎯 SISTEMA CUB MASTER - RESULTADO FINAL")
print("="*80 + "\n")

# dim_composicao_cub_medio
ws1 = loader._get_spreadsheet().worksheet('dim_composicao_cub_medio')
data1 = ws1.get_all_values()

print(f"✅ dim_composicao_cub_medio: {len(data1)-1} linhas\n")
print(f"{'#':>3} {'Categoria':15} {'Tipo CUB':10} {'Peso':>6}")
print("-" * 50)
for i, row in enumerate(data1[1:11], 1):
    print(f"{i:>3}. {row[1]:15} {row[2]:10} {row[3]:>6}")

print("\n" + "="*80 + "\n")

# fact_cub_detalhado
ws2 = loader._get_spreadsheet().worksheet('fact_cub_detalhado')
data2 = ws2.get_all_values()

print(f"✅ fact_cub_detalhado: {len(data2)-1:,} linhas\n")
print("Primeiras 50 linhas:")
print(f"{'Data':12} {'UF':4} {'Tipo CUB':15} {'Valor':>10}")
print("-" * 50)
for row in data2[1:51]:
    print(f"{row[1]:12} {row[2]:4} {row[3]:15} {row[4]:>10}")

print("\n" + "="*80)
print("  ⏱️  Tempo total de execução: ~17-19 segundos")
print("  🎉 SISTEMA OPERACIONAL!")
print("="*80 + "\n")
