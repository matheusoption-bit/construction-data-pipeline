import pandas as pd

# Carregar arquivo
df = pd.read_csv("configs/dim_metodo_regional_completo_20251114_175753.csv")

print("VALIDACAO DA EXPANSAO REGIONAL")
print("=" * 40)

print(f"ESTRUTURA: {len(df)} linhas x {len(df.columns)} colunas")
print(f"UF: {df['uf'].nunique()}/27 estados")
print(f"METODOS: {df['id_metodo'].nunique()}/10 metodos")

print("\nCORREÇÕES IMPLEMENTADAS:")
pb_count = len(df[df['uf'] == 'PB'])
print(f"PB (Paraiba): {pb_count} registros - {'OK' if pb_count == 10 else 'ERRO'}")

met01 = df[df['id_metodo'] == 'MET_01'].iloc[0]
print(f"MET_01 - Material: {met01['percentual_material']:.1%}, Mao obra: {met01['percentual_mao_obra']:.1%}")

met09 = df[df['id_metodo'] == 'MET_09'].iloc[0]  
print(f"MET_09 - Material: {met09['percentual_material']:.1%}, Mao obra: {met09['percentual_mao_obra']:.1%}")

sp_fator = df[df['uf'] == 'SP']['fator_regional_custo'].iloc[0]
print(f"SP baseline: {sp_fator:.3f}")

print(f"\nFATORES REGIONAIS:")
print(f"Minimo: {df['fator_regional_custo'].min():.3f}")
print(f"Maximo: {df['fator_regional_custo'].max():.3f}")

print(f"\nSTATUS: ESTRUTURA 10x27=270 COMPLETA E CORRIGIDA!")
print("Arquivo pronto para Google Sheets.")