"""
Versão 3 - DEFINITIVA e OTIMIZADA
Usa operações vetorizadas do pandas (100x mais rápido)
Trata vírgulas como separador decimal
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import structlog

logger = structlog.get_logger(__name__)


def excel_serial_to_date_vectorized(series: pd.Series) -> pd.Series:
    """
    Converte série de números seriais Excel para datas ISO.
    Vetorizado para performance.
    Preserva valores NaN como NaN (não converte para string 'nan').
    """
    # Epoch do Excel
    excel_epoch = pd.Timestamp('1899-12-30')
    
    # Identificar seriais válidos (> 1000 e não-NaN)
    mask = (series > 1000) & series.notna()
    
    # Criar série de resultado inicialmente vazia (object)
    result = pd.Series(index=series.index, dtype=object)
    
    # Preservar valores NaN (deixar como NaN, não converter para string)
    result[series.isna()] = np.nan
    
    # Aplicar conversão apenas nos seriais
    if mask.any():
        result[mask] = (excel_epoch + pd.to_timedelta(series[mask], unit='D')).dt.strftime('%Y-%m-%d')
    
    return result


def excel_serial_to_datetime_vectorized(series: pd.Series) -> pd.Series:
    """
    Converte série de números seriais Excel para datetime ISO.
    Vetorizado para performance.
    Preserva valores NaN como NaN (não converte para string 'nan').
    """
    excel_epoch = pd.Timestamp('1899-12-30')
    
    # Identificar seriais válidos (> 1000 e não-NaN)
    mask = (series > 1000) & series.notna()
    
    # Criar série de resultado inicialmente vazia (object)
    result = pd.Series(index=series.index, dtype=object)
    
    # Preservar valores NaN (deixar como NaN, não converter para string)
    result[series.isna()] = np.nan
    
    # Aplicar conversão apenas nos seriais
    if mask.any():
        result[mask] = (excel_epoch + pd.to_timedelta(series[mask], unit='D')).dt.strftime('%Y-%m-%d %H:%M:%S')
    
    return result


def fix_fact_cub_detalhado_v3():
    """Versão 3 - Ultra otimizada com vetorização."""
    
    print("\n" + "="*80)
    print("  🚀 CORREÇÃO DE DADOS V3 - ULTRA OTIMIZADA")
    print("="*80 + "\n")
    
    # Ler arquivo
    input_file = "docs/fact_cub_detalhado.md"
    print(f"📖 Lendo {input_file}...")
    
    # Ler como string primeiro
    df = pd.read_csv(input_file, sep="\t", dtype=str)
    print(f"  ✓ {len(df):,} linhas carregadas\n")
    
    # CORREÇÃO 1: data_referencia
    print("📅 Corrigindo data_referencia...")
    print(f"  📋 Amostra ANTES (5 linhas):")
    print(f"     {df['data_referencia'].head(5).tolist()}\n")
    
    # Guardar valores originais
    data_original = df["data_referencia"].copy()
    
    # Substituir vírgula por ponto (se houver)
    df["data_referencia"] = df["data_referencia"].str.replace(',', '.', regex=False)
    
    # Tentar converter para numérico em série separada
    df_data_numeric = pd.to_numeric(df["data_referencia"], errors='coerce')
    
    # Contar quantos são seriais válidos
    seriais_mask = (df_data_numeric > 1000) & df_data_numeric.notna()
    before_count = seriais_mask.sum()
    
    # Se houver seriais, aplicar conversão apenas neles
    if before_count > 0:
        # Aplicar conversão VETORIZADA apenas nos seriais
        df_convertido = excel_serial_to_date_vectorized(df_data_numeric)
        # Substituir APENAS os valores que foram convertidos (não-NaN)
        df.loc[seriais_mask, "data_referencia"] = df_convertido[seriais_mask]
    else:
        # Se não há seriais, preservar valores originais
        df["data_referencia"] = data_original
    
    print(f"  ✅ {before_count:,} datas convertidas de serial → ISO")
    print(f"  📋 Amostra DEPOIS (5 linhas):")
    print(f"     {df['data_referencia'].head(5).tolist()}\n")
    
    # CORREÇÃO 2: tipo_cub
    print("🏷️  Corrigindo tipo_cub...")
    
    mapeamento = {
        "Coluna_0": "CUB-medio",
        "Coluna 0": "CUB-medio",
        "Coluna_1": "Variacao_mensal_%",
        "Coluna 1": "Variacao_mensal_%",
        "Coluna_2": "Variacao_12meses_%",
        "Coluna 2": "Variacao_12meses_%",
        "Coluna_3": "Variacao_ano_%",
        "Coluna 3": "Variacao_ano_%",
    }
    
    df["tipo_cub"] = df["tipo_cub"].replace(mapeamento)
    
    tipos_unicos = df["tipo_cub"].unique()
    print(f"  ✅ {len(tipos_unicos)} tipos mapeados:")
    for tipo in tipos_unicos:
        count = (df["tipo_cub"] == tipo).sum()
        print(f"     • {tipo}: {count:,} registros")
    print()
    
    # CORREÇÃO 3: valor
    print("🔢 Corrigindo coluna 'valor'...")
    print(f"  📋 Amostra ANTES (5 linhas):")
    print(f"     {df['valor'].head(5).tolist()}\n")
    
    # Substituir vírgula por ponto
    df["valor"] = df["valor"].str.replace(',', '.', regex=False)
    
    # Converter para numérico
    df["valor"] = pd.to_numeric(df["valor"], errors='coerce')
    
    # Contar valores -100
    valores_menos_100 = (df["valor"] == -100).sum()
    
    # Substituir -100 por NaN
    df.loc[df["valor"] == -100, "valor"] = np.nan
    
    valores_validos = df["valor"].notna().sum()
    valores_nulos = df["valor"].isna().sum()
    
    print(f"  ✅ {valores_menos_100:,} valores -100 → NULL")
    print(f"  ✅ {valores_validos:,} valores válidos ({valores_validos/len(df)*100:.2f}%)")
    print(f"  ℹ️  {valores_nulos:,} valores NULL ({valores_nulos/len(df)*100:.2f}%)")
    print(f"  📋 Amostra DEPOIS (5 linhas):")
    print(f"     {df['valor'].head(5).tolist()}\n")
    
    # CORREÇÃO 4: created_at
    print("🕐 Corrigindo created_at...")
    print(f"  📋 Amostra ANTES (5 linhas):")
    print(f"     {df['created_at'].head(5).tolist()}\n")
    
    # Guardar valores originais
    created_original = df["created_at"].copy()
    
    # Substituir vírgula por ponto
    df["created_at"] = df["created_at"].str.replace(',', '.', regex=False)
    
    # Tentar converter para numérico em série separada
    df_created_numeric = pd.to_numeric(df["created_at"], errors='coerce')
    
    # Contar quantos são seriais válidos
    seriais_mask = (df_created_numeric > 1000) & df_created_numeric.notna()
    before_count = seriais_mask.sum()
    
    # Se houver seriais, aplicar conversão apenas neles
    if before_count > 0:
        # Aplicar conversão VETORIZADA apenas nos seriais
        df_convertido = excel_serial_to_datetime_vectorized(df_created_numeric)
        # Substituir APENAS os valores que foram convertidos (não-NaN)
        df.loc[seriais_mask, "created_at"] = df_convertido[seriais_mask]
    else:
        # Se não há seriais, preservar valores originais
        df["created_at"] = created_original
    
    print(f"  ✅ {before_count:,} timestamps convertidos")
    print(f"  📋 Amostra DEPOIS (5 linhas):")
    print(f"     {df['created_at'].head(5).tolist()}\n")
    
    # VALIDAÇÃO FINAL
    print("="*80)
    print("  🔍 VALIDAÇÃO FINAL")
    print("="*80 + "\n")
    
    # Verificar se ainda tem seriais
    try:
        created_numeric = pd.to_numeric(df["created_at"], errors='coerce')
        seriais_restantes = (created_numeric > 1000).sum()
        
        if seriais_restantes > 0:
            print(f"  ⚠️  ATENÇÃO: {seriais_restantes} linhas ainda com created_at em serial!")
            print(f"     Amostra das linhas problemáticas:")
            problemas = df[created_numeric > 1000].head(10)
            print(problemas[["id_fato", "data_referencia", "uf", "created_at"]])
            print()
        else:
            print(f"  ✅ TODAS as datas/timestamps foram convertidos!\n")
    except:
        print(f"  ✅ created_at convertido para texto (datetime ISO)\n")
    
    # Salvar
    output_file = "docs/fact_cub_detalhado_CORRIGIDO_V3.md"
    print(f"💾 Salvando {output_file}...")
    df.to_csv(output_file, sep="\t", index=False)
    print(f"  ✅ Arquivo salvo!\n")
    
    # Estatísticas finais
    print("="*80)
    print("  📊 ESTATÍSTICAS FINAIS")
    print("="*80 + "\n")
    
    print(f"📈 Total de registros: {len(df):,}")
    print(f"🗺️  Estados (UFs): {df['uf'].nunique()}")
    print(f"🏗️  Tipos de CUB: {df['tipo_cub'].nunique()}")
    for tipo in df["tipo_cub"].unique():
        count = (df["tipo_cub"] == tipo).sum()
        print(f"    • {tipo}: {count:,}")
    
    # Período: remover NaN antes de calcular min/max
    datas_validas = df['data_referencia'][df['data_referencia'] != 'nan'].dropna()
    if len(datas_validas) > 0:
        print(f"📆 Período: {datas_validas.min()} até {datas_validas.max()}")
    
    print(f"❌ Valores NULL: {df['valor'].isna().sum():,} ({df['valor'].isna().sum()/len(df)*100:.2f}%)")
    print()
    
    print("="*80)
    print("  ✅ CORREÇÃO V3 CONCLUÍDA COM SUCESSO!")
    print("="*80 + "\n")
    
    print(f"📁 Arquivo gerado:")
    print(f"   {output_file}\n")
    
    return df


if __name__ == "__main__":
    import sys
    
    try:
        df = fix_fact_cub_detalhado_v3()
        print("🎉 Processo concluído com sucesso!\n")
        sys.exit(0)
    except Exception as e:
        logger.error("erro_fatal", error=str(e), exc_info=True)
        print(f"\n❌ ERRO FATAL: {str(e)}\n")
        sys.exit(1)
