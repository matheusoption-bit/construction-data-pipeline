"""
Versão CORRIGIDA do script de correção.
Força conversão numérica antes de processar.
"""

import pandas as pd
from datetime import datetime, timedelta
import structlog

logger = structlog.get_logger(__name__)


def excel_serial_to_date(serial: float) -> str:
    """Converte serial Excel para data ISO."""
    if pd.isna(serial) or serial < 1000:
        return serial
    excel_epoch = datetime(1899, 12, 30)
    date = excel_epoch + timedelta(days=int(serial))
    return date.strftime("%Y-%m-%d")


def excel_serial_to_datetime(serial: float) -> str:
    """Converte serial Excel para datetime ISO."""
    if pd.isna(serial) or serial < 1000:
        return serial
    excel_epoch = datetime(1899, 12, 30)
    days = int(serial)
    fraction = serial - days
    dt = excel_epoch + timedelta(days=days, seconds=int(fraction * 86400))
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def fix_fact_cub_detalhado_v2():
    """Versão corrigida com conversão forçada para numérico."""
    
    print("\n" + "="*70)
    print("  🔧 CORREÇÃO DE DADOS v2 - fact_cub_detalhado")
    print("="*70 + "\n")
    
    # Ler arquivo
    input_file = "docs/fact_cub_detalhado.md"
    print(f"📖 Lendo {input_file}...")
    df = pd.read_csv(input_file, sep="\t", dtype=str)  # LER TUDO COMO STRING
    print(f"  ✓ {len(df):,} linhas carregadas\n")
    
    # CORREÇÃO 1: data_referencia
    print("📅 Corrigindo data_referencia...")
    print(f"  Amostra ANTES: {df['data_referencia'].head(3).tolist()}")
    
    # FORÇAR conversão numérica
    df_numeric = pd.to_numeric(df["data_referencia"], errors='coerce')
    
    # Converter seriais para datas (aplicar na coluna original)
    converted = 0
    for idx in df.index:
        val = df_numeric.loc[idx]
        if pd.notna(val) and val > 1000:
            df.at[idx, "data_referencia"] = excel_serial_to_date(val)
            converted += 1
    
    print(f"  ✓ {converted:,} datas convertidas")
    print(f"  Amostra DEPOIS: {df['data_referencia'].head(3).tolist()}\n")
    
    # CORREÇÃO 2: tipo_cub
    print("🏷️  Corrigindo tipo_cub...")
    mapeamento = {
        "Coluna_0": "CUB-medio",
        "Coluna 0": "CUB-medio",
        "Coluna_1": "Variacao_mensal_%",
        "Coluna 1": "Variacao_mensal_%",
        "Coluna_2": "Variacao_12meses_%",
        "Coluna_3": "Variacao_ano_%",
    }
    df["tipo_cub"] = df["tipo_cub"].replace(mapeamento)
    print(f"  ✓ Tipos mapeados: {df['tipo_cub'].unique().tolist()}\n")
    
    # CORREÇÃO 3: valores -100
    print("🔢 Corrigindo valores -100...")
    
    # FORÇAR conversão numérica (tratando vírgula brasileira)
    if df["valor"].dtype == 'object':
        df["valor"] = df["valor"].str.replace(',', '.')
    df["valor"] = pd.to_numeric(df["valor"], errors='coerce')
    
    count_invalid = (df["valor"] == -100).sum()
    print(f"  Valores -100 encontrados: {count_invalid}")
    df.loc[df["valor"] == -100, "valor"] = None
    print(f"  ✓ Valores -100 substituídos por NULL\n")
    
    # CORREÇÃO 4: created_at
    print("🕐 Corrigindo created_at...")
    print(f"  Amostra ANTES: {df['created_at'].head(3).tolist()}")
    
    # FORÇAR conversão numérica
    df_numeric = pd.to_numeric(df["created_at"], errors='coerce')
    
    # Converter seriais para datetime (aplicar na coluna original)
    converted = 0
    for idx in df.index:
        val = df_numeric.loc[idx]
        if pd.notna(val) and val > 1000:
            df.at[idx, "created_at"] = excel_serial_to_datetime(val)
            converted += 1
    
    print(f"  ✓ {converted:,} timestamps convertidos")
    print(f"  Amostra DEPOIS: {df['created_at'].head(3).tolist()}\n")
    
    # Salvar
    output_file = "docs/fact_cub_detalhado_CORRIGIDO_V2.md"
    df.to_csv(output_file, sep="\t", index=False)
    
    print("="*70)
    print(f"  ✅ CORREÇÃO V2 CONCLUÍDA!")
    print(f"     {output_file}")
    print("="*70 + "\n")
    
    # Estatísticas
    print("📊 ESTATÍSTICAS:")
    print(f"  Total: {len(df):,} registros")
    print(f"  UFs: {df['uf'].nunique()}")
    print(f"  Tipos CUB: {df['tipo_cub'].unique().tolist()}")
    print(f"  Período: {df['data_referencia'].min()} até {df['data_referencia'].max()}")
    print(f"  Valores NULL: {df['valor'].isna().sum()} ({df['valor'].isna().sum()/len(df)*100:.2f}%)\n")


if __name__ == "__main__":
    fix_fact_cub_detalhado_v2()
