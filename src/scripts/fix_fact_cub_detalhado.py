"""
Script para corrigir dados malformados em fact_cub_detalhado.md

Corrige:
1. Datas seriais Excel → ISO format (YYYY-MM-DD)
2. Tipos CUB genéricos → Descritivos
3. Valores -100 → NULL
4. Created_at serial → Datetime ISO

Uso:
    python -m src.scripts.fix_fact_cub_detalhado
"""

import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import shutil
import structlog

logger = structlog.get_logger(__name__)


# Excel epoch: 1899-12-30 (Windows)
EXCEL_EPOCH = datetime(1899, 12, 30)


def excel_serial_to_date(serial: float) -> str:
    """
    Converte serial Excel para data ISO.
    
    Excel armazena datas como número de dias desde 1899-12-30.
    
    Args:
        serial: Número serial do Excel (ex: 45413)
    
    Returns:
        Data no formato ISO: "YYYY-MM-DD"
    
    Exemplo:
        >>> excel_serial_to_date(45413)
        '2024-05-01'
    """
    try:
        # Converter serial para inteiro (ignorar fração)
        days = int(serial)
        
        # Calcular data
        date = EXCEL_EPOCH + timedelta(days=days)
        
        return date.strftime("%Y-%m-%d")
    
    except Exception as e:
        logger.warning("date_conversion_failed", serial=serial, error=str(e))
        return str(serial)


def excel_serial_to_datetime(serial: float) -> str:
    """
    Converte serial Excel com fração para datetime ISO.
    
    Parte inteira = dias desde epoch
    Parte decimal = fração do dia (0.5 = meio-dia)
    
    Args:
        serial: Número serial do Excel com decimais (ex: 45972.74425)
    
    Returns:
        Datetime no formato ISO: "YYYY-MM-DD HH:MM:SS"
    
    Exemplo:
        >>> excel_serial_to_datetime(45972.74425)
        '2025-11-11 17:51:43'
    """
    try:
        # Parte inteira = dias
        days = int(serial)
        
        # Parte decimal = fração do dia
        fraction = serial - days
        
        # Converter para segundos (86400 seg/dia)
        seconds = int(fraction * 86400)
        
        # Calcular datetime
        dt = EXCEL_EPOCH + timedelta(days=days, seconds=seconds)
        
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    
    except Exception as e:
        logger.warning("datetime_conversion_failed", serial=serial, error=str(e))
        return str(serial)


def map_tipo_cub(tipo_generico: str) -> str:
    """
    Mapeia tipo CUB genérico para descritivo.
    
    Args:
        tipo_generico: Valor genérico (ex: "Coluna_0", "Coluna 1")
    
    Returns:
        Nome descritivo do tipo CUB
    
    Mapeamento:
        - Coluna_0/Coluna 0 → CUB-medio
        - Coluna_1/Coluna 1 → Variacao_mensal_%
        - Coluna_2/Coluna 2 → Variacao_12meses_%
        - Coluna_3/Coluna 3 → Variacao_ano_%
    """
    # Normalizar formato (remover espaços, underscores)
    tipo_norm = tipo_generico.replace(" ", "_").upper()
    
    mapping = {
        "COLUNA_0": "CUB-medio",
        "COLUNA_1": "Variacao_mensal_%",
        "COLUNA_2": "Variacao_12meses_%",
        "COLUNA_3": "Variacao_ano_%"
    }
    
    return mapping.get(tipo_norm, tipo_generico)


def fix_fact_cub_detalhado(input_file: str = "docs/fact_cub_detalhado.md"):
    """
    Função principal para corrigir fact_cub_detalhado.
    
    Processo:
    1. Ler arquivo TSV
    2. Criar backup
    3. Corrigir data_referencia (serial → ISO date)
    4. Corrigir tipo_cub (genérico → descritivo)
    5. Corrigir valores -100 → None
    6. Corrigir created_at (serial → ISO datetime)
    7. Salvar arquivo corrigido
    8. Exibir estatísticas
    
    Args:
        input_file: Caminho do arquivo a corrigir
    """
    print("\n" + "="*80)
    print("  🔧 CORREÇÃO DE DADOS - fact_cub_detalhado")
    print("="*80 + "\n")
    
    input_path = Path(input_file)
    
    if not input_path.exists():
        print(f"❌ Arquivo não encontrado: {input_file}\n")
        return
    
    # 1. BACKUP
    print("📦 Criando backup...")
    backup_path = input_path.with_suffix('.md_BACKUP.md')
    shutil.copy2(input_path, backup_path)
    print(f"  ✓ Backup salvo: {backup_path.name}\n")
    
    logger.info("backup_created", path=str(backup_path))
    
    # 2. LER DADOS
    print("📖 Lendo arquivo TSV...")
    
    try:
        # Ler como TSV (tab-separated)
        df = pd.read_csv(input_path, sep='\t')
        
        print(f"  ✓ {len(df):,} linhas carregadas\n")
        
        logger.info("file_loaded", rows=len(df), columns=df.columns.tolist())
    
    except Exception as e:
        print(f"❌ Erro ao ler arquivo: {str(e)}\n")
        logger.error("file_read_failed", error=str(e))
        return
    
    # 3. CORRIGIR data_referencia
    print("📅 Corrigindo data_referencia...")
    
    if 'data_referencia' in df.columns:
        # Detectar se é serial (numérico) ou já está corrigido (string)
        sample_value = df['data_referencia'].iloc[0] if len(df) > 0 else None
        
        if sample_value and isinstance(sample_value, (int, float)):
            # Aplicar conversão
            df['data_referencia'] = df['data_referencia'].apply(
                lambda x: excel_serial_to_date(x) if pd.notna(x) else None
            )
            print(f"  ✓ {len(df)} datas convertidas\n")
            logger.info("dates_converted", rows=len(df))
        else:
            print("  ℹ️  Datas já estão no formato correto\n")
    else:
        print("  ⚠️  Coluna 'data_referencia' não encontrada\n")
    
    # 4. CORRIGIR tipo_cub
    print("🏷️  Corrigindo tipo_cub...")
    
    if 'tipo_cub' in df.columns:
        # Contar tipos antes
        tipos_antes = df['tipo_cub'].nunique()
        
        # Aplicar mapeamento
        df['tipo_cub'] = df['tipo_cub'].apply(map_tipo_cub)
        
        # Contar tipos depois
        tipos_depois = df['tipo_cub'].nunique()
        
        print(f"  ✓ Tipos mapeados: {tipos_antes} → {tipos_depois}\n")
        logger.info("tipos_mapped", before=tipos_antes, after=tipos_depois)
    else:
        print("  ⚠️  Coluna 'tipo_cub' não encontrada\n")
    
    # 5. CORRIGIR valores -100
    print("🔢 Corrigindo valores -100...")
    
    if 'valor' in df.columns:
        # Contar valores -100
        count_invalid = (df['valor'] == -100).sum()
        
        # Substituir por None
        df.loc[df['valor'] == -100, 'valor'] = None
        
        print(f"  ✓ {count_invalid:,} valores -100 substituídos por NULL\n")
        logger.info("invalid_values_fixed", count=count_invalid)
    else:
        print("  ⚠️  Coluna 'valor' não encontrada\n")
    
    # 6. CORRIGIR created_at
    print("🕐 Corrigindo created_at...")
    
    if 'created_at' in df.columns:
        # Detectar se é serial ou já está corrigido
        sample_value = df['created_at'].iloc[0] if len(df) > 0 else None
        
        if sample_value and isinstance(sample_value, (int, float)):
            # Aplicar conversão
            df['created_at'] = df['created_at'].apply(
                lambda x: excel_serial_to_datetime(x) if pd.notna(x) else None
            )
            print(f"  ✓ {len(df)} timestamps convertidos\n")
            logger.info("timestamps_converted", rows=len(df))
        else:
            print("  ℹ️  Timestamps já estão no formato correto\n")
    else:
        print("  ⚠️  Coluna 'created_at' não encontrada\n")
    
    # 7. SALVAR ARQUIVO CORRIGIDO
    print("💾 Salvando arquivo corrigido...")
    
    output_path = input_path.with_stem(input_path.stem + "_CORRIGIDO")
    
    try:
        # Salvar como TSV
        df.to_csv(output_path, sep='\t', index=False)
        
        print(f"  ✓ Arquivo salvo: {output_path.name}\n")
        
        logger.info("file_saved", path=str(output_path))
    
    except Exception as e:
        print(f"❌ Erro ao salvar arquivo: {str(e)}\n")
        logger.error("file_save_failed", error=str(e))
        return
    
    # 8. ESTATÍSTICAS
    print("="*80)
    print("  📊 ESTATÍSTICAS FINAIS")
    print("="*80 + "\n")
    
    # Total de linhas
    print(f"📈 Total de registros: {len(df):,}")
    
    # UFs únicas
    if 'uf' in df.columns:
        ufs = df['uf'].nunique()
        print(f"🗺️  Estados (UFs): {ufs}")
    
    # Tipos de CUB
    if 'tipo_cub' in df.columns:
        tipos = df['tipo_cub'].nunique()
        tipos_list = df['tipo_cub'].unique().tolist()
        print(f"🏗️  Tipos de CUB: {tipos}")
        print(f"    {', '.join(tipos_list)}")
    
    # Período temporal
    if 'data_referencia' in df.columns:
        df['data_referencia'] = pd.to_datetime(df['data_referencia'], errors='coerce')
        min_date = df['data_referencia'].min()
        max_date = df['data_referencia'].max()
        print(f"📆 Período: {min_date.strftime('%Y-%m-%d')} até {max_date.strftime('%Y-%m-%d')}")
    
    # Valores NULL
    if 'valor' in df.columns:
        null_count = df['valor'].isna().sum()
        null_pct = (null_count / len(df) * 100)
        print(f"❌ Valores NULL: {null_count:,} ({null_pct:.2f}%)")
    
    print("\n" + "="*80)
    print("  ✅ CORREÇÃO CONCLUÍDA COM SUCESSO!")
    print("="*80 + "\n")
    
    print(f"📁 Arquivos gerados:")
    print(f"   • Backup: {backup_path.name}")
    print(f"   • Corrigido: {output_path.name}\n")


def main():
    """Entry point do script."""
    
    # Configurar logger
    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.add_log_level,
            structlog.processors.JSONRenderer()
        ],
        logger_factory=structlog.PrintLoggerFactory(),
    )
    
    # Executar correção
    fix_fact_cub_detalhado()
    
    return 0


if __name__ == "__main__":
    import sys
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\n⚠️  Interrompido pelo usuário\n")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ ERRO FATAL: {str(e)}\n")
        logger.error("fatal_error", error=str(e), exc_info=True)
        sys.exit(1)
