"""
Script para separar fact_cub_detalhado em 2 abas temáticas.

Separa os dados em:
1. fact_cub_por_uf.md - Apenas CUB-medio por estado
2. fact_cub_variacao.md - Apenas variações percentuais

Uso:
    python -m src.scripts.split_cub_tabs
    python -m src.scripts.split_cub_tabs --input custom.md --output-dir output/

Saída:
    - docs/fact_cub_por_uf.md (CUB-medio)
    - docs/fact_cub_variacao.md (Variações percentuais)
"""

import sys
import argparse
from pathlib import Path
from typing import Tuple

import pandas as pd
import structlog
from tqdm import tqdm

# Configurar logger
logger = structlog.get_logger(__name__)


def split_cub_data(input_file: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Separa dados CUB em duas categorias.
    
    Args:
        input_file: Caminho para o arquivo de entrada
        
    Returns:
        Tupla com (df_cub_medio, df_variacao)
        
    Raises:
        FileNotFoundError: Se arquivo não existe
        ValueError: Se arquivo está vazio ou malformado
    """
    print(f"\n📖 Lendo dados de {input_file}...")
    
    if not Path(input_file).exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {input_file}")
    
    try:
        # Ler arquivo TSV
        df = pd.read_csv(input_file, sep="\t")
        
        if df.empty:
            raise ValueError(f"Arquivo está vazio: {input_file}")
        
        total_records = len(df)
        print(f"  ✅ {total_records:,} registros carregados\n")
        
        logger.info("data_loaded", 
                   file=input_file, 
                   total_records=total_records,
                   columns=list(df.columns))
        
        # Filtrar CUB-medio
        print("🔍 Filtrando dados...")
        df_cub = df[df['tipo_cub'] == 'CUB-medio'].copy()
        cub_count = len(df_cub)
        print(f"  ✅ {cub_count:,} registros CUB-medio")
        
        logger.info("cub_filtered", records=cub_count)
        
        # Filtrar variações (qualquer tipo com "Variacao" no nome)
        df_variacao = df[df['tipo_cub'].str.contains('Variacao', na=False)].copy()
        var_count = len(df_variacao)
        print(f"  ✅ {var_count:,} registros de variação")
        
        # Mostrar tipos de variação encontrados
        tipos_var = df_variacao['tipo_cub'].unique()
        print(f"     Tipos: {', '.join(tipos_var)}")
        
        logger.info("variacao_filtered", 
                   records=var_count,
                   tipos=list(tipos_var))
        
        # Validar soma
        soma = cub_count + var_count
        if soma != total_records:
            logger.warning("validation_mismatch",
                          total=total_records,
                          cub=cub_count,
                          variacao=var_count,
                          soma=soma)
            print(f"\n  ⚠️  ATENÇÃO: Soma ({soma:,}) != Total ({total_records:,})")
            print(f"     Diferença: {abs(soma - total_records):,} registros não classificados")
        else:
            print(f"  ✅ Validação OK: {soma:,} = {total_records:,} (100%)\n")
            logger.info("validation_success", total=total_records)
        
        return df_cub, df_variacao
        
    except Exception as e:
        logger.error("split_failed", error=str(e), exc_info=True)
        raise ValueError(f"Erro ao processar arquivo: {str(e)}")


def save_tabs(
    df_cub: pd.DataFrame, 
    df_variacao: pd.DataFrame,
    output_dir: str
) -> Tuple[str, str]:
    """
    Salva DataFrames em arquivos TSV separados.
    
    Args:
        df_cub: DataFrame com CUB-medio
        df_variacao: DataFrame com variações
        output_dir: Diretório de saída
        
    Returns:
        Tupla com (caminho_cub, caminho_variacao)
    """
    # Garantir que diretório existe
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    # Definir caminhos de saída
    output_cub = Path(output_dir) / "fact_cub_por_uf.md"
    output_var = Path(output_dir) / "fact_cub_variacao.md"
    
    print("💾 Salvando arquivos...")
    
    # Salvar CUB-medio
    with tqdm(total=1, desc="  fact_cub_por_uf.md", ncols=80, leave=False) as pbar:
        df_cub.to_csv(output_cub, sep="\t", index=False)
        pbar.update(1)
    
    print(f"  ✅ {output_cub} ({len(df_cub):,} registros)")
    logger.info("file_saved", 
               file=str(output_cub), 
               records=len(df_cub),
               size_bytes=output_cub.stat().st_size)
    
    # Salvar variações
    with tqdm(total=1, desc="  fact_cub_variacao.md", ncols=80, leave=False) as pbar:
        df_variacao.to_csv(output_var, sep="\t", index=False)
        pbar.update(1)
    
    print(f"  ✅ {output_var} ({len(df_variacao):,} registros)\n")
    logger.info("file_saved", 
               file=str(output_var), 
               records=len(df_variacao),
               size_bytes=output_var.stat().st_size)
    
    return str(output_cub), str(output_var)


def display_statistics(df_cub: pd.DataFrame, df_variacao: pd.DataFrame):
    """
    Exibe estatísticas detalhadas sobre os dados separados.
    
    Args:
        df_cub: DataFrame com CUB-medio
        df_variacao: DataFrame com variações
    """
    print("="*80)
    print("  📊 ESTATÍSTICAS")
    print("="*80 + "\n")
    
    # Estatísticas CUB-medio
    print("🏗️  CUB-MEDIO POR UF:")
    print(f"   • Total de registros: {len(df_cub):,}")
    print(f"   • UFs únicas: {df_cub['uf'].nunique()}")
    print(f"   • UFs: {', '.join(sorted(df_cub['uf'].unique()))}")
    
    if 'data_referencia' in df_cub.columns:
        datas_validas_cub = df_cub['data_referencia'].dropna()
        if len(datas_validas_cub) > 0:
            print(f"   • Período: {datas_validas_cub.min()} até {datas_validas_cub.max()}")
    
    if 'valor' in df_cub.columns:
        valores_validos_cub = df_cub['valor'].dropna()
        if len(valores_validos_cub) > 0:
            print(f"   • Valor médio: R$ {valores_validos_cub.mean():.2f}")
            print(f"   • Valor mínimo: R$ {valores_validos_cub.min():.2f}")
            print(f"   • Valor máximo: R$ {valores_validos_cub.max():.2f}")
    
    print()
    
    # Estatísticas variações
    print("📈 VARIAÇÕES:")
    print(f"   • Total de registros: {len(df_variacao):,}")
    print(f"   • UFs únicas: {df_variacao['uf'].nunique()}")
    print(f"   • Tipos de variação: {df_variacao['tipo_cub'].nunique()}")
    
    # Detalhar por tipo
    for tipo in sorted(df_variacao['tipo_cub'].unique()):
        count = (df_variacao['tipo_cub'] == tipo).sum()
        print(f"     - {tipo}: {count:,} registros")
    
    if 'data_referencia' in df_variacao.columns:
        datas_validas_var = df_variacao['data_referencia'].dropna()
        if len(datas_validas_var) > 0:
            print(f"   • Período: {datas_validas_var.min()} até {datas_validas_var.max()}")
    
    print()
    
    # Comparação
    total = len(df_cub) + len(df_variacao)
    perc_cub = (len(df_cub) / total * 100) if total > 0 else 0
    perc_var = (len(df_variacao) / total * 100) if total > 0 else 0
    
    print("📊 DISTRIBUIÇÃO:")
    print(f"   • CUB-medio: {len(df_cub):,} ({perc_cub:.1f}%)")
    print(f"   • Variações: {len(df_variacao):,} ({perc_var:.1f}%)")
    print(f"   • TOTAL: {total:,} (100%)")
    print()


def main():
    """Função principal do script."""
    
    # Parse argumentos CLI
    parser = argparse.ArgumentParser(
        description="Separar fact_cub_detalhado em abas temáticas",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos de uso:
  python -m src.scripts.split_cub_tabs
  python -m src.scripts.split_cub_tabs --input custom.md
  python -m src.scripts.split_cub_tabs --output-dir output/
  
Saída:
  1. fact_cub_por_uf.md - CUB-medio por estado
  2. fact_cub_variacao.md - Variações percentuais (mensal, 12 meses, anual)
        """
    )
    
    parser.add_argument(
        "--input",
        type=str,
        default="docs/fact_cub_detalhado_CORRIGIDO_V3.md",
        help="Caminho do arquivo de entrada (default: docs/fact_cub_detalhado_CORRIGIDO_V3.md)"
    )
    
    parser.add_argument(
        "--output-dir",
        type=str,
        default="docs/",
        help="Diretório de saída (default: docs/)"
    )
    
    args = parser.parse_args()
    
    # Banner
    print("\n" + "="*80)
    print("  📊 SEPARAÇÃO DE DADOS CUB EM ABAS TEMÁTICAS")
    print("="*80)
    
    try:
        # 1. Separar dados
        df_cub, df_variacao = split_cub_data(args.input)
        
        # 2. Salvar arquivos
        path_cub, path_var = save_tabs(df_cub, df_variacao, args.output_dir)
        
        # 3. Exibir estatísticas
        display_statistics(df_cub, df_variacao)
        
        # 4. Resumo final
        print("="*80)
        print("  ✅ SEPARAÇÃO CONCLUÍDA COM SUCESSO!")
        print("="*80 + "\n")
        
        print("📁 Arquivos gerados:")
        print(f"   1. {path_cub}")
        print(f"   2. {path_var}")
        print()
        
        logger.info("script_completed",
                   input_file=args.input,
                   output_cub=path_cub,
                   output_variacao=path_var,
                   cub_records=len(df_cub),
                   variacao_records=len(df_variacao))
        
        sys.exit(0)
        
    except FileNotFoundError as e:
        print(f"\n❌ ERRO: {str(e)}\n")
        logger.error("file_not_found", error=str(e))
        sys.exit(1)
        
    except ValueError as e:
        print(f"\n❌ ERRO: {str(e)}\n")
        logger.error("value_error", error=str(e))
        sys.exit(1)
        
    except Exception as e:
        print(f"\n❌ ERRO FATAL: {str(e)}\n")
        logger.error("fatal_error", error=str(e), exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
