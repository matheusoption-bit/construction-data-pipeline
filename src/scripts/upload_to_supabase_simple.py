#!/usr/bin/env python3
"""
🚀 UPLOAD SIMPLES DE DADOS PARA SUPABASE
==========================================

Versão simplificada usando INSERT (não UPSERT).
As tabelas devem ser criadas PRIMEIRO no Supabase.

USO:
    1. Execute o SQL de criação: sql/create_tables_v2.sql no Supabase Dashboard
    2. Execute este script: python src/scripts/upload_to_supabase_simple.py

Autor: Pipeline de Dados
Data: 2026-01-28
"""

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any
import logging
from dotenv import load_dotenv

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Diretórios
PROJECT_ROOT = Path(__file__).parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data" / "production_ready"
ENV_FILE = PROJECT_ROOT / ".env"

# Carregar variáveis de ambiente
load_dotenv(ENV_FILE)

# Configuração do Supabase
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# Tamanho do lote para upload
CHUNK_SIZE = 500  # Menor para evitar timeouts


# ═══════════════════════════════════════════════════════════════════════════════
# MAPEAMENTO: CSV → TABELA SUPABASE (ordem de inserção - dimensões primeiro)
# ═══════════════════════════════════════════════════════════════════════════════

TABLE_ORDER = [
    # Dimensões primeiro (sem dependências)
    "dim_topografia",
    "dim_padrao_acabamento",
    "dim_fatores_pavimento",
    "dim_profundidade_subsolo",
    "dim_cenarios_construcao",
    "dim_tipos_contencao",
    "dim_metodos_construtivos_base",
    "dim_localidade",
    "dim_fatores_regionais_uf",
    "dim_taxas_cartoriais",
    "dim_taxas_municipais",
    "dim_metodos_construtivos",
    "dim_series_bcb",
    # Facts por último
    "fact_cub",
    "fact_macroeconomia",
]


def clean_value(val):
    """Limpa valor para inserção no PostgreSQL."""
    if pd.isna(val):
        return None
    if isinstance(val, (np.integer, np.int64, np.int32)):
        return int(val)
    if isinstance(val, (np.floating, np.float64, np.float32)):
        if np.isnan(val) or np.isinf(val):
            return None
        return float(val)
    if isinstance(val, np.bool_):
        return bool(val)
    return val


def df_to_records(df: pd.DataFrame) -> List[Dict]:
    """Converte DataFrame para lista de dicionários limpos."""
    records = []
    for _, row in df.iterrows():
        record = {col: clean_value(row[col]) for col in df.columns}
        records.append(record)
    return records


def upload_table(client, table_name: str, csv_path: Path) -> tuple:
    """
    Faz upload de uma tabela.
    
    Returns:
        (success: bool, records_inserted: int, error_msg: str or None)
    """
    if not csv_path.exists():
        return False, 0, f"Arquivo não encontrado: {csv_path}"
    
    try:
        # Ler CSV
        df = pd.read_csv(csv_path)
        total_records = len(df)
        
        if total_records == 0:
            return True, 0, None
        
        # Converter para records
        records = df_to_records(df)
        
        # Upload em chunks
        total_inserted = 0
        total_chunks = (total_records + CHUNK_SIZE - 1) // CHUNK_SIZE
        
        for i in range(0, total_records, CHUNK_SIZE):
            chunk = records[i:i + CHUNK_SIZE]
            chunk_num = (i // CHUNK_SIZE) + 1
            
            try:
                # INSERT simples (não UPSERT)
                response = client.table(table_name).insert(chunk).execute()
                total_inserted += len(chunk)
                logger.info(f"   ✓ Chunk {chunk_num}/{total_chunks}: {len(chunk)} registros")
            except Exception as e:
                error_str = str(e)
                # Se for erro de duplicata, ignorar
                if "duplicate key" in error_str.lower():
                    logger.warning(f"   ⚠️ Chunk {chunk_num} tem duplicatas, pulando...")
                    continue
                else:
                    return False, total_inserted, f"Erro no chunk {chunk_num}: {e}"
        
        return True, total_inserted, None
        
    except Exception as e:
        return False, 0, str(e)


def main():
    """Função principal."""
    logger.info("=" * 70)
    logger.info("🚀 UPLOAD SIMPLES PARA SUPABASE")
    logger.info("=" * 70)
    logger.info(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"📁 Origem: {DATA_DIR}")
    
    # Verificar credenciais
    if not SUPABASE_URL or not SUPABASE_KEY:
        logger.error("❌ SUPABASE_URL e SUPABASE_KEY devem estar no arquivo .env")
        sys.exit(1)
    
    # Importar supabase aqui para evitar erro se não instalado
    try:
        from supabase import create_client
    except ImportError:
        logger.error("❌ Biblioteca supabase não instalada. Execute: pip install supabase")
        sys.exit(1)
    
    # Conectar
    logger.info(f"🔗 Conectando ao Supabase...")
    client = create_client(SUPABASE_URL, SUPABASE_KEY)
    logger.info("✅ Conectado!\n")
    
    # Estatísticas
    stats = {
        'sucesso': 0,
        'erro': 0,
        'total_registros': 0,
        'erros': []
    }
    
    # Processar cada tabela na ordem
    for table_name in TABLE_ORDER:
        csv_path = DATA_DIR / f"{table_name}.csv"
        
        logger.info(f"📊 Processando: {table_name}")
        
        success, records, error = upload_table(client, table_name, csv_path)
        
        if success:
            stats['sucesso'] += 1
            stats['total_registros'] += records
            logger.info(f"   ✅ Concluído: {records:,} registros\n")
        else:
            stats['erro'] += 1
            stats['erros'].append(f"{table_name}: {error}")
            logger.error(f"   ❌ Erro: {error}\n")
    
    # Resumo
    logger.info("=" * 70)
    logger.info("📊 RESUMO DO UPLOAD")
    logger.info("=" * 70)
    logger.info(f"✅ Tabelas com sucesso: {stats['sucesso']}")
    logger.info(f"❌ Tabelas com erro: {stats['erro']}")
    logger.info(f"📝 Total de registros inseridos: {stats['total_registros']:,}")
    
    if stats['erros']:
        logger.info("\n⚠️ ERROS ENCONTRADOS:")
        for err in stats['erros']:
            logger.info(f"   - {err}")
    
    logger.info("=" * 70)


if __name__ == "__main__":
    main()
