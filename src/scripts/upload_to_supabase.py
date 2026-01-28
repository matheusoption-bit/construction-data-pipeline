#!/usr/bin/env python3
"""
🚀 UPLOAD DE DADOS PARA SUPABASE
=================================

Script para enviar os dados consolidados da pasta `data/production_ready/`
para o banco PostgreSQL do Supabase.

FUNCIONALIDADES:
- Lê CSVs da pasta production_ready
- Converte NaN para None (compatível com PostgreSQL)
- Faz Upsert em lotes (chunks) de 1000 registros
- Tratamento de erros por arquivo (continua se um falhar)
- Logging detalhado de progresso

CONFIGURAÇÃO:
- Requer SUPABASE_URL e SUPABASE_KEY no arquivo .env

USO:
    python src/scripts/upload_to_supabase.py [--dry-run] [--table TABLE_NAME]

Opções:
    --dry-run       Simula o upload sem enviar dados
    --table NAME    Faz upload apenas da tabela especificada

Autor: Pipeline de Dados - construction-data-pipeline
Data: 2026-01-28
Versão: 1.0
"""

import os
import sys
import argparse
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
CHUNK_SIZE = 1000

# ═══════════════════════════════════════════════════════════════════════════════
# MAPEAMENTO: CSV → TABELA SUPABASE
# ═══════════════════════════════════════════════════════════════════════════════

TABLE_MAPPING: Dict[str, Dict[str, Any]] = {
    # FACTS (tabelas de fatos)
    "fact_cub": {
        "csv_file": "fact_cub.csv",
        "primary_key": "id",
        "upsert_columns": ["data_referencia", "uf", "tipo_cub", "regime_tributario"],
        "description": "Custo Unitário Básico consolidado"
    },
    "fact_macroeconomia": {
        "csv_file": "fact_macroeconomia.csv",
        "primary_key": "id",
        "upsert_columns": ["data_referencia", "indicador"],
        "description": "Indicadores macroeconômicos verticalizados"
    },
    
    # DIMENSIONS (tabelas de dimensão)
    "dim_taxas_municipais": {
        "csv_file": "dim_taxas_municipais.csv",
        "primary_key": "id",
        "upsert_columns": ["cidade", "uf"],
        "description": "Taxas ISS, ITBI, Alvarás por município"
    },
    "dim_metodos_construtivos": {
        "csv_file": "dim_metodos_construtivos.csv",
        "primary_key": "id",
        "upsert_columns": ["id"],
        "description": "Métodos construtivos com fatores regionais"
    },
    "dim_topografia": {
        "csv_file": "dim_topografia.csv",
        "primary_key": "id",
        "upsert_columns": ["topografia_id"],
        "description": "Fatores de topografia"
    },
    "dim_localidade": {
        "csv_file": "dim_localidade.csv",
        "primary_key": "id",
        "upsert_columns": ["id"],
        "description": "UFs e Cidades do Brasil"
    },
    "dim_cenarios_construcao": {
        "csv_file": "dim_cenarios_construcao.csv",
        "primary_key": "id",
        "upsert_columns": ["cenario_id"],
        "description": "Cenários de construção (topografia + subsolo)"
    },
    "dim_tipos_contencao": {
        "csv_file": "dim_tipos_contencao.csv",
        "primary_key": "id",
        "upsert_columns": ["tipo_contencao"],
        "description": "Tipos de contenção e custos"
    },
    "dim_profundidade_subsolo": {
        "csv_file": "dim_profundidade_subsolo.csv",
        "primary_key": "id",
        "upsert_columns": ["profundidade_id"],
        "description": "Multiplicadores por profundidade de subsolo"
    },
    "dim_metodos_construtivos_base": {
        "csv_file": "dim_metodos_construtivos_base.csv",
        "primary_key": "id",
        "upsert_columns": ["metodo_id"],
        "description": "Métodos construtivos base"
    },
    "dim_fatores_pavimento": {
        "csv_file": "dim_fatores_pavimento.csv",
        "primary_key": "id",
        "upsert_columns": ["pavimento_tipo"],
        "description": "Fatores por tipo de pavimento"
    },
    "dim_padrao_acabamento": {
        "csv_file": "dim_padrao_acabamento.csv",
        "primary_key": "id",
        "upsert_columns": ["padrao_id"],
        "description": "Padrões de acabamento CUB"
    },
    "dim_fatores_regionais_uf": {
        "csv_file": "dim_fatores_regionais_uf.csv",
        "primary_key": "id",
        "upsert_columns": ["uf"],
        "description": "Fatores regionais por UF"
    },
    "dim_taxas_cartoriais": {
        "csv_file": "dim_taxas_cartoriais.csv",
        "primary_key": "id",
        "upsert_columns": ["uf"],
        "description": "Taxas cartoriais estaduais"
    },
    "dim_series_bcb": {
        "csv_file": "dim_series_bcb.csv",
        "primary_key": "id",
        "upsert_columns": ["serie_id"],
        "description": "Séries do BCB para monitoramento"
    },
}


class SupabaseUploader:
    """Classe para upload de dados para o Supabase."""
    
    def __init__(self, dry_run: bool = False):
        """
        Inicializa o uploader.
        
        Args:
            dry_run: Se True, apenas simula o upload sem enviar dados.
        """
        self.dry_run = dry_run
        self.client = None
        self.stats = {
            'tabelas_processadas': 0,
            'tabelas_sucesso': 0,
            'tabelas_erro': 0,
            'registros_enviados': 0,
            'erros': []
        }
        
        if not dry_run:
            self._setup_client()
    
    def _setup_client(self):
        """Configura o cliente Supabase."""
        if not SUPABASE_URL or not SUPABASE_KEY:
            logger.error("❌ SUPABASE_URL e SUPABASE_KEY devem estar definidos no .env")
            logger.error("   Exemplo de .env:")
            logger.error("   SUPABASE_URL=https://xxx.supabase.co")
            logger.error("   SUPABASE_KEY=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...")
            sys.exit(1)
        
        try:
            from supabase import create_client, Client
            self.client: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
            logger.info("✅ Conexão com Supabase estabelecida")
            logger.info(f"   URL: {SUPABASE_URL[:50]}...")
        except ImportError:
            logger.error("❌ Biblioteca 'supabase' não instalada.")
            logger.error("   Execute: pip install supabase")
            sys.exit(1)
        except Exception as e:
            logger.error(f"❌ Erro ao conectar com Supabase: {e}")
            sys.exit(1)
    
    def _clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Limpa o DataFrame para compatibilidade com Supabase.
        
        - Converte NaN para None
        - Converte tipos numpy para tipos Python nativos
        - Remove colunas completamente vazias
        """
        # Substituir NaN por None
        df = df.replace({np.nan: None})
        
        # Converter tipos numpy para Python nativo
        for col in df.columns:
            # Converter numpy int/float para Python int/float
            if df[col].dtype in ['int64', 'int32']:
                df[col] = df[col].apply(lambda x: int(x) if pd.notna(x) and x is not None else None)
            elif df[col].dtype in ['float64', 'float32']:
                df[col] = df[col].apply(lambda x: float(x) if pd.notna(x) and x is not None else None)
        
        return df
    
    def _df_to_records(self, df: pd.DataFrame) -> List[Dict]:
        """Converte DataFrame para lista de dicionários."""
        # Limpar o DataFrame
        df = self._clean_dataframe(df)
        
        # Converter para records
        records = df.to_dict(orient='records')
        
        # Garantir que valores None estejam corretos
        for record in records:
            for key, value in record.items():
                if pd.isna(value) or value != value:  # NaN check
                    record[key] = None
        
        return records
    
    def upload_table(self, table_name: str, config: Dict) -> bool:
        """
        Faz upload de uma tabela específica.
        
        Args:
            table_name: Nome da tabela no Supabase
            config: Configuração da tabela (csv_file, primary_key, etc.)
            
        Returns:
            True se sucesso, False se erro
        """
        csv_file = config['csv_file']
        csv_path = DATA_DIR / csv_file
        upsert_cols = config.get('upsert_columns', ['id'])
        
        logger.info(f"\n📊 Processando: {table_name}")
        logger.info(f"   Arquivo: {csv_file}")
        logger.info(f"   Descrição: {config.get('description', 'N/A')}")
        
        # Verificar se arquivo existe
        if not csv_path.exists():
            logger.warning(f"   ⚠️ Arquivo não encontrado: {csv_path}")
            return False
        
        try:
            # Ler CSV
            df = pd.read_csv(csv_path)
            total_records = len(df)
            logger.info(f"   Registros: {total_records:,}")
            
            if total_records == 0:
                logger.warning(f"   ⚠️ Arquivo vazio, pulando...")
                return True
            
            # Converter para records
            records = self._df_to_records(df)
            
            if self.dry_run:
                logger.info(f"   🔄 [DRY-RUN] Simulando upload de {total_records:,} registros")
                logger.info(f"   🔄 [DRY-RUN] Upsert columns: {upsert_cols}")
                # Mostrar amostra
                if records:
                    logger.info(f"   🔄 [DRY-RUN] Amostra do primeiro registro:")
                    for k, v in list(records[0].items())[:5]:
                        logger.info(f"      {k}: {v}")
                self.stats['registros_enviados'] += total_records
                return True
            
            # Upload em chunks
            total_chunks = (total_records + CHUNK_SIZE - 1) // CHUNK_SIZE
            
            for i in range(0, total_records, CHUNK_SIZE):
                chunk = records[i:i + CHUNK_SIZE]
                chunk_num = (i // CHUNK_SIZE) + 1
                
                try:
                    # Usar upsert com on_conflict
                    response = self.client.table(table_name).upsert(
                        chunk,
                        on_conflict=','.join(upsert_cols)
                    ).execute()
                    
                    logger.info(f"   ✓ Chunk {chunk_num}/{total_chunks}: {len(chunk)} registros enviados")
                    self.stats['registros_enviados'] += len(chunk)
                    
                except Exception as e:
                    logger.error(f"   ❌ Erro no chunk {chunk_num}: {e}")
                    # Tentar inserir registro por registro para identificar o problema
                    if len(chunk) > 1:
                        logger.info(f"   🔄 Tentando inserção individual...")
                        success_count = 0
                        for record in chunk:
                            try:
                                self.client.table(table_name).upsert(
                                    [record],
                                    on_conflict=','.join(upsert_cols)
                                ).execute()
                                success_count += 1
                            except Exception as e2:
                                logger.debug(f"      Registro falhou: {e2}")
                        logger.info(f"   ✓ {success_count}/{len(chunk)} registros individuais enviados")
                        self.stats['registros_enviados'] += success_count
            
            logger.info(f"   ✅ Upload concluído: {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"   ❌ Erro ao processar {table_name}: {e}")
            self.stats['erros'].append(f"{table_name}: {e}")
            return False
    
    def run(self, specific_table: Optional[str] = None):
        """
        Executa o upload de todas as tabelas ou de uma específica.
        
        Args:
            specific_table: Se fornecido, faz upload apenas desta tabela.
        """
        logger.info("=" * 70)
        logger.info("🚀 UPLOAD DE DADOS PARA SUPABASE")
        logger.info("=" * 70)
        logger.info(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"📁 Origem: {DATA_DIR}")
        
        if self.dry_run:
            logger.info("⚠️ MODO DRY-RUN: Nenhum dado será enviado")
        
        logger.info("")
        
        # Filtrar tabelas se específica
        tables_to_process = TABLE_MAPPING
        if specific_table:
            if specific_table not in TABLE_MAPPING:
                logger.error(f"❌ Tabela '{specific_table}' não encontrada no mapeamento.")
                logger.error(f"   Tabelas disponíveis: {list(TABLE_MAPPING.keys())}")
                return
            tables_to_process = {specific_table: TABLE_MAPPING[specific_table]}
        
        # Processar cada tabela
        for table_name, config in tables_to_process.items():
            self.stats['tabelas_processadas'] += 1
            
            success = self.upload_table(table_name, config)
            
            if success:
                self.stats['tabelas_sucesso'] += 1
            else:
                self.stats['tabelas_erro'] += 1
        
        # Resumo
        self._print_summary()
    
    def _print_summary(self):
        """Imprime resumo da execução."""
        logger.info("")
        logger.info("=" * 70)
        logger.info("📊 RESUMO DO UPLOAD")
        logger.info("=" * 70)
        logger.info(f"   • Tabelas processadas: {self.stats['tabelas_processadas']}")
        logger.info(f"   • Tabelas com sucesso: {self.stats['tabelas_sucesso']}")
        logger.info(f"   • Tabelas com erro: {self.stats['tabelas_erro']}")
        logger.info(f"   • Registros enviados: {self.stats['registros_enviados']:,}")
        
        if self.stats['erros']:
            logger.warning("")
            logger.warning("⚠️ ERROS ENCONTRADOS:")
            for erro in self.stats['erros']:
                logger.warning(f"   - {erro}")
        
        logger.info("")
        if self.dry_run:
            logger.info("=" * 70)
            logger.info("⚠️ MODO DRY-RUN: Execute sem --dry-run para enviar os dados")
            logger.info("=" * 70)
        else:
            logger.info("=" * 70)
            logger.info("✅ UPLOAD CONCLUÍDO!")
            logger.info("=" * 70)


def main():
    """Função principal."""
    parser = argparse.ArgumentParser(
        description='Upload de dados para Supabase',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos:
  python upload_to_supabase.py                    # Upload de todas as tabelas
  python upload_to_supabase.py --dry-run          # Simula sem enviar
  python upload_to_supabase.py --table fact_cub   # Upload apenas de fact_cub
  python upload_to_supabase.py --list             # Lista tabelas disponíveis
        """
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Simula o upload sem enviar dados'
    )
    
    parser.add_argument(
        '--table',
        type=str,
        help='Faz upload apenas da tabela especificada'
    )
    
    parser.add_argument(
        '--list',
        action='store_true',
        help='Lista todas as tabelas disponíveis'
    )
    
    args = parser.parse_args()
    
    # Listar tabelas
    if args.list:
        print("\n📋 TABELAS DISPONÍVEIS:")
        print("-" * 70)
        for table_name, config in TABLE_MAPPING.items():
            csv_path = DATA_DIR / config['csv_file']
            exists = "✓" if csv_path.exists() else "✗"
            print(f"  {exists} {table_name:<35} <- {config['csv_file']}")
        print("")
        return
    
    # Executar upload
    uploader = SupabaseUploader(dry_run=args.dry_run)
    uploader.run(specific_table=args.table)


if __name__ == "__main__":
    main()
