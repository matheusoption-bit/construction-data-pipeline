#!/usr/bin/env python3
"""
🔄 SCRIPT DE ATUALIZAÇÃO DE TABELAS DE CONFIGURAÇÃO
=====================================================

Script para carregar arquivos CSV/JSON de configuração e preparar
para upload no Supabase (PostgreSQL).

RESPONSABILIDADES:
1. Ler arquivos de configuração locais (configs/)
2. Validar e transformar dados
3. Gerar arquivos production_ready para upload
4. Opcionalmente fazer upload direto via API Supabase

USO:
    python src/scripts/update_config_tables.py [--upload]

Autor: Pipeline de Dados - construction-data-pipeline
Data: 2026-01-28
Versão: 1.0
"""

import os
import sys
import json
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any
import logging

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Diretórios
PROJECT_ROOT = Path(__file__).parent.parent.parent
CONFIG_DIR = PROJECT_ROOT / "configs"
OUTPUT_DIR = PROJECT_ROOT / "data" / "production_ready"


class ConfigTableUpdater:
    """Classe para atualização de tabelas de configuração."""
    
    def __init__(self):
        """Inicializa o atualizador."""
        self.stats = {
            'arquivos_lidos': 0,
            'tabelas_geradas': 0,
            'registros_total': 0,
            'erros': []
        }
        
        # Garantir que diretório de output existe
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    # =========================================================================
    # LEITURA DE ARQUIVOS DE CONFIGURAÇÃO
    # =========================================================================
    
    def _read_csv(self, filename: str) -> Optional[pd.DataFrame]:
        """Lê um arquivo CSV da pasta configs."""
        filepath = CONFIG_DIR / filename
        if not filepath.exists():
            logger.warning(f"Arquivo não encontrado: {filepath}")
            return None
        
        try:
            df = pd.read_csv(filepath)
            self.stats['arquivos_lidos'] += 1
            logger.info(f"✓ Lido: {filename} ({len(df)} registros)")
            return df
        except Exception as e:
            logger.error(f"✗ Erro ao ler {filename}: {e}")
            self.stats['erros'].append(f"{filename}: {e}")
            return None
    
    def _read_json(self, filename: str) -> Optional[Dict]:
        """Lê um arquivo JSON da pasta configs."""
        filepath = CONFIG_DIR / filename
        if not filepath.exists():
            logger.warning(f"Arquivo não encontrado: {filepath}")
            return None
        
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            self.stats['arquivos_lidos'] += 1
            logger.info(f"✓ Lido: {filename}")
            return data
        except Exception as e:
            logger.error(f"✗ Erro ao ler {filename}: {e}")
            self.stats['erros'].append(f"{filename}: {e}")
            return None
    
    def _save_csv(self, df: pd.DataFrame, tablename: str) -> bool:
        """Salva DataFrame como CSV na pasta production_ready."""
        try:
            filepath = OUTPUT_DIR / f"{tablename}.csv"
            df.to_csv(filepath, index=False)
            self.stats['tabelas_geradas'] += 1
            self.stats['registros_total'] += len(df)
            logger.info(f"💾 Salvo: {tablename}.csv ({len(df)} registros)")
            return True
        except Exception as e:
            logger.error(f"✗ Erro ao salvar {tablename}: {e}")
            self.stats['erros'].append(f"Salvar {tablename}: {e}")
            return False
    
    # =========================================================================
    # PROCESSAMENTO DE TABELAS ESPECÍFICAS
    # =========================================================================
    
    def process_taxas_municipais(self) -> Optional[pd.DataFrame]:
        """Processa taxas municipais de SC."""
        df = self._read_csv('taxas_municipais_sc.csv')
        if df is None:
            return None
        
        # Padronizar colunas para snake_case
        df.columns = [col.lower().replace(' ', '_') for col in df.columns]
        
        # Converter tipos numéricos
        numeric_cols = [
            'fator_regional', 'iss_aliquota', 'alvara_valor_base', 'alvara_valor_m2',
            'habite_se_valor_m2', 'habite_se_valor_fixo', 'habite_se_minimo',
            'habite_se_sanitario_m2', 'habite_se_sanitario_vistoria',
            'inss_aliquota', 'inss_base_percent', 'art_rrt_base', 'art_rrt_medio',
            'itbi_aliquota'
        ]
        
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Adicionar timestamps
        df['created_at'] = datetime.now().isoformat()
        df['updated_at'] = datetime.now().isoformat()
        
        return df
    
    def process_cenarios_construcao(self) -> Optional[pd.DataFrame]:
        """Processa cenários de construção."""
        df = self._read_csv('dim_cenarios_construcao.csv')
        if df is None:
            return None
        
        # Converter tipos numéricos
        numeric_cols = [
            'multiplicador_custo', 'multiplicador_prazo',
            'fator_fundacao', 'fator_estrutura', 'fator_terraplanagem',
            'fator_contencao', 'fator_drenagem', 'contingencia_percent'
        ]
        
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        return df
    
    def process_tipos_contencao(self) -> Optional[pd.DataFrame]:
        """Processa tipos de contenção."""
        df = self._read_csv('dim_tipos_contencao.csv')
        if df is None:
            return None
        
        df['custo_m2'] = pd.to_numeric(df['custo_m2'], errors='coerce')
        
        return df
    
    def process_profundidade_subsolo(self) -> Optional[pd.DataFrame]:
        """Processa profundidades de subsolo."""
        df = self._read_csv('dim_profundidade_subsolo.csv')
        if df is None:
            return None
        
        numeric_cols = ['faixa_metros_min', 'faixa_metros_max', 'multiplicador']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        return df
    
    def process_metodos_construtivos(self) -> Optional[pd.DataFrame]:
        """Processa métodos construtivos base."""
        df = self._read_csv('dim_metodos_construtivos_base.csv')
        if df is None:
            return None
        
        df['fator_custo'] = pd.to_numeric(df['fator_custo'], errors='coerce')
        df['custo_base_ref'] = pd.to_numeric(df['custo_base_ref'], errors='coerce')
        
        return df
    
    def process_topografia(self) -> Optional[pd.DataFrame]:
        """Processa fatores de topografia."""
        df = self._read_csv('dim_topografia.csv')
        if df is None:
            return None
        
        df['fator_custo'] = pd.to_numeric(df['fator_custo'], errors='coerce')
        
        return df
    
    def process_fatores_pavimento(self) -> Optional[pd.DataFrame]:
        """Processa fatores por tipo de pavimento."""
        df = self._read_csv('dim_fatores_pavimento.csv')
        if df is None:
            return None
        
        df['fator_custo'] = pd.to_numeric(df['fator_custo'], errors='coerce')
        
        return df
    
    def process_padrao_acabamento(self) -> Optional[pd.DataFrame]:
        """Processa padrões de acabamento."""
        df = self._read_csv('dim_padrao_acabamento.csv')
        return df
    
    def process_fatores_regionais(self) -> Optional[pd.DataFrame]:
        """Processa fatores regionais por UF."""
        df = self._read_csv('fatores_regionais_uf.csv')
        if df is None:
            return None
        
        numeric_cols = ['fator_regional', 'cub_r1b', 'cub_r1n', 'cub_r1a', 'inflacao_mensal']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        return df
    
    def process_taxas_cartoriais(self) -> Optional[pd.DataFrame]:
        """Processa taxas cartoriais estaduais."""
        df = self._read_csv('taxas_cartoriais_estaduais.csv')
        if df is None:
            return None
        
        # Converter todas as colunas numéricas
        for col in df.columns:
            if 'faixa' in col or 'pct' in col or 'emolumentos' in col or 'taxa' in col or 'aliquota' in col:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        return df
    
    def process_series_mapping(self) -> Optional[pd.DataFrame]:
        """
        Processa o series_mapping.json e gera tabela de séries BCB.
        """
        data = self._read_json('series_mapping.json')
        if data is None:
            return None
        
        series_list = []
        
        # Processar séries SGS
        if 'series' in data:
            for serie_id, info in data['series'].items():
                series_list.append({
                    'serie_id': serie_id,
                    'bcb_code': info.get('bcb_code'),
                    'nome_completo': info.get('nome_completo'),
                    'categoria': info.get('categoria'),
                    'unidade': info.get('unidade'),
                    'frequencia': info.get('frequencia'),
                    'relevancia_bi': info.get('relevancia_bi'),
                    'uso': info.get('uso'),
                    'tipo_fonte': 'SGS',
                    'endpoint': None
                })
        
        # Processar expectativas Focus
        if 'expectativas_focus' in data:
            for exp_id, info in data['expectativas_focus'].get('endpoints', {}).items():
                series_list.append({
                    'serie_id': f"focus_{exp_id}",
                    'bcb_code': None,
                    'nome_completo': info.get('nome'),
                    'categoria': 'expectativas',
                    'unidade': None,
                    'frequencia': info.get('frequencia'),
                    'relevancia_bi': info.get('relevancia_bi'),
                    'uso': info.get('uso'),
                    'tipo_fonte': 'FOCUS_ODATA',
                    'endpoint': info.get('endpoint')
                })
        
        df = pd.DataFrame(series_list)
        df.insert(0, 'id', range(1, len(df) + 1))
        
        return df
    
    # =========================================================================
    # EXECUÇÃO PRINCIPAL
    # =========================================================================
    
    def run(self):
        """Executa o processamento de todas as tabelas."""
        logger.info("=" * 70)
        logger.info("🔄 ATUALIZAÇÃO DE TABELAS DE CONFIGURAÇÃO")
        logger.info("=" * 70)
        logger.info(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"📁 Config: {CONFIG_DIR}")
        logger.info(f"📁 Output: {OUTPUT_DIR}")
        logger.info("")
        
        # Lista de processadores
        processors = [
            ('dim_taxas_municipais', self.process_taxas_municipais),
            ('dim_cenarios_construcao', self.process_cenarios_construcao),
            ('dim_tipos_contencao', self.process_tipos_contencao),
            ('dim_profundidade_subsolo', self.process_profundidade_subsolo),
            ('dim_metodos_construtivos_base', self.process_metodos_construtivos),
            ('dim_topografia', self.process_topografia),
            ('dim_fatores_pavimento', self.process_fatores_pavimento),
            ('dim_padrao_acabamento', self.process_padrao_acabamento),
            ('dim_fatores_regionais_uf', self.process_fatores_regionais),
            ('dim_taxas_cartoriais', self.process_taxas_cartoriais),
            ('dim_series_bcb', self.process_series_mapping),
        ]
        
        # Processar cada tabela
        for tablename, processor in processors:
            logger.info(f"📊 Processando {tablename}...")
            df = processor()
            if df is not None and not df.empty:
                self._save_csv(df, tablename)
            else:
                logger.warning(f"   ⚠️ Nenhum dado para {tablename}")
        
        # Resumo
        self._print_summary()
    
    def _print_summary(self):
        """Imprime resumo da execução."""
        logger.info("")
        logger.info("=" * 70)
        logger.info("📊 RESUMO DA ATUALIZAÇÃO")
        logger.info("=" * 70)
        logger.info(f"   • Arquivos lidos: {self.stats['arquivos_lidos']}")
        logger.info(f"   • Tabelas geradas: {self.stats['tabelas_geradas']}")
        logger.info(f"   • Registros totais: {self.stats['registros_total']}")
        
        if self.stats['erros']:
            logger.warning(f"   • Erros: {len(self.stats['erros'])}")
            for erro in self.stats['erros']:
                logger.warning(f"     - {erro}")
        else:
            logger.info("   • Erros: 0")
        
        logger.info("")
        logger.info("=" * 70)
        logger.info("✅ TABELAS DE CONFIGURAÇÃO ATUALIZADAS!")
        logger.info("=" * 70)
        
        # SQL para criar tabelas
        self._print_sql_ddl()
    
    def _print_sql_ddl(self):
        """Gera DDL SQL para as novas tabelas."""
        ddl = """
-- ============================================
-- TABELAS DE CONFIGURAÇÃO - CALCULADORA
-- ============================================

-- Taxas Municipais (SC)
CREATE TABLE IF NOT EXISTS dim_taxas_municipais (
    id SERIAL PRIMARY KEY,
    cidade VARCHAR(100) NOT NULL,
    uf VARCHAR(2) NOT NULL,
    codigo_ibge VARCHAR(10),
    fator_regional DECIMAL(6,4),
    iss_aliquota DECIMAL(5,4),
    alvara_valor_base DECIMAL(10,2),
    alvara_valor_m2 DECIMAL(10,2),
    habite_se_valor_m2 DECIMAL(10,2),
    habite_se_valor_fixo DECIMAL(10,2),
    habite_se_minimo DECIMAL(10,2),
    habite_se_sanitario_m2 DECIMAL(10,2),
    habite_se_sanitario_vistoria DECIMAL(10,2),
    inss_aliquota DECIMAL(5,4),
    inss_base_percent DECIMAL(5,4),
    art_rrt_base DECIMAL(10,2),
    art_rrt_medio DECIMAL(10,2),
    itbi_aliquota DECIMAL(5,4),
    codigo_tributario_lei VARCHAR(200),
    codigo_obras_lei VARCHAR(200),
    plano_diretor_lei VARCHAR(200),
    fonte_url TEXT,
    observacoes TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Cenários de Construção
CREATE TABLE IF NOT EXISTS dim_cenarios_construcao (
    id SERIAL PRIMARY KEY,
    cenario_id VARCHAR(50) UNIQUE NOT NULL,
    nome VARCHAR(100) NOT NULL,
    descricao TEXT,
    multiplicador_custo DECIMAL(6,4) NOT NULL,
    multiplicador_prazo DECIMAL(6,4) NOT NULL,
    fator_fundacao DECIMAL(6,4),
    fator_estrutura DECIMAL(6,4),
    fator_terraplanagem DECIMAL(6,4),
    fator_contencao DECIMAL(6,4),
    fator_drenagem DECIMAL(6,4),
    nivel_risco VARCHAR(20),
    cor_risco VARCHAR(20),
    contingencia_percent DECIMAL(5,2),
    notas TEXT
);

-- Tipos de Contenção
CREATE TABLE IF NOT EXISTS dim_tipos_contencao (
    id SERIAL PRIMARY KEY,
    tipo_contencao VARCHAR(50) UNIQUE NOT NULL,
    nome VARCHAR(100) NOT NULL,
    custo_m2 DECIMAL(10,2),
    cenarios_aplicaveis TEXT,
    descricao TEXT
);

-- Profundidade de Subsolo
CREATE TABLE IF NOT EXISTS dim_profundidade_subsolo (
    id SERIAL PRIMARY KEY,
    profundidade_id VARCHAR(50) UNIQUE NOT NULL,
    nome VARCHAR(100) NOT NULL,
    faixa_metros_min DECIMAL(6,2),
    faixa_metros_max DECIMAL(6,2),
    multiplicador DECIMAL(6,4) NOT NULL
);

-- Métodos Construtivos Base
CREATE TABLE IF NOT EXISTS dim_metodos_construtivos_base (
    id SERIAL PRIMARY KEY,
    metodo_id VARCHAR(50) UNIQUE NOT NULL,
    nome VARCHAR(100) NOT NULL,
    fator_custo DECIMAL(6,4) NOT NULL,
    custo_base_ref DECIMAL(10,2),
    descricao TEXT
);

-- Topografia
CREATE TABLE IF NOT EXISTS dim_topografia (
    id SERIAL PRIMARY KEY,
    topografia_id VARCHAR(50) UNIQUE NOT NULL,
    nome VARCHAR(100) NOT NULL,
    fator_custo DECIMAL(6,4) NOT NULL
);

-- Fatores por Pavimento
CREATE TABLE IF NOT EXISTS dim_fatores_pavimento (
    id SERIAL PRIMARY KEY,
    pavimento_tipo VARCHAR(50) UNIQUE NOT NULL,
    nome VARCHAR(100) NOT NULL,
    fator_custo DECIMAL(6,4) NOT NULL
);

-- Padrão de Acabamento
CREATE TABLE IF NOT EXISTS dim_padrao_acabamento (
    id SERIAL PRIMARY KEY,
    padrao_id VARCHAR(50) UNIQUE NOT NULL,
    nome VARCHAR(100) NOT NULL,
    codigo_cub VARCHAR(10),
    descricao TEXT
);

-- Fatores Regionais por UF
CREATE TABLE IF NOT EXISTS dim_fatores_regionais_uf (
    id SERIAL PRIMARY KEY,
    uf VARCHAR(2) UNIQUE NOT NULL,
    nome_uf VARCHAR(100),
    fator_regional DECIMAL(6,4) NOT NULL,
    cub_r1b DECIMAL(10,2),
    cub_r1n DECIMAL(10,2),
    cub_r1a DECIMAL(10,2),
    inflacao_mensal DECIMAL(8,6),
    data_referencia DATE,
    fonte VARCHAR(100)
);

-- Taxas Cartoriais Estaduais
CREATE TABLE IF NOT EXISTS dim_taxas_cartoriais (
    id SERIAL PRIMARY KEY,
    uf VARCHAR(2) UNIQUE NOT NULL,
    itbi_aliquota DECIMAL(5,4),
    -- Faixas de averbação
    faixa_1_ate DECIMAL(15,2),
    faixa_1_emolumentos DECIMAL(10,2),
    faixa_1_taxa_judiciaria DECIMAL(10,2),
    faixa_2_ate DECIMAL(15,2),
    faixa_2_emolumentos DECIMAL(10,2),
    faixa_2_taxa_judiciaria DECIMAL(10,2),
    faixa_3_ate DECIMAL(15,2),
    faixa_3_emolumentos DECIMAL(10,2),
    faixa_3_taxa_judiciaria DECIMAL(10,2),
    faixa_4_emolumentos DECIMAL(10,2),
    faixa_4_taxa_base DECIMAL(10,2),
    faixa_4_pct_excesso DECIMAL(8,6),
    faixa_4_pct_excesso_taxa DECIMAL(8,6),
    -- Faixas de escritura
    escritura_faixa1_ate DECIMAL(15,2),
    escritura_faixa1_emolumentos DECIMAL(10,2),
    escritura_faixa2_ate DECIMAL(15,2),
    escritura_faixa2_base DECIMAL(10,2),
    escritura_faixa2_pct DECIMAL(8,6),
    escritura_faixa3_base DECIMAL(10,2),
    escritura_faixa3_pct DECIMAL(8,6),
    escritura_taxas_fixas DECIMAL(10,2),
    legislacao VARCHAR(200),
    data_atualizacao DATE
);

-- Séries BCB
CREATE TABLE IF NOT EXISTS dim_series_bcb (
    id SERIAL PRIMARY KEY,
    serie_id VARCHAR(50) UNIQUE NOT NULL,
    bcb_code VARCHAR(20),
    nome_completo VARCHAR(200),
    categoria VARCHAR(50),
    unidade VARCHAR(50),
    frequencia VARCHAR(20),
    relevancia_bi VARCHAR(20),
    uso TEXT,
    tipo_fonte VARCHAR(20),
    endpoint VARCHAR(100)
);

CREATE INDEX idx_taxas_municipais_cidade ON dim_taxas_municipais(cidade, uf);
CREATE INDEX idx_cenarios_id ON dim_cenarios_construcao(cenario_id);
CREATE INDEX idx_series_categoria ON dim_series_bcb(categoria);
"""
        logger.info("\n📝 SQL DDL PARA SUPABASE:")
        logger.info("-" * 70)
        print(ddl)


def main():
    """Função principal."""
    updater = ConfigTableUpdater()
    updater.run()


if __name__ == "__main__":
    main()
