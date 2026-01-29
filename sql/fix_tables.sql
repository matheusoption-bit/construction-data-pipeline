-- ═══════════════════════════════════════════════════════════════════════════════
-- 🏗️ CORREÇÃO DE TABELAS SUPABASE - Construction Data Pipeline
-- ═══════════════════════════════════════════════════════════════════════════════
-- 
-- Este script CORRIGE as tabelas com tipos incompatíveis.
-- Execute no SQL Editor do Supabase Dashboard DEPOIS de create_tables_v2.sql
--
-- Criado em: 2026-01-28
-- ═══════════════════════════════════════════════════════════════════════════════

-- Limpar dados existentes e recriar tabelas com tipos corretos

-- 📍 DIM_LOCALIDADE - id é VARCHAR, não INTEGER
DROP TABLE IF EXISTS dim_localidade CASCADE;
CREATE TABLE dim_localidade (
    id VARCHAR(20) PRIMARY KEY,  -- Ex: "UF_AC"
    tipo VARCHAR(20),
    codigo_ibge INTEGER,
    sigla VARCHAR(5),
    nome VARCHAR(100) NOT NULL,
    regiao VARCHAR(30),
    uf_pai VARCHAR(2),
    populacao BIGINT,
    latitude DECIMAL(10, 6),
    longitude DECIMAL(10, 6),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_localidade_sigla ON dim_localidade(sigla);


-- 🏗️ DIM_CENARIOS_CONSTRUCAO - contingencia_percent é INTEGER
DROP TABLE IF EXISTS dim_cenarios_construcao CASCADE;
CREATE TABLE dim_cenarios_construcao (
    id INTEGER PRIMARY KEY,
    cenario_id VARCHAR(50),
    nome VARCHAR(100) NOT NULL,
    descricao TEXT,
    multiplicador_custo DECIMAL(8, 4),  -- Aumentado precisão
    multiplicador_prazo DECIMAL(8, 4),
    fator_fundacao DECIMAL(8, 4),
    fator_estrutura DECIMAL(8, 4),
    fator_terraplanagem DECIMAL(8, 4),
    fator_contencao DECIMAL(8, 4),
    fator_drenagem DECIMAL(8, 4),
    nivel_risco VARCHAR(20),
    cor_risco VARCHAR(20),
    contingencia_percent INTEGER,  -- É inteiro (5, 10, 12, etc)
    notas TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);


-- 🏛️ DIM_TAXAS_MUNICIPAIS - codigo_ibge pode ser string/decimal
DROP TABLE IF EXISTS dim_taxas_municipais CASCADE;
CREATE TABLE dim_taxas_municipais (
    id INTEGER PRIMARY KEY,
    cidade VARCHAR(100) NOT NULL,
    uf VARCHAR(2) NOT NULL,
    codigo_ibge VARCHAR(20),  -- Mudou para VARCHAR
    fator_regional DECIMAL(8, 4),
    iss_aliquota DECIMAL(8, 6),
    alvara_valor_base DECIMAL(12, 2),
    alvara_valor_m2 DECIMAL(12, 4),
    habite_se_valor_m2 DECIMAL(12, 4),
    habite_se_valor_fixo DECIMAL(12, 2),
    habite_se_minimo DECIMAL(12, 2),
    habite_se_sanitario_m2 DECIMAL(12, 4),
    habite_se_sanitario_vistoria DECIMAL(12, 2),
    inss_aliquota DECIMAL(8, 6),
    inss_base_percent DECIMAL(8, 6),
    art_rrt_base DECIMAL(12, 2),
    art_rrt_medio DECIMAL(12, 2),
    itbi_aliquota DECIMAL(8, 6),
    codigo_tributario_lei VARCHAR(200),
    codigo_obras_lei VARCHAR(200),
    plano_diretor_lei VARCHAR(200),
    fonte_url TEXT,
    data_atualizacao DATE,
    observacoes TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);


-- 🔧 DIM_METODOS_CONSTRUTIVOS - id pode ser string
DROP TABLE IF EXISTS dim_metodos_construtivos CASCADE;
CREATE TABLE dim_metodos_construtivos (
    id VARCHAR(50) PRIMARY KEY,  -- Ex: "MET_01_AC_001"
    codigo_metodo VARCHAR(50),
    nome VARCHAR(100) NOT NULL,
    uf VARCHAR(2),
    nome_uf VARCHAR(50),
    regiao VARCHAR(30),
    fator_custo DECIMAL(8, 4),
    fator_prazo DECIMAL(8, 4),
    pct_material DECIMAL(8, 6),
    pct_mao_obra DECIMAL(8, 6),
    status VARCHAR(20),
    data_atualizacao DATE,
    origem_fator VARCHAR(50),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_metodos_uf ON dim_metodos_construtivos(uf);


-- 📈 DIM_SERIES_BCB - bcb_code pode ser decimal string
DROP TABLE IF EXISTS dim_series_bcb CASCADE;
CREATE TABLE dim_series_bcb (
    id INTEGER PRIMARY KEY,
    serie_id VARCHAR(50),
    bcb_code VARCHAR(20),  -- Mudou para VARCHAR (pode ser "1.0")
    nome_completo VARCHAR(200) NOT NULL,
    categoria VARCHAR(100),
    unidade VARCHAR(50),
    frequencia VARCHAR(20),
    relevancia_bi VARCHAR(20),
    uso TEXT,
    tipo_fonte VARCHAR(50),
    endpoint TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);


-- 📊 FACT_CUB - uf pode ser "BRASIL" (6 chars)
DROP TABLE IF EXISTS fact_cub CASCADE;
CREATE TABLE fact_cub (
    id INTEGER PRIMARY KEY,
    data_referencia DATE NOT NULL,
    uf VARCHAR(10) NOT NULL,  -- Aumentado para "BRASIL"
    tipo_cub VARCHAR(50) NOT NULL,
    regime_tributario VARCHAR(30) NOT NULL,
    valor_m2 DECIMAL(12, 2) NOT NULL,
    fonte VARCHAR(50),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_fact_cub_data ON fact_cub(data_referencia);
CREATE INDEX idx_fact_cub_uf ON fact_cub(uf);


-- 📊 FACT_MACROECONOMIA - valor pode ser muito grande (10^16)
DROP TABLE IF EXISTS fact_macroeconomia CASCADE;
CREATE TABLE fact_macroeconomia (
    id INTEGER PRIMARY KEY,
    data_referencia DATE NOT NULL,
    indicador VARCHAR(100) NOT NULL,
    valor DOUBLE PRECISION,  -- Mudou para DOUBLE PRECISION (suporta grandes valores)
    unidade VARCHAR(50),
    variacao_mes DOUBLE PRECISION,
    fonte VARCHAR(50),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_fact_macro_data ON fact_macroeconomia(data_referencia);
CREATE INDEX idx_fact_macro_indicador ON fact_macroeconomia(indicador);


-- ═══════════════════════════════════════════════════════════════════════════════
-- VERIFICAÇÃO FINAL
-- ═══════════════════════════════════════════════════════════════════════════════

SELECT 
    table_name,
    (SELECT count(*) FROM information_schema.columns c WHERE c.table_name = t.table_name AND c.table_schema = 'public') as num_columns
FROM information_schema.tables t
WHERE table_schema = 'public' 
AND table_type = 'BASE TABLE'
AND (table_name LIKE 'fact_%' OR table_name LIKE 'dim_%')
ORDER BY table_name;
