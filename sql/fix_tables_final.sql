-- ═══════════════════════════════════════════════════════════════════════════════
-- 🏗️ CORREÇÃO FINAL DE TABELAS - Construction Data Pipeline
-- ═══════════════════════════════════════════════════════════════════════════════
-- Execute no SQL Editor do Supabase Dashboard
-- ═══════════════════════════════════════════════════════════════════════════════

-- 📍 DIM_LOCALIDADE - populacao é DOUBLE PRECISION (pode ser NaN/float)
DROP TABLE IF EXISTS dim_localidade CASCADE;
CREATE TABLE dim_localidade (
    id VARCHAR(20) PRIMARY KEY,
    tipo VARCHAR(20),
    codigo_ibge INTEGER,
    sigla VARCHAR(5),
    nome VARCHAR(100) NOT NULL,
    regiao VARCHAR(30),
    uf_pai VARCHAR(2),
    populacao DOUBLE PRECISION,  -- Era BIGINT, agora DOUBLE para aceitar floats
    latitude DECIMAL(10, 6),
    longitude DECIMAL(10, 6),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);


-- 🏛️ DIM_TAXAS_MUNICIPAIS - id é VARCHAR (nome da cidade)
DROP TABLE IF EXISTS dim_taxas_municipais CASCADE;
CREATE TABLE dim_taxas_municipais (
    id VARCHAR(100) PRIMARY KEY,  -- É o nome da cidade, não integer
    cidade VARCHAR(100) NOT NULL,
    uf VARCHAR(2) NOT NULL,
    codigo_ibge VARCHAR(20),
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


-- 🔧 DIM_METODOS_CONSTRUTIVOS - aumentar tamanho de status
DROP TABLE IF EXISTS dim_metodos_construtivos CASCADE;
CREATE TABLE dim_metodos_construtivos (
    id VARCHAR(50) PRIMARY KEY,
    codigo_metodo VARCHAR(50),
    nome VARCHAR(100) NOT NULL,
    uf VARCHAR(2),
    nome_uf VARCHAR(50),
    regiao VARCHAR(30),
    fator_custo DECIMAL(8, 4),
    fator_prazo DECIMAL(8, 4),
    pct_material DECIMAL(8, 6),
    pct_mao_obra DECIMAL(8, 6),
    status VARCHAR(50),  -- Aumentado de 20 para 50
    data_atualizacao DATE,
    origem_fator VARCHAR(50),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);


-- 📊 FACT_CUB - usar DOUBLE PRECISION para valores extremos
DROP TABLE IF EXISTS fact_cub CASCADE;
CREATE TABLE fact_cub (
    id INTEGER PRIMARY KEY,
    data_referencia DATE NOT NULL,
    uf VARCHAR(10) NOT NULL,
    tipo_cub VARCHAR(50) NOT NULL,
    regime_tributario VARCHAR(30) NOT NULL,
    valor_m2 DOUBLE PRECISION NOT NULL,  -- Mudou para DOUBLE PRECISION
    fonte VARCHAR(50),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_fact_cub_data ON fact_cub(data_referencia);
CREATE INDEX idx_fact_cub_uf ON fact_cub(uf);


-- ═══════════════════════════════════════════════════════════════════════════════
-- VERIFICAÇÃO
-- ═══════════════════════════════════════════════════════════════════════════════
SELECT table_name FROM information_schema.tables 
WHERE table_schema = 'public' AND table_name LIKE 'dim_%' OR table_name LIKE 'fact_%'
ORDER BY table_name;
