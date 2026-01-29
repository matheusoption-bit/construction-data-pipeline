-- ═══════════════════════════════════════════════════════════════════════════════
-- 🏗️ CRIAÇÃO DE TABELAS SUPABASE - Construction Data Pipeline
-- ═══════════════════════════════════════════════════════════════════════════════
-- 
-- Este script cria todas as tabelas baseado na estrutura real dos CSVs.
-- Execute no SQL Editor do Supabase Dashboard.
--
-- Criado em: 2026-01-28
-- ═══════════════════════════════════════════════════════════════════════════════

-- ═══════════════════════════════════════════════════════════════════════════════
-- TABELAS DE FATOS (FACTS)
-- ═══════════════════════════════════════════════════════════════════════════════

-- 📊 FACT_CUB - Custo Unitário Básico consolidado
DROP TABLE IF EXISTS fact_cub CASCADE;
CREATE TABLE fact_cub (
    id INTEGER PRIMARY KEY,
    data_referencia DATE NOT NULL,
    uf VARCHAR(2) NOT NULL,
    tipo_cub VARCHAR(50) NOT NULL,
    regime_tributario VARCHAR(30) NOT NULL,
    valor_m2 DECIMAL(10, 2) NOT NULL,
    fonte VARCHAR(50),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_fact_cub_data ON fact_cub(data_referencia);
CREATE INDEX idx_fact_cub_uf ON fact_cub(uf);


-- 📊 FACT_MACROECONOMIA - Indicadores macroeconômicos
DROP TABLE IF EXISTS fact_macroeconomia CASCADE;
CREATE TABLE fact_macroeconomia (
    id INTEGER PRIMARY KEY,
    data_referencia DATE NOT NULL,
    indicador VARCHAR(100) NOT NULL,
    valor DECIMAL(18, 6),
    unidade VARCHAR(50),
    variacao_mes DECIMAL(10, 6),
    fonte VARCHAR(50),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_fact_macro_data ON fact_macroeconomia(data_referencia);
CREATE INDEX idx_fact_macro_indicador ON fact_macroeconomia(indicador);


-- ═══════════════════════════════════════════════════════════════════════════════
-- TABELAS DE DIMENSÃO (DIMENSIONS)
-- ═══════════════════════════════════════════════════════════════════════════════

-- 🏛️ DIM_TAXAS_MUNICIPAIS
DROP TABLE IF EXISTS dim_taxas_municipais CASCADE;
CREATE TABLE dim_taxas_municipais (
    id INTEGER PRIMARY KEY,
    cidade VARCHAR(100) NOT NULL,
    uf VARCHAR(2) NOT NULL,
    codigo_ibge INTEGER,
    fator_regional DECIMAL(6, 4),
    iss_aliquota DECIMAL(5, 4),
    alvara_valor_base DECIMAL(10, 2),
    alvara_valor_m2 DECIMAL(10, 2),
    habite_se_valor_m2 DECIMAL(10, 2),
    habite_se_valor_fixo DECIMAL(10, 2),
    habite_se_minimo DECIMAL(10, 2),
    habite_se_sanitario_m2 DECIMAL(10, 2),
    habite_se_sanitario_vistoria DECIMAL(10, 2),
    inss_aliquota DECIMAL(5, 4),
    inss_base_percent DECIMAL(5, 4),
    art_rrt_base DECIMAL(10, 2),
    art_rrt_medio DECIMAL(10, 2),
    itbi_aliquota DECIMAL(5, 4),
    codigo_tributario_lei VARCHAR(200),
    codigo_obras_lei VARCHAR(200),
    plano_diretor_lei VARCHAR(200),
    fonte_url TEXT,
    data_atualizacao DATE,
    observacoes TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);


-- 🔧 DIM_METODOS_CONSTRUTIVOS
DROP TABLE IF EXISTS dim_metodos_construtivos CASCADE;
CREATE TABLE dim_metodos_construtivos (
    id INTEGER PRIMARY KEY,
    codigo_metodo VARCHAR(50),
    nome VARCHAR(100) NOT NULL,
    uf VARCHAR(2),
    nome_uf VARCHAR(50),
    regiao VARCHAR(30),
    fator_custo DECIMAL(6, 4),
    fator_prazo DECIMAL(6, 4),
    pct_material DECIMAL(5, 4),
    pct_mao_obra DECIMAL(5, 4),
    status VARCHAR(20),
    data_atualizacao DATE,
    origem_fator VARCHAR(50),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_metodos_uf ON dim_metodos_construtivos(uf);


-- 🏔️ DIM_TOPOGRAFIA
DROP TABLE IF EXISTS dim_topografia CASCADE;
CREATE TABLE dim_topografia (
    id INTEGER PRIMARY KEY,
    topografia_id VARCHAR(50) NOT NULL,
    nome VARCHAR(100) NOT NULL,
    fator_custo DECIMAL(6, 4),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);


-- 📍 DIM_LOCALIDADE
DROP TABLE IF EXISTS dim_localidade CASCADE;
CREATE TABLE dim_localidade (
    id INTEGER PRIMARY KEY,
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

CREATE INDEX idx_localidade_uf ON dim_localidade(sigla);


-- 🏗️ DIM_CENARIOS_CONSTRUCAO
DROP TABLE IF EXISTS dim_cenarios_construcao CASCADE;
CREATE TABLE dim_cenarios_construcao (
    id INTEGER PRIMARY KEY,
    cenario_id VARCHAR(50),
    nome VARCHAR(100) NOT NULL,
    descricao TEXT,
    multiplicador_custo DECIMAL(6, 4),
    multiplicador_prazo DECIMAL(6, 4),
    fator_fundacao DECIMAL(6, 4),
    fator_estrutura DECIMAL(6, 4),
    fator_terraplanagem DECIMAL(6, 4),
    fator_contencao DECIMAL(6, 4),
    fator_drenagem DECIMAL(6, 4),
    nivel_risco VARCHAR(20),
    cor_risco VARCHAR(20),
    contingencia_percent DECIMAL(5, 4),
    notas TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);


-- 🧱 DIM_TIPOS_CONTENCAO
DROP TABLE IF EXISTS dim_tipos_contencao CASCADE;
CREATE TABLE dim_tipos_contencao (
    id INTEGER PRIMARY KEY,
    tipo_contencao VARCHAR(50),
    nome VARCHAR(100) NOT NULL,
    custo_m2 DECIMAL(10, 2),
    cenarios_aplicaveis TEXT,
    descricao TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);


-- ⬇️ DIM_PROFUNDIDADE_SUBSOLO
DROP TABLE IF EXISTS dim_profundidade_subsolo CASCADE;
CREATE TABLE dim_profundidade_subsolo (
    id INTEGER PRIMARY KEY,
    profundidade_id VARCHAR(50),
    nome VARCHAR(100) NOT NULL,
    faixa_metros_min DECIMAL(5, 2),
    faixa_metros_max DECIMAL(5, 2),
    multiplicador DECIMAL(6, 4),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);


-- 🔧 DIM_METODOS_CONSTRUTIVOS_BASE
DROP TABLE IF EXISTS dim_metodos_construtivos_base CASCADE;
CREATE TABLE dim_metodos_construtivos_base (
    id INTEGER PRIMARY KEY,
    metodo_id VARCHAR(50),
    nome VARCHAR(100) NOT NULL,
    fator_custo DECIMAL(6, 4),
    custo_base_ref DECIMAL(10, 2),
    descricao TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);


-- 🏢 DIM_FATORES_PAVIMENTO
DROP TABLE IF EXISTS dim_fatores_pavimento CASCADE;
CREATE TABLE dim_fatores_pavimento (
    id INTEGER PRIMARY KEY,
    pavimento_tipo VARCHAR(50),
    nome VARCHAR(100) NOT NULL,
    fator_custo DECIMAL(6, 4),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);


-- ✨ DIM_PADRAO_ACABAMENTO
DROP TABLE IF EXISTS dim_padrao_acabamento CASCADE;
CREATE TABLE dim_padrao_acabamento (
    id INTEGER PRIMARY KEY,
    padrao_id VARCHAR(50),
    nome VARCHAR(100) NOT NULL,
    codigo_cub VARCHAR(20),
    descricao TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);


-- 🗺️ DIM_FATORES_REGIONAIS_UF
DROP TABLE IF EXISTS dim_fatores_regionais_uf CASCADE;
CREATE TABLE dim_fatores_regionais_uf (
    uf VARCHAR(2) PRIMARY KEY,
    nome_uf VARCHAR(50),
    fator_regional DECIMAL(6, 4),
    cub_r1b DECIMAL(10, 2),
    cub_r1n DECIMAL(10, 2),
    cub_r1a DECIMAL(10, 2),
    inflacao_mensal DECIMAL(8, 6),
    data_referencia DATE,
    fonte VARCHAR(50),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);


-- 📜 DIM_TAXAS_CARTORIAIS
DROP TABLE IF EXISTS dim_taxas_cartoriais CASCADE;
CREATE TABLE dim_taxas_cartoriais (
    uf VARCHAR(2) PRIMARY KEY,
    itbi_aliquota DECIMAL(5, 4),
    faixa_1_ate DECIMAL(15, 2),
    faixa_1_emolumentos DECIMAL(10, 2),
    faixa_1_taxa_judiciaria DECIMAL(10, 2),
    faixa_2_ate DECIMAL(15, 2),
    faixa_2_emolumentos DECIMAL(10, 2),
    faixa_2_taxa_judiciaria DECIMAL(10, 2),
    faixa_3_ate DECIMAL(15, 2),
    faixa_3_emolumentos DECIMAL(10, 2),
    faixa_3_taxa_judiciaria DECIMAL(10, 2),
    faixa_4_emolumentos DECIMAL(10, 2),
    faixa_4_taxa_base DECIMAL(10, 2),
    faixa_4_pct_excesso DECIMAL(8, 6),
    faixa_4_pct_excesso_taxa DECIMAL(8, 6),
    escritura_faixa1_ate DECIMAL(15, 2),
    escritura_faixa1_emolumentos DECIMAL(10, 2),
    escritura_faixa2_ate DECIMAL(15, 2),
    escritura_faixa2_base DECIMAL(10, 2),
    escritura_faixa2_pct DECIMAL(8, 6),
    escritura_faixa3_base DECIMAL(10, 2),
    escritura_faixa3_pct DECIMAL(8, 6),
    escritura_taxas_fixas DECIMAL(10, 2),
    legislacao TEXT,
    data_atualizacao DATE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);


-- 📈 DIM_SERIES_BCB
DROP TABLE IF EXISTS dim_series_bcb CASCADE;
CREATE TABLE dim_series_bcb (
    id INTEGER PRIMARY KEY,
    serie_id VARCHAR(50),
    bcb_code INTEGER,
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
