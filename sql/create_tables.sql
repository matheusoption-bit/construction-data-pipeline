-- ═══════════════════════════════════════════════════════════════════════════════
-- 🏗️ CRIAÇÃO DE TABELAS SUPABASE - Construction Data Pipeline
-- ═══════════════════════════════════════════════════════════════════════════════
-- 
-- Este script cria todas as tabelas necessárias para o pipeline de dados.
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
    id SERIAL PRIMARY KEY,
    data_referencia DATE NOT NULL,
    uf VARCHAR(2) NOT NULL,
    tipo_cub VARCHAR(50) NOT NULL,
    regime_tributario VARCHAR(30) NOT NULL,
    valor_m2 DECIMAL(10, 2) NOT NULL,
    fonte VARCHAR(50),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(data_referencia, uf, tipo_cub, regime_tributario)
);

CREATE INDEX idx_fact_cub_data ON fact_cub(data_referencia);
CREATE INDEX idx_fact_cub_uf ON fact_cub(uf);
CREATE INDEX idx_fact_cub_tipo ON fact_cub(tipo_cub);

COMMENT ON TABLE fact_cub IS 'Custo Unitário Básico da construção civil por UF e tipo';


-- 📊 FACT_MACROECONOMIA - Indicadores macroeconômicos verticalizados
DROP TABLE IF EXISTS fact_macroeconomia CASCADE;
CREATE TABLE fact_macroeconomia (
    id SERIAL PRIMARY KEY,
    data_referencia DATE NOT NULL,
    indicador VARCHAR(100) NOT NULL,
    valor DECIMAL(18, 6),
    unidade VARCHAR(30),
    fonte VARCHAR(50),
    serie_bcb INTEGER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(data_referencia, indicador)
);

CREATE INDEX idx_fact_macro_data ON fact_macroeconomia(data_referencia);
CREATE INDEX idx_fact_macro_indicador ON fact_macroeconomia(indicador);

COMMENT ON TABLE fact_macroeconomia IS 'Série temporal de indicadores macroeconômicos do BCB';


-- ═══════════════════════════════════════════════════════════════════════════════
-- TABELAS DE DIMENSÃO (DIMENSIONS)
-- ═══════════════════════════════════════════════════════════════════════════════

-- 🏛️ DIM_TAXAS_MUNICIPAIS - Taxas ISS, ITBI, Alvarás por município
DROP TABLE IF EXISTS dim_taxas_municipais CASCADE;
CREATE TABLE dim_taxas_municipais (
    id SERIAL PRIMARY KEY,
    cidade VARCHAR(100) NOT NULL,
    uf VARCHAR(2) NOT NULL,
    iss_construcao DECIMAL(5, 4),
    itbi DECIMAL(5, 4),
    taxa_alvara DECIMAL(10, 2),
    taxa_habite_se DECIMAL(10, 2),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(cidade, uf)
);

COMMENT ON TABLE dim_taxas_municipais IS 'Taxas municipais de ISS, ITBI e alvarás por cidade';


-- 🔧 DIM_METODOS_CONSTRUTIVOS - Métodos construtivos com fatores regionais
DROP TABLE IF EXISTS dim_metodos_construtivos CASCADE;
CREATE TABLE dim_metodos_construtivos (
    id SERIAL PRIMARY KEY,
    metodo VARCHAR(100) NOT NULL,
    uf VARCHAR(2) NOT NULL,
    custo_base_m2 DECIMAL(10, 2),
    fator_regional DECIMAL(6, 4),
    custo_ajustado_m2 DECIMAL(10, 2),
    tempo_medio_meses INTEGER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(id)
);

CREATE INDEX idx_metodos_uf ON dim_metodos_construtivos(uf);
CREATE INDEX idx_metodos_metodo ON dim_metodos_construtivos(metodo);

COMMENT ON TABLE dim_metodos_construtivos IS 'Métodos construtivos com custos e fatores regionais por UF';


-- 🏔️ DIM_TOPOGRAFIA - Tipos de topografia
DROP TABLE IF EXISTS dim_topografia CASCADE;
CREATE TABLE dim_topografia (
    id SERIAL PRIMARY KEY,
    topografia_id VARCHAR(50) NOT NULL,
    nome VARCHAR(100) NOT NULL,
    descricao TEXT,
    fator_custo DECIMAL(5, 3),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(topografia_id)
);

COMMENT ON TABLE dim_topografia IS 'Tipos de topografia e seus fatores de custo';


-- 📍 DIM_LOCALIDADE - Localidades (UF, Região)
DROP TABLE IF EXISTS dim_localidade CASCADE;
CREATE TABLE dim_localidade (
    id SERIAL PRIMARY KEY,
    uf VARCHAR(2) NOT NULL,
    nome_uf VARCHAR(50) NOT NULL,
    regiao VARCHAR(30),
    ibge_codigo INTEGER,
    populacao BIGINT,
    pib_per_capita DECIMAL(12, 2),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(uf)
);

COMMENT ON TABLE dim_localidade IS 'Dimensão de localidade com dados demográficos';


-- 🏗️ DIM_CENARIOS_CONSTRUCAO - Cenários de construção
DROP TABLE IF EXISTS dim_cenarios_construcao CASCADE;
CREATE TABLE dim_cenarios_construcao (
    id SERIAL PRIMARY KEY,
    cenario VARCHAR(50) NOT NULL,
    descricao TEXT,
    fator_multiplicador DECIMAL(5, 3),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(cenario)
);

COMMENT ON TABLE dim_cenarios_construcao IS 'Cenários de construção (otimista, base, pessimista)';


-- 🧱 DIM_TIPOS_CONTENCAO - Tipos de contenção
DROP TABLE IF EXISTS dim_tipos_contencao CASCADE;
CREATE TABLE dim_tipos_contencao (
    id SERIAL PRIMARY KEY,
    tipo VARCHAR(100) NOT NULL,
    descricao TEXT,
    custo_base_m2 DECIMAL(10, 2),
    aplicacao TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(tipo)
);

COMMENT ON TABLE dim_tipos_contencao IS 'Tipos de contenção com custos e aplicações';


-- ⬇️ DIM_PROFUNDIDADE_SUBSOLO - Faixas de profundidade
DROP TABLE IF EXISTS dim_profundidade_subsolo CASCADE;
CREATE TABLE dim_profundidade_subsolo (
    id SERIAL PRIMARY KEY,
    faixa VARCHAR(50) NOT NULL,
    profundidade_min DECIMAL(5, 2),
    profundidade_max DECIMAL(5, 2),
    fator_custo DECIMAL(5, 3),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(faixa)
);

COMMENT ON TABLE dim_profundidade_subsolo IS 'Faixas de profundidade do subsolo e fatores de custo';


-- 🔧 DIM_METODOS_CONSTRUTIVOS_BASE - Métodos base (sem regionalização)
DROP TABLE IF EXISTS dim_metodos_construtivos_base CASCADE;
CREATE TABLE dim_metodos_construtivos_base (
    id SERIAL PRIMARY KEY,
    metodo VARCHAR(100) NOT NULL,
    custo_base_m2 DECIMAL(10, 2),
    tempo_medio_meses INTEGER,
    descricao TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(metodo)
);

COMMENT ON TABLE dim_metodos_construtivos_base IS 'Métodos construtivos base sem regionalização';


-- 🏢 DIM_FATORES_PAVIMENTO - Fatores por número de pavimentos
DROP TABLE IF EXISTS dim_fatores_pavimento CASCADE;
CREATE TABLE dim_fatores_pavimento (
    id SERIAL PRIMARY KEY,
    faixa_pavimentos VARCHAR(50) NOT NULL,
    pavimentos_min INTEGER,
    pavimentos_max INTEGER,
    fator DECIMAL(5, 3),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(faixa_pavimentos)
);

COMMENT ON TABLE dim_fatores_pavimento IS 'Fatores de custo por faixa de pavimentos';


-- ✨ DIM_PADRAO_ACABAMENTO - Padrões de acabamento
DROP TABLE IF EXISTS dim_padrao_acabamento CASCADE;
CREATE TABLE dim_padrao_acabamento (
    id SERIAL PRIMARY KEY,
    padrao VARCHAR(50) NOT NULL,
    descricao TEXT,
    fator DECIMAL(5, 3),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(padrao)
);

COMMENT ON TABLE dim_padrao_acabamento IS 'Padrões de acabamento e seus fatores de custo';


-- 🗺️ DIM_FATORES_REGIONAIS_UF - Fatores regionais por UF
DROP TABLE IF EXISTS dim_fatores_regionais_uf CASCADE;
CREATE TABLE dim_fatores_regionais_uf (
    id SERIAL PRIMARY KEY,
    uf VARCHAR(2) NOT NULL,
    fator_regional DECIMAL(6, 4),
    fonte VARCHAR(50),
    data_referencia DATE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(uf)
);

COMMENT ON TABLE dim_fatores_regionais_uf IS 'Fatores regionais consolidados por UF';


-- 📜 DIM_TAXAS_CARTORIAIS - Taxas de cartório
DROP TABLE IF EXISTS dim_taxas_cartoriais CASCADE;
CREATE TABLE dim_taxas_cartoriais (
    id SERIAL PRIMARY KEY,
    tipo VARCHAR(100) NOT NULL,
    valor_base DECIMAL(10, 2),
    percentual_valor DECIMAL(5, 4),
    uf VARCHAR(2),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(tipo, uf)
);

COMMENT ON TABLE dim_taxas_cartoriais IS 'Taxas cartoriais por tipo e UF';


-- 📈 DIM_SERIES_BCB - Metadados das séries do BCB
DROP TABLE IF EXISTS dim_series_bcb CASCADE;
CREATE TABLE dim_series_bcb (
    id SERIAL PRIMARY KEY,
    codigo_serie INTEGER NOT NULL,
    nome VARCHAR(200) NOT NULL,
    unidade VARCHAR(50),
    frequencia VARCHAR(20),
    fonte VARCHAR(100),
    descricao TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(codigo_serie)
);

COMMENT ON TABLE dim_series_bcb IS 'Metadados das séries temporais do Banco Central';


-- ═══════════════════════════════════════════════════════════════════════════════
-- TRIGGER PARA ATUALIZAÇÃO AUTOMÁTICA DO updated_at
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Aplicar trigger em todas as tabelas
DO $$
DECLARE
    t text;
BEGIN
    FOR t IN 
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_type = 'BASE TABLE'
        AND table_name IN (
            'fact_cub', 'fact_macroeconomia', 
            'dim_taxas_municipais', 'dim_metodos_construtivos', 
            'dim_topografia', 'dim_localidade', 
            'dim_cenarios_construcao', 'dim_tipos_contencao',
            'dim_profundidade_subsolo', 'dim_metodos_construtivos_base',
            'dim_fatores_pavimento', 'dim_padrao_acabamento',
            'dim_fatores_regionais_uf', 'dim_taxas_cartoriais',
            'dim_series_bcb'
        )
    LOOP
        EXECUTE format('
            DROP TRIGGER IF EXISTS update_%I_updated_at ON %I;
            CREATE TRIGGER update_%I_updated_at
            BEFORE UPDATE ON %I
            FOR EACH ROW
            EXECUTE FUNCTION update_updated_at_column();
        ', t, t, t, t);
    END LOOP;
END $$;


-- ═══════════════════════════════════════════════════════════════════════════════
-- POLÍTICAS RLS (Row Level Security) - Opcional
-- ═══════════════════════════════════════════════════════════════════════════════

-- Habilitar RLS em todas as tabelas (descomente se necessário)
-- ALTER TABLE fact_cub ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE fact_macroeconomia ENABLE ROW LEVEL SECURITY;
-- ... etc


-- ═══════════════════════════════════════════════════════════════════════════════
-- VERIFICAÇÃO FINAL
-- ═══════════════════════════════════════════════════════════════════════════════

SELECT 
    table_name,
    (SELECT count(*) FROM information_schema.columns WHERE table_name = t.table_name) as num_columns
FROM information_schema.tables t
WHERE table_schema = 'public' 
AND table_type = 'BASE TABLE'
AND table_name LIKE 'fact_%' OR table_name LIKE 'dim_%'
ORDER BY table_name;
