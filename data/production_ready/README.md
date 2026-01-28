# 📁 Dados de Produção - Prontos para Supabase

Diretório contendo os arquivos CSV consolidados e limpos, prontos para upload no PostgreSQL/Supabase.

**Gerado em:** 2026-01-28  
**Script:** `src/scripts/prepare_for_supabase.py`

## 📊 Arquivos Disponíveis

| Arquivo | Registros | Descrição |
|---------|-----------|-----------|
| `fact_cub.csv` | 4.750 | Custo Unitário Básico unificado (todos UFs + Brasil) |
| `fact_macroeconomia.csv` | 5.899 | Indicadores econômicos verticalizados |
| `dim_metodos_construtivos.csv` | 270 | Métodos construtivos com fatores regionais |
| `dim_taxas_municipais.csv` | 5 | Taxas ISS, ITBI, alvarás por município |
| `dim_localidade.csv` | 322 | UFs e cidades do Brasil |

---

## 🗄️ Estrutura das Tabelas

### fact_cub
Dados de Custo Unitário Básico consolidados de múltiplas fontes CBIC.

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `id` | SERIAL | Chave primária |
| `data_referencia` | DATE | Mês de referência (YYYY-MM-DD) |
| `uf` | VARCHAR(2) | UF ou 'BR' para nacional |
| `tipo_cub` | VARCHAR(20) | Tipo (R8-N, R16-N, MEDIO, etc.) |
| `regime_tributario` | VARCHAR(20) | ONEROSO ou DESONERADO |
| `valor_m2` | DECIMAL(10,2) | Custo por m² em R$ |
| `fonte` | VARCHAR(50) | Origem dos dados |

### fact_macroeconomia
Tabela vertical (narrow table) com todos indicadores econômicos.

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `id` | SERIAL | Chave primária |
| `data_referencia` | DATE | Data de referência |
| `indicador` | VARCHAR(50) | Nome do indicador |
| `valor` | DECIMAL(15,6) | Valor do indicador |
| `unidade` | VARCHAR(20) | Unidade (% a.a., R$, etc.) |
| `variacao_mes` | DECIMAL(10,6) | Variação mensal |
| `fonte` | VARCHAR(50) | Origem (BCB, IBGE, etc.) |

**Indicadores disponíveis:**
- SELIC, IPCA, IGP-M, INPC, TR
- POUPANCA, RENDIMENTO_POUPANCA
- USD_BRL, CREDITO_HABITACIONAL

### dim_metodos_construtivos
**⚠️ DADOS PROPRIETÁRIOS** - Fatores regionais por método construtivo e UF.

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `id` | VARCHAR(50) | Chave (METODO_UF) |
| `codigo_metodo` | VARCHAR(20) | Código do método |
| `nome` | VARCHAR(100) | Nome do método construtivo |
| `uf` | VARCHAR(2) | UF |
| `fator_custo` | DECIMAL(6,4) | Fator regional de custo |
| `fator_prazo` | DECIMAL(6,4) | Fator regional de prazo |
| `pct_material` | DECIMAL(5,4) | % de material na composição |
| `pct_mao_obra` | DECIMAL(5,4) | % de mão de obra |

### dim_taxas_municipais
**⚠️ DADOS PROPRIETÁRIOS** - Pesquisa de legislação municipal.

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `id` | SERIAL | Chave primária |
| `cidade` | VARCHAR(100) | Nome do município |
| `uf` | VARCHAR(2) | UF |
| `itbi_aliquota` | DECIMAL(5,2) | Alíquota ITBI % |
| `iss_construcao_aliquota` | DECIMAL(5,2) | Alíquota ISS construção % |
| `iss_deducao_materiais` | DECIMAL(5,2) | Dedução de materiais % |
| `codigo_tributario_lei` | VARCHAR(100) | Lei do código tributário |
| `fonte_url` | TEXT | URL da fonte |

### dim_localidade
Tabela unificada de UFs e cidades.

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `id` | VARCHAR(20) | Chave (UF_XX ou CID_IBGE) |
| `tipo` | VARCHAR(10) | 'UF' ou 'CIDADE' |
| `codigo_ibge` | VARCHAR(10) | Código IBGE |
| `nome` | VARCHAR(100) | Nome da localidade |
| `uf_pai` | VARCHAR(20) | FK para UF (cidades) |

---

## 🚀 Upload para Supabase

### 1. Criar as tabelas

Execute o SQL gerado pelo script `prepare_for_supabase.py` no console SQL do Supabase.

### 2. Importar via CLI

```bash
# Usando supabase CLI
supabase db import fact_cub.csv --table fact_cub
supabase db import fact_macroeconomia.csv --table fact_macroeconomia
supabase db import dim_metodos_construtivos.csv --table dim_metodos_construtivos
supabase db import dim_taxas_municipais.csv --table dim_taxas_municipais
supabase db import dim_localidade.csv --table dim_localidade
```

### 3. Ou via Python

```python
from supabase import create_client
import pandas as pd

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

df = pd.read_csv('fact_cub.csv')
supabase.table('fact_cub').insert(df.to_dict('records')).execute()
```

---

## 📈 Estatísticas

- **Total de registros:** 11.246
- **Período de dados:** 2007-2026
- **Cobertura geográfica:** 27 UFs + 295 cidades
- **Indicadores macroeconômicos:** 9
