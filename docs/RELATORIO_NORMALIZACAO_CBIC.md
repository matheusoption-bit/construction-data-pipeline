# 📊 RELATÓRIO DE NORMALIZAÇÃO - DADOS CBIC PARA BI

## 📅 Data: 2026-01-28

---

## 🎯 OBJETIVO

Transformar dados brutos da CBIC (Câmara Brasileira da Indústria da Construção), 
originalmente em formato visual de relatório, em datasets estruturados e limpos 
prontos para conexão com dashboards de BI (Power BI, Looker, Metabase, etc).

---

## 📈 RESULTADOS

### Estatísticas Gerais

| Métrica | Valor |
|---------|-------|
| **Abas Normalizadas** | 9 |
| **Total de Registros** | 1.535 |
| **Colunas Padronizadas** | 7-10 por tabela |
| **Fontes Processadas** | CBIC, IBGE, BCB |

### Detalhamento por Tabela Fact

| Aba | Registros | Colunas | Período |
|-----|-----------|---------|---------|
| `fact_tr_normalizado` | 419 | 10 | 1991-2025 |
| `fact_poupanca_normalizado` | 163 | 10 | 2012-2025 |
| `fact_pib_brasil_normalizado` | 23 | 7 | 2000-2023 |
| `fact_pib_construcao_normalizado` | 23 | 7 | 2000-2023 |
| `fact_inv_construcao_normalizado` | 23 | 7 | 2000-2023 |
| `fact_cimento_consumo_normalizado` | 396 | 8 | 2024 |
| `fact_cimento_producao_normalizado` | 336 | 8 | 2024 |
| `fact_cub_brasil_normalizado` | 91 | 8 | 2017-2024 |
| `fact_cub_desonerado_normalizado` | 61 | 8 | 2019-2024 |

---

## 🔧 TRANSFORMAÇÕES APLICADAS

### 1. **Limpeza de Cabeçalhos**
- Remoção de linhas com títulos de relatório
- Eliminação de headers "unnamed_X"
- Remoção de notas de rodapé e fontes

### 2. **Conversão de Formato Pivotado**
- Dados de cimento: meses em colunas → formato tabular (unpivot)
- CUB: anos x meses → registros individuais por data

### 3. **Padronização de Tipos**
- `data_referencia`: formato ISO (YYYY-MM-DD)
- `valor_*`: numéricos (ponto decimal)
- `variacao_*`: percentuais como float
- `mes_num`: inteiro 1-12

### 4. **Tratamento de Nulos**
- Valores `...`, `-`, `N/D` → NULL
- Strings vazias → NULL
- Propagação de ano em linhas vazias (carry forward)

### 5. **Eliminação de Ruído**
- Linhas com "Fonte:", "Elaboração:", "Nota:"
- Linhas >80% vazias
- Totais e subtotais duplicados

---

## 📊 SCHEMA DAS TABELAS

### Índices Mensais (TR, Poupança)
```
data_referencia  : DATE       -- Primeiro dia do mês
ano              : INT        -- Ano (1991-2025)
mes              : VARCHAR(3) -- Mês abreviado (JAN-DEZ)
mes_num          : INT        -- Mês numérico (1-12)
indice           : VARCHAR    -- Nome do índice
valor            : FLOAT      -- Valor do índice
variacao_mes     : FLOAT      -- Variação % mês
variacao_ano     : FLOAT      -- Variação % acumulada ano
variacao_12m     : FLOAT      -- Variação % 12 meses
fonte            : VARCHAR    -- Fonte dos dados
```

### Séries Anuais (PIB)
```
ano              : INT        -- Ano
data_referencia  : DATE       -- 01/01 do ano
serie            : VARCHAR    -- Nome da série
pib_corrente     : FLOAT      -- Valor preços correntes (R$ milhões)
pib_anterior     : FLOAT      -- Valor preços ano anterior
variacao_pct     : FLOAT      -- Variação em volume %
fonte            : VARCHAR    -- Fonte dos dados
```

### Cimento (Consumo/Produção)
```
data_referencia  : DATE       -- Primeiro dia do mês
ano              : INT        -- Ano
mes              : VARCHAR(3) -- Mês
mes_num          : INT        -- Mês numérico
localidade       : VARCHAR    -- UF ou região
tipo             : VARCHAR    -- CONSUMO ou PRODUCAO
valor_toneladas  : FLOAT      -- Volume em toneladas
fonte            : VARCHAR    -- CBIC/SNIC
```

### CUB (Brasil/Desonerado)
```
data_referencia  : DATE       -- Primeiro dia do mês
ano              : INT        -- Ano
mes              : VARCHAR(3) -- Mês
mes_num          : INT        -- Mês numérico
regiao           : VARCHAR    -- BRASIL ou região
valor_m2         : FLOAT      -- Custo R$/m²
tipo_cub         : VARCHAR    -- MEDIO ou DESONERADO
fonte            : VARCHAR    -- CBIC
```

---

## 🔗 CONEXÃO COM BI

### Google Sheets como Data Source
```
Spreadsheet ID: 11-KC18ShMKXZOSbWvHcLHJwz3oDjexGQLb26xm2Wq4w

Abas disponíveis:
- fact_tr_normalizado
- fact_poupanca_normalizado
- fact_pib_brasil_normalizado
- fact_pib_construcao_normalizado
- fact_inv_construcao_normalizado
- fact_cimento_consumo_normalizado
- fact_cimento_producao_normalizado
- fact_cub_brasil_normalizado
- fact_cub_desonerado_normalizado
```

### Exemplos de Queries SQL (BigQuery)

```sql
-- PIB Construção vs PIB Total
SELECT 
  a.ano,
  a.pib_corrente as pib_brasil,
  b.va_corrente as pib_construcao,
  ROUND(b.va_corrente / a.pib_corrente * 100, 2) as participacao_pct
FROM fact_pib_brasil_normalizado a
JOIN fact_pib_construcao_normalizado b ON a.ano = b.ano
ORDER BY a.ano;

-- Evolução CUB Mensal
SELECT 
  data_referencia,
  regiao,
  valor_m2,
  LAG(valor_m2) OVER (PARTITION BY regiao ORDER BY data_referencia) as valor_anterior,
  ROUND((valor_m2 / LAG(valor_m2) OVER (PARTITION BY regiao ORDER BY data_referencia) - 1) * 100, 2) as var_mensal
FROM fact_cub_brasil_normalizado
ORDER BY regiao, data_referencia;
```

---

## ⚙️ SCRIPTS UTILIZADOS

| Script | Função |
|--------|--------|
| `normalizar_dados_cbic_v2.py` | ETL principal - normalização completa |
| `implement_cbic_fase1.py` | Extração inicial de dados CBIC |
| `processar_desemprego.py` | Processamento PNAD específico |

---

## 📋 PRÓXIMOS PASSOS RECOMENDADOS

1. **Adicionar dimensão tempo** (`dim_tempo`) com feriados e dias úteis
2. **Criar views consolidadas** juntando facts relacionados
3. **Implementar refresh automático** via GitHub Actions
4. **Adicionar data quality checks** para monitorar anomalias
5. **Expandir cobertura regional** para CUB por UF

---

## 📞 SUPORTE

Documentação completa em: `docs/`
Configurações em: `configs/`
Logs de ingestão em: `_ingestion_log` (aba Google Sheets)

---

*Gerado automaticamente pelo pipeline de dados - construction-data-pipeline*
