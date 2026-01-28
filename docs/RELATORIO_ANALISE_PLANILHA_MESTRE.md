# 📊 Relatório de Análise - Planilha-Mestre Centro de Inteligência CC

**Data da Análise:** 20 de Dezembro de 2025  
**Arquivo Analisado:** `docs/Planilha-Mestre - Centro de Inteligência CC.xlsx`  
**Versão do Relatório:** 1.0

---

## 📋 Sumário Executivo

| Métrica | Valor |
|---------|-------|
| **Total de Abas** | 54 |
| **Abas com Dados** | 31 (57,4%) |
| **Abas com Schema (sem dados)** | 10 (18,5%) |
| **Abas Completamente Vazias** | 13 (24,1%) |
| **Total de Registros** | 41.287 |
| **Total de Células** | 350.945 |
| **Células Preenchidas** | 288.591 |
| **Completude Geral** | 82,2% |

### Avaliação Geral de Qualidade

| Categoria | Status | Observação |
|-----------|--------|------------|
| **Estrutura** | ✅ Boa | Modelo dimensional bem definido (dim/fact) |
| **Completude** | ⚠️ Parcial | 23 abas sem dados (43% do total) |
| **Integridade Referencial** | ⚠️ Parcial | 6 UFs faltando em algumas tabelas fato |
| **Consistência** | ✅ Boa | Sem duplicatas nas tabelas principais |
| **Metadados** | ⚠️ Parcial | Campos de auditoria frequentemente vazios |

---

## 📁 Inventário Completo das Abas

### 1. Tabelas Dimensionais (dim_*)

| Aba | Registros | Colunas | Completude | Status |
|-----|-----------|---------|------------|--------|
| `dim_uf` | 27 | 14 | 71,4% | ✅ Populada |
| `dim_cidade` | 295 | 14 | 42,9% | ⚠️ Incompleta |
| `dim_tempo` | 131 | 8 | 100% | ✅ Completa |
| `dim_metodo_fase2` | 270 | 23 | 100% | ✅ Completa |
| `dim_series` | 10 | 12 | 75% | ⚠️ Parcial |
| `dim_topografia` | 10 | 15 | 65,3% | ⚠️ Parcial |
| `dim_projeto_obra` | 11 | 6 | 100% | ✅ Completa |
| `dim_tipo_cub` | 4 | 9 | 100% | ✅ Completa |
| `dim_cub_tipologia` | 19 | 10 | 95,3% | ✅ Boa |
| `dim_clima` | 0 | 13 | 0% | ❌ Vazia |
| `dim_bairro` | 0 | 12 | 0% | ❌ Vazia |
| `dim_geo` | 0 | 14 | 0% | ❌ Vazia |

### 2. Tabelas Fato (fact_*)

| Aba | Registros | Colunas | Completude | Período |
|-----|-----------|---------|------------|---------|
| `fact_cub_variacao` | 13.461 | 6 | 99,8% | 2007-03 a 2025-12 |
| `fact_series` | 9.433 | 8 | 33,5% | 2018-01 a 2025-11 |
| `fact_cub_por_uf` | 4.598 | 6 | 100% | 2007-02 a 2025-09 |
| `fact_cub_historico` | 118 | 9 | 100% | 2015-12 a 2025-09 |
| `fact_credito` | 0 | 7 | 0% | - |
| `fact_emprego` | 0 | 7 | 0% | - |
| `fact_materiais` | 0 | 7 | 0% | - |
| `fact_clima` | 0 | 7 | 0% | - |
| `fact_taxas_municipais` | 0 | 7 | 0% | - |

### 3. Indicadores Econômicos

| Aba | Registros | Período | Completude | Valores |
|-----|-----------|---------|------------|---------|
| `taxa_ref` | 2.924 | 2018-01 a 2025-11 | 88,5% | 0,0 a 0,24 |
| `taxa_selic` | 2.872 | 2018-01 a 2025-11 | 92,8% | 2,0% a 15,0% |
| `taxa_cambio` | 1.976 | 2018-01 a 2025-11 | 99,0% | R$ 3,14 a R$ 6,21 |
| `inpc_infla` | 95 | 2018-01 a 2025-11 | 91,9% | 1,9% a 14,9% |
| `credito_pf` | 95 | 2018-01 a 2025-11 | 91,9% | 0,13 a 1,28 |
| `igp_m_infla` | 94 | 2018-01 a 2025-10 | 91,9% | -1,93% a 4,34% |
| `ipca_infla` | 94 | 2018-01 a 2025-10 | 91,9% | -0,68% a 1,62% |
| `rend_poupanca` | 94 | 2018-01 a 2025-10 | 91,9% | -0,73% a 1,73% |
| `credito_habit` | 92 | 2018-01 a 2025-08 | 91,8% | 83,18 a 110,25 |

### 4. Tabelas CBIC e Fatores Regionais

| Aba | Registros | Colunas | Completude | Detalhes |
|-----|-----------|---------|------------|----------|
| `dim_metodo_fase2` | 270 | 23 | 100% | 27 UFs x 10 métodos |
| `meta_metodos_construtivos` | 270 | 26 | 88,5% | Metadados dos métodos |
| `comparacao_fatores` | 27 | 11 | 89,9% | Comparativo por UF |
| `fatores_empiricos` | 21 | 9 | 100% | Fatores baseados em dados reais |
| `city_params` | 5 | 12 | 100% | Apenas SC (Florianópolis região) |

### 5. Tabelas de Metadados e Controle

| Aba | Registros | Propósito | Status |
|-----|-----------|-----------|--------|
| `_data_sources` | 4 | Fontes de dados catalogadas | ✅ Funcional |
| `_update_schedule` | 4 | Agendamento de atualizações | ✅ Funcional |
| `_ingestion_log` | 48 | Log de ingestão de dados | ⚠️ Erros registrados |
| `_quality_flags` | 4.133 | Flags de qualidade | ✅ Funcional |
| `_map_sidra` | 0 | Mapeamento API SIDRA | ❌ Vazia |

### 6. Abas Completamente Vazias (Planejadas)

| Aba | Propósito Presumido |
|-----|---------------------|
| `cub_on_global` | CUB consolidado nacional |
| `cub_on_global_uf` | CUB consolidado por UF |
| `cub_des_global` | Desoneração CUB |
| `pib_brasil_serie` | Série histórica PIB Brasil |
| `pib_construcao_civil` | PIB do setor construção |
| `inv_construcao_civil` | Investimentos construção |
| `inv_infraestrutura` | Investimentos infraestrutura |
| `pib_part_construcao` | Participação construção no PIB |
| `ind_taxa_desemprego` | Indicador desemprego |
| `ind_taxa_selic` | Indicador SELIC |
| `ind_ipca_consumidor` | Indicador IPCA |
| `mat_cimento_producao` | Produção de cimento |
| `mat_cimento_consumo` | Consumo de cimento |

---

## 🔍 Análise Detalhada por Categoria

### A. Dados Geográficos

#### dim_uf (Unidades Federativas)
- **27 UFs cadastradas** (todas as unidades brasileiras)
- **Colunas completas:** id_uf, codigo_ibge_uf, sigla_uf, nome_uf, regiao, sigla_regiao, capital, populacao_2024, area_km2, is_ativo
- **Colunas vazias:** fonte_dados, fonte_url, created_at, updated_at (100% nulos)
- **Regiões cobertas:** Norte, Nordeste, Centro-Oeste, Sudeste, Sul

#### dim_cidade
- **295 cidades cadastradas**
- **Colunas com problemas:**
  - `id_cidade`: 100% nulo
  - `pib_per_capita`: 100% nulo
  - `renda_media`: 100% nulo
  - `indice_desenvolvimento_local`: 100% nulo
- **Campos de auditoria:** 100% vazios

#### city_params (Parâmetros Municipais)
- **Apenas 5 municípios** cadastrados (todos de Santa Catarina):
  - Florianópolis (fator_cidade: 1,18)
  - São José (fator_cidade: 1,12)
  - Palhoça (fator_cidade: 1,05)
  - Biguaçu (fator_cidade: 1,02)
  - Santo Amaro da Imperatriz (fator_cidade: 1,00)
- **Inclui:** ITBI, alvará, habite-se, ISS

### B. Dados de Custo (CUB)

#### fact_cub_variacao
- **13.461 registros** de variação do CUB
- **Período:** março/2007 a dezembro/2025
- **21 UFs com dados** (faltam: AC, AP, PI, RN, RR, TO)
- **Tipos de variação:**
  - Variacao_mensal_%
  - Variacao_12meses_%
  - Variacao_ano_%
- **147 valores nulos** nos registros mais recentes (out-dez/2025)

#### fact_cub_por_uf
- **4.598 registros** de CUB por UF
- **Período:** fevereiro/2007 a setembro/2025
- **Completude:** 100%

#### fact_cub_historico
- **118 registros** históricos consolidados
- **Período:** dezembro/2015 a setembro/2025
- **Completude:** 100%

### C. Métodos Construtivos

#### dim_metodo_fase2
- **270 registros** (27 UFs × 10 métodos construtivos)
- **10 métodos identificados:**
  1. Alvenaria Convencional
  2. Alvenaria Estrutural
  3. Concreto Armado
  4. Steel Frame
  5. Wood Frame
  6. Drywall
  7. Pré-moldados
  8. Industrializado
  9. Misto
  10. Especial
- **Fatores regionais de custo:** 0,85 a 1,25
- **Fatores regionais de prazo:** 0,90 a 1,20
- **Todas as 27 UFs cobertas**

#### fatores_empiricos
- **21 UFs com fatores empíricos** calculados
- **UFs sem dados empíricos:** AC, AP, PI, RN, RR, TO
- **Métricas incluídas:** valor_medio, desvio_padrao, coef_variacao

### D. Topografia

#### dim_topografia
- **10 classes de topografia:**
  | Código | Classe | Fator Custo | Fator Prazo |
  |--------|--------|-------------|-------------|
  | TOPO_01 | Plano | 1,00 | 1,00 |
  | TOPO_02 | Aclive Leve | 1,08 | 1,05 |
  | TOPO_03 | Aclive Moderado | 1,15 | 1,10 |
  | TOPO_04 | Aclive Acentuado | 1,25 | 1,15 |
  | TOPO_05 | Declive Leve | 1,10 | 1,08 |
  | TOPO_06 | Declive Moderado | 1,18 | 1,12 |
  | TOPO_07 | Declive Acentuado | 1,28 | 1,18 |
  | TOPO_08 | Irregular/Ondulado | 1,20 | 1,15 |
  | TOPO_09 | Aterro/Várzea | 1,35 | 1,25 |
  | TOPO_10 | Rochoso | 1,40 | 1,20 |

---

## ⚠️ Problemas de Qualidade Identificados

### 1. Problemas Críticos

| Problema | Localização | Impacto | Recomendação |
|----------|-------------|---------|--------------|
| **77% de valores nulos** | `fact_series.valor` | Alto | Investigar pipeline de ingestão |
| **6 UFs sem dados CUB** | `fact_cub_variacao` | Médio | Buscar fontes alternativas |
| **Erros de API BCB** | `_ingestion_log` | Alto | Corrigir endpoints/autenticação |

### 2. Campos de Auditoria Vazios

Múltiplas tabelas dimensionais possuem campos de metadados/auditoria completamente vazios:
- `fonte_dados`
- `fonte_url`
- `created_at`
- `updated_at`

**Tabelas afetadas:** dim_uf, dim_cidade, dim_series, dim_topografia, meta_metodos_construtivos

### 3. Erros no Log de Ingestão

O `_ingestion_log` registra **40 execuções com problemas**:
- **39 ocorrências:** "BCB_SGS_1207: Nenhum dado retornado pela API"
- **1 ocorrência:** Erro 403 (Forbidden) em múltiplas séries do BCB:
  - SGS 432, 226, 1, 433, 189, 7478, 4189, 4390, 1207, 24364

### 4. Flags de Qualidade

A aba `_quality_flags` contém **4.133 alertas**:
- **High (Severidade Alta):** 2.519 (61%)
- **Medium (Severidade Média):** 1.614 (39%)

### 5. UFs com Cobertura Incompleta

| UF | dim_metodo_fase2 | fact_cub_variacao | fatores_empiricos |
|----|------------------|-------------------|-------------------|
| AC | ✅ | ❌ | ❌ |
| AP | ✅ | ❌ | ❌ |
| PI | ✅ | ❌ | ❌ |
| RN | ✅ | ❌ | ❌ |
| RR | ✅ | ❌ | ❌ |
| TO | ✅ | ❌ | ❌ |

---

## 📊 O Que a Planilha Possui (Resumo)

### ✅ Dados Completos e Funcionais

1. **Estrutura Dimensional Sólida**
   - Modelo Star Schema bem definido
   - Separação clara entre dimensões e fatos

2. **Cobertura Geográfica**
   - Todas as 27 UFs brasileiras nas dimensões
   - 295 cidades catalogadas
   - 5 regiões do Brasil

3. **Dados CUB Históricos**
   - Série histórica de 2007 a 2025
   - 3 tipos de variação monitorados
   - 21 UFs com dados de variação

4. **Indicadores Econômicos**
   - Taxa SELIC (diária)
   - Taxa de câmbio USD/BRL
   - IPCA, INPC, IGP-M
   - Taxa referencial
   - Crédito habitacional
   - Rendimento poupança

5. **Metodologia Construtiva**
   - 10 métodos construtivos
   - Fatores de custo e prazo por UF
   - Composição de custos (material, mão de obra, admin)

6. **Tipologias CUB**
   - 19 tipologias cadastradas
   - Padrões construtivos definidos

7. **Sistema de Controle**
   - Log de ingestão
   - Flags de qualidade
   - Agendamento de atualizações

---

## 🚫 O Que Ainda Falta

### 1. Dados Geográficos Granulares
- [ ] `dim_clima` - Dados climáticos por cidade
- [ ] `dim_bairro` - Granularidade de bairros
- [ ] `dim_geo` - Coordenadas e características geotécnicas

### 2. Dados Macroeconômicos
- [ ] `pib_brasil_serie` - PIB histórico do Brasil
- [ ] `pib_construcao_civil` - PIB setorial
- [ ] `pib_part_construcao` - Participação no PIB
- [ ] `inv_construcao_civil` - Investimentos no setor
- [ ] `inv_infraestrutura` - Investimentos em infraestrutura

### 3. Indicadores de Mercado
- [ ] `ind_taxa_desemprego` - Taxa de desemprego
- [ ] `mat_cimento_producao` - Produção de cimento
- [ ] `mat_cimento_consumo` - Consumo de cimento
- [ ] `fact_materiais` - Preços de materiais
- [ ] `fact_emprego` - Dados de emprego no setor

### 4. Dados Financeiros Detalhados
- [ ] `fin_params_caixa` - Parâmetros de financiamento
- [ ] `fact_credito` - Operações de crédito detalhadas
- [ ] `fact_taxas_municipais` - Taxas e impostos municipais

### 5. Dados CUB Faltantes
- [ ] `cub_on_global` - CUB onerado consolidado
- [ ] `cub_on_global_uf` - CUB onerado por UF
- [ ] `cub_des_global` - CUB desonerado

### 6. Cobertura de UFs nos Fatos
- [ ] Dados de CUB para: AC, AP, PI, RN, RR, TO
- [ ] Fatores empíricos para as mesmas UFs

### 7. Parâmetros Municipais
- [ ] Expansão de `city_params` além de SC
- [ ] Cobertura de capitais estaduais
- [ ] Principais centros urbanos por região

### 8. Mapeamento SIDRA
- [ ] `_map_sidra` - Configuração da API IBGE/SIDRA

---

## 📈 Métricas de Qualidade por Aba

| Aba | Completude | Integridade | Atualidade | Score |
|-----|------------|-------------|------------|-------|
| dim_uf | 71% | Alta | N/A | ⭐⭐⭐ |
| dim_cidade | 43% | Média | N/A | ⭐⭐ |
| dim_tempo | 100% | Alta | Alta | ⭐⭐⭐⭐⭐ |
| dim_metodo_fase2 | 100% | Alta | Alta | ⭐⭐⭐⭐⭐ |
| dim_topografia | 65% | Alta | N/A | ⭐⭐⭐ |
| fact_cub_variacao | 99% | Alta | Alta | ⭐⭐⭐⭐⭐ |
| fact_cub_por_uf | 100% | Alta | Alta | ⭐⭐⭐⭐⭐ |
| fact_series | 34% | Baixa | Média | ⭐⭐ |
| taxa_selic | 93% | Alta | Alta | ⭐⭐⭐⭐ |
| taxa_cambio | 99% | Alta | Alta | ⭐⭐⭐⭐⭐ |
| ipca_infla | 92% | Alta | Média | ⭐⭐⭐⭐ |

---

## 🔧 Recomendações de Melhoria

### Prioridade Alta

1. **Resolver problemas de ingestão BCB**
   - Verificar autenticação e endpoints das APIs
   - Implementar retry com backoff exponencial
   - Adicionar monitoramento de falhas

2. **Preencher fact_series**
   - Investigar causa dos 77% de valores nulos
   - Validar mapeamento de séries
   - Implementar validação na ingestão

3. **Expandir cobertura de UFs**
   - Buscar fontes alternativas para AC, AP, PI, RN, RR, TO
   - Considerar uso de estimativas regionais

### Prioridade Média

4. **Preencher campos de auditoria**
   - Adicionar `created_at`/`updated_at` em todas as tabelas
   - Documentar fontes de dados
   - Registrar URLs de referência

5. **Expandir city_params**
   - Adicionar capitais estaduais
   - Incluir principais regiões metropolitanas

6. **Implementar abas planejadas**
   - Priorizar: pib_construcao_civil, fact_materiais, fact_emprego
   - Definir fontes de dados (IBGE, CAGED, PNAD)

### Prioridade Baixa

7. **Dados granulares**
   - dim_clima: integrar com INMET
   - dim_bairro: API Correios/IBGE
   - dim_geo: integrar com Google/OpenStreetMap

---

## 📊 Cobertura Temporal dos Dados

```
2007 ├─────────────────────────────────────────────────────────────┤ 2025
     │                                                             │
     │  fact_cub_variacao  ████████████████████████████████████████
     │  fact_cub_por_uf    ████████████████████████████████████████
     │                                                             │
     │  fact_cub_historico           ██████████████████████████████
     │                                                             │
     │  taxa_cambio                          ██████████████████████
     │  taxa_selic                           ██████████████████████
     │  ipca_infla                           ██████████████████████
     │  inpc_infla                           ██████████████████████
     │  igp_m_infla                          ██████████████████████
     │  credito_habit                        ██████████████████████
     │                                                             │
     └─────────────────────────────────────────────────────────────┘
```

---

## 📝 Conclusão

A **Planilha-Mestre do Centro de Inteligência CC** representa uma base de dados bem estruturada para análise do setor de construção civil no Brasil. Com **41.287 registros** distribuídos em **31 abas populadas**, ela oferece:

**Pontos Fortes:**
- Excelente cobertura de dados CUB (2007-2025)
- Modelo dimensional bem implementado
- Indicadores econômicos atualizados
- Fatores de custo regionalizados para todos os métodos construtivos

**Pontos de Atenção:**
- 23 abas ainda sem dados (43% do total)
- Problemas de ingestão nas APIs do BCB
- 6 UFs com dados incompletos
- 77% de valores nulos em fact_series

**Recomendação Geral:** A planilha está em **estágio intermediário de maturidade**. Os dados core (CUB, indicadores econômicos, métodos construtivos) estão sólidos, mas há oportunidades significativas de expansão e melhoria na qualidade de dados secundários.

---

*Relatório gerado automaticamente em 20/12/2025*
