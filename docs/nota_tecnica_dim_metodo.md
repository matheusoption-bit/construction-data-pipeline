# Nota Técnica - dim_metodo: 10 Métodos Construtivos
## Documentação Completa com Dados CBIC Validados

**Data de Criação:** 2025-11-14  
**Última Atualização:** 2025-11-14 15:33:39 UTC  
**Responsável:** matheusoption-bit  
**Projeto:** construction-data-pipeline  
**Repositório:** [matheusoption-bit/construction-data-pipeline](https://github.com/matheusoption-bit/construction-data-pipeline)

---

## 📊 Resumo Executivo

A aba **dim_metodo** foi reestruturada de **5 colunas** para **26 colunas**, incorporando:

- ✅ Dados CBIC reais (fact_cub_por_uf, fact_cub_detalhado)
- ✅ Rastreabilidade completa (fontes + validação)
- ✅ Composição de custos (material/mão_obra/admin)
- ✅ Aplicabilidade por segmento (residencial/comercial/industrial)
- ✅ Limitações técnicas e recomendações de uso

**Total:** 10 métodos × 27 colunas = 270 células de dados

---

## 🏆 Destaques

| Indicador | Método | Valor | Observação |
|-----------|--------|-------|-------------|
| 🥇 **Mais barato** | MET_09 (0.82) | -18% vs convencional | EPS/ICF com economia significativa |
| ⚡ **Mais rápido** | MET_10 (0.60) | -40% tempo | Container com montagem acelerada |
| 💰 **Mais caro** | MET_04 (1.45) | +45% vs convencional | Concreto protendido para grandes vãos |

---

## 📋 Tabela Comparativa (Resumida)

| id_metodo   | nome_metodo                    |   fator_custo_base |   fator_prazo_base | custo_inicial_m2_sudeste   | status_validacao      | fonte_primaria   |
|-------------|--------------------------------|--------------------|--------------------|----------------------------|-----------------------|------------------|
| MET_01      | Alvenaria Convencional         |               1    |               1    | R$ 1,847.32                | VALIDADO              | CBIC Estudo 2024 |
| MET_02      | Alvenaria Estrutural           |               0.92 |               0.85 | R$ 1,699.53                | VALIDADO              | CBIC Estudo 2024 |
| MET_03      | Concreto Armado                |               1.15 |               0.9  | R$ 2,124.42                | VALIDADO              | CBIC Estudo 2024 |
| MET_04      | Concreto Protendido            |               1.45 |               0.85 | R$ 2,678.61                | VALIDADO              | CBIC Estudo 2024 |
| MET_05      | Steel Frame                    |               1.35 |               0.7  | R$ 2,493.88                | VALIDADO              | CBIC Estudo 2024 |
| MET_06      | Wood Frame                     |               1.2  |               0.75 | R$ 2,216.78                | PARCIALMENTE_VALIDADO | CBIC Estudo 2024 |
| MET_07      | Pré-Moldado                    |               1.25 |               0.8  | R$ 2,309.15                | VALIDADO              | CBIC Estudo 2024 |
| MET_08      | Alvenaria + Estrutura Metálica |               1.3  |               0.88 | R$ 2,401.52                | PARCIALMENTE_VALIDADO | CBIC Estudo 2024 |
| MET_09      | EPS/ICF                        |               0.82 |               0.92 | R$ 1,514.40                | ESTIMADO              | CBIC Estudo 2024 |
| MET_10      | Container                      |               1.1  |               0.6  | R$ 2,031.05                | ESTIMADO              | CBIC Estudo 2024 |

> **Nota:** Tabela resumida com as colunas principais. A aba completa no Google Sheets contém todas as 26 colunas.

---

## 🔍 Metodologia de Cálculo

### 1. Custos Base (custo_inicial_m2_sudeste)
- **Fonte:** fact_cub_por_uf (UF=SP, período=2025-11)
- **Cálculo:** Filtro por tipo_cub_sinapi + período mais recente
- **Validação:** Cruzamento com dim_composicao_cub_medio
- **Regionalização:** Sudeste como referência, fatores por UF disponíveis

### 2. Composição de Custos (percentuais)
- **Fonte:** fact_cub_detalhado
- **Regra:** material + mão_obra + admin = 100%
- **Validação:** Diferença <5% vs dados CBIC
- **Atualização:** Trimestral com novos dados CUB

### 3. Fatores de Ajuste
- **fator_custo_base:** Multiplicador sobre custo convencional (MET_01 = 1.0)
- **fator_prazo_base:** Multiplicador sobre prazo convencional (MET_01 = 1.0)
- **Baseline:** Alvenaria Convencional como referência nacional

### 4. Classificação CUB SINAPI
- **Tipo 1:** Alvenaria convencional e sistemas similares
- **Tipo 2:** Concreto armado e estruturas pesadas
- **Tipo 3:** Estruturas metálicas e sistemas industrializados
- **Tipo 4:** Madeira e sistemas alternativos

---

## ⚠️ Limitações e Alertas

### MET_05 (Steel Frame)
- **🚨 Alerta:** Fator custo 1.35 pode estar **SUBESTIMADO**
- **Literatura:** Aponta variação de +52% a +112% em algumas regiões
- **Recomendação:** Revisar com dados reais de fabricantes (BlueSteel, Atex, Kingspan)
- **Limitação:** Mão de obra certificada concentrada no eixo Sul-Sudeste

### MET_09 (EPS/ICF)
- **Status:** Sistema **emergente** no Brasil
- **Limitação:** Baixa disponibilidade de mão de obra certificada
- **Aplicação:** Concentrada em DF, GO, SP
- **Potencial:** Maior economia detectada (-18% vs convencional)

### MET_10 (Container)
- **Status:** **Sem norma ABNT oficial**
- **Limitação:** Aplicação restrita a projetos específicos
- **Custo:** Varia +10% (usado) a +40% (novo)
- **Vantagem:** Execução mais rápida (-40% tempo)

### Dados CBIC
- **Período:** 2025-11 (mais recente disponível)
- **Cobertura:** fact_cub_por_uf (4.598 linhas), fact_cub_detalhado (18.059 linhas)
- **Inconsistências:** 0 warnings detectados e documentados

---

## 📚 Fontes Consultadas

- ABCP Alvenaria Estrutural
- ABNT NBR 15253
- ABNT NBR 6118
- ABNT NBR 7190
- ABNT NBR 8800
- ABNT NBR 9062
- Manual Técnico ICF
- Normas Marítimas ISO
- SINAPI CAIXA
- [ABNT - Associação Brasileira de Normas Técnicas](https://www.abnt.org.br)
- [CBIC - Câmara Brasileira da Indústria da Construção](https://cbic.org.br)
- [CBIC Estudo 2024](https://cbic.org.br/wp-content/uploads/2024/08/Estudo_Metodos_Construtivos_CBIC_2024.pdf)
- [CBIC Estudo 2024](https://cbic.org.tr/wp-content/uploads/2024/08/Estudo_Metodos_Construtivos_CBIC_2024.pdf)
- [IBGE - Instituto Brasileiro de Geografia e Estatística](https://sidra.ibge.gov.br)
- [SINAPI - Sistema Nacional de Pesquisa de Custos](https://www.caixa.gov.br/sinapi)

### Estudos Acadêmicos Consultados
- UFMG - Dissertações sobre métodos construtivos alternativos
- UNIPAC - Pesquisas em sistemas industrializados
- PUC-SP - Análises de custos Steel Frame
- UEPG - Estudos sobre EPS/ICF no Paraná

### Fabricantes e Institutos
- BlueSteel, Atex, Kingspan (Steel Frame)
- ABCP - Associação Brasileira de Cimento Portland
- IBÉ - Instituto Brasileiro de Executivos de Finanças

---

## 🎯 Próximos Passos

### 1. Expansão Regionalizada
- **Objetivo:** dim_metodo_regional (10 métodos × 27 UFs = 270 linhas)
- **Fonte:** fact_cub_por_uf com fatores regionais
- **Cronograma:** Q1 2026

### 2. Atualização Trimestral
- **Gatilho:** Quando sair novo CUB (a cada 3 meses)
- **Ações:** 
  - Recalcular custo_inicial_m2_sudeste
  - Revisar status_validacao
  - Atualizar data_atualizacao_cub

### 3. Revisão MET_05 (Steel Frame)
- **Objetivo:** Consultar fabricantes diretamente
- **Ação:** Recalibrar fator_custo para 1.50-2.10
- **Prazo:** Até dezembro 2025

### 4. Normalização MET_10 (Container)
- **Objetivo:** Acompanhar desenvolvimento de normas ABNT
- **Ação:** Revisar status quando norma for publicada

---

## 📈 Histórico de Versões

| Versão | Data | Alterações | Responsável |
|---------|------|-------------|-------------|
| 1.0 | 2025-11-14 | Criação inicial - expansão 5→ 26 colunas | matheusoption-bit |
| 0.5 | 2025-11-13 | Estrutura original - 5 colunas | matheusoption-bit |

---

## 📝 Metadados Técnicos

- **Script gerador:** `src/scripts/update_dim_metodo_complete.py`
- **Versão do script:** 1.0
- **Ambiente:** Python 3.13.7
- **Dependências:** pandas, gspread, structlog
- **Validação:** 10 linhas × 27 colunas
- **Status:** VÁLIDO

---

**Documento gerado automaticamente em 2025-11-14 15:33:39 UTC**  
**Para atualizações, execute:** `python src/scripts/update_dim_metodo_complete.py`
