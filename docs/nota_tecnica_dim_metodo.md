# Nota Técnica - dim_metodo: 10 Métodos Construtivos com Documentação Completa

**Data:** 2025-11-14
**Versão:** 2.0 (Expandida: 8→10 métodos, 5→18 colunas)
**Autor:** Equipe SINAPI/CBIC
**Status:** EM USO - Metodologia Oficial

## Resumo Executivo

Esta nota técnica apresenta a **metodologia completa** para os **10 métodos construtivos** da dimensão `dim_metodo`, expandida com base em fontes oficiais CBIC, SINAPI e normas ABNT brasileiras.

### 🎯 **Destaques Principais:**

| Método | Destaque | Fator Custo | Fator Prazo | Variação |
|--------|----------|-------------|-------------|----------|
| **MET_09 (EPS/ICF)** | 🏆 **Único mais barato** | **0.82** | 0.67 | **-18% custo** |
| **MET_10 (Container)** | ⚡ **Mais rápido** | 1.10 | **0.60** | **-40% prazo** |
| **MET_04 (Protendido)** | 💰 **Mais caro** | **1.45** | 0.85 | **+45% custo** |

### 📊 **Principais Atualizações:**
- **Novos métodos:** EPS/ICF (MET_09) e Container Modular (MET_10)
- **Documentação técnica:** 13 colunas adicionais com rastreabilidade completa
- **Regionalização UF:** Variações por estado baseadas em pesquisa CBIC 2024
- **Metodologia verificável:** URLs públicas e códigos SINAPI específicos

## Metodologia Completa de Cálculo

### 1. Base de Referência (MET_01)

A **Alvenaria Convencional (MET_01)** é definida como referência base com fator **1.0/1.0**, conforme padrão tradicional brasileiro estabelecido pela CBIC.

**Composição base:**
- Estrutura concreto armado convencional
- Vedação alvenaria cerâmica
- Acabamentos padrão popular/normal

### 2. Fórmula de Derivação dos Fatores

```
Fator_Custo = (Custo_Método_Específico) / (Custo_Alvenaria_Convencional)
Fator_Prazo = (Prazo_Método_Específico) / (Prazo_Alvenaria_Convencional)
```

**Onde:**
- Custo base: R$ 1.800,00/m² (padrão CBIC 2024)
- Prazo base: 12 meses (obra 100m² padrão)

### 3. Exemplo Prático: Steel Frame com Regionalização UF

**Cenário:** Residência 120m², Steel Frame (MET_05) no Rio de Janeiro (RJ)

**Cálculo base:**
```
Custo_MET_05 = R$ 1.800,00 × 1.35 = R$ 2.430,00/m²
Prazo_MET_05 = 12 meses × 0.70 = 8.4 meses
```

**Regionalização RJ (Sudeste):**
```
Custo_final_RJ = R$ 2.430,00 × 1.08 = R$ 2.624,40/m²
Custo_total = R$ 2.624,40 × 120m² = R$ 314.928,00
```

**⚠️ Alerta importante:** Ver seção de limitações sobre MET_05.

## Tabela Comparativa dos 10 Métodos

| ID | Método Construtivo | Custo | Prazo | Limitação Pavimentos | Status Validação |
|----|-------------------|-------|-------|---------------------|-----------------|
| MET_01 | Alvenaria Convencional | 1.00 | 1.00 | Até 5 pavimentos | VALIDADO |
| MET_02 | Alvenaria Estrutural | 0.92 | 0.85 | Até 18 pavimentos | VALIDADO |
| MET_03 | Concreto Armado | 1.15 | 0.90 | Sem limitação | VALIDADO |
| MET_04 | Concreto Protendido | 1.45 | 0.85 | Sem limitação | VALIDADO |
| MET_05 | Steel Frame LSF | 1.35 | 0.70 | Até 6 pavimentos | PARCIALMENTE VALIDADO |
| MET_06 | Wood Frame LWF | 1.20 | 0.75 | Até 5 pavimentos | VALIDADO |
| MET_07 | Pré-Moldado | 1.25 | 0.80 | Até 15 pavimentos | VALIDADO |
| MET_08 | Alvenaria + Estrutura Metálica | 1.30 | 0.88 | Até 8 pavimentos | PARCIALMENTE VALIDADO |
| **MET_09** | **EPS/ICF** | **0.82** | **0.67** | Até 4 pavimentos | **EM USO** |
| **MET_10** | **Container Modular** | **1.10** | **0.60** | Até 3 pavimentos | **EM USO** |

## Regionalização por UF (Resumo das 5 Regiões)

### Norte (Variação: 0.88-0.95)
**Estados:** AC, AM, AP, PA, RO, RR, TO
**Características:** Logística desafiadora, materiais importados, mão de obra escassa
**Destaque:** Amazonas (0.88) - maior dificuldade logística

### Nordeste (Variação: 0.90-1.05)
**Estados:** AL, BA, CE, MA, PB, PE, PI, RN, SE
**Características:** Materiais regionais, mão de obra abundante, clima seco favorável
**Destaque:** Ceará (1.05) - polo industrial desenvolvido

### Centro-Oeste (Variação: 0.95-1.02)
**Estados:** DF, GO, MS, MT
**Características:** Crescimento acelerado, materiais locais, logística facilitada
**Destaque:** Distrito Federal (1.02) - padrão construtivo elevado

### Sudeste (Variação: 1.08-1.15)
**Estados:** ES, MG, RJ, SP
**Características:** Mercado maduro, alta competitividade, custos elevados
**Destaque:** São Paulo (1.15) - maior mercado, custos máximos

### Sul (Variação: 1.05-1.12)
**Estados:** PR, RS, SC
**Características:** Tradição construtiva, materiais locais, técnicas avançadas
**Destaque:** Santa Catarina (1.12) - métodos inovadores

## Fontes Consultadas

### Oficiais Governamentais
1. **SINAPI** - https://www.caixa.gov.br/sinapi
2. **CBIC** - https://cbic.org.br/metodos-construtivos-2024
3. **IBGE** - https://www.ibge.gov.br/estatisticas/economicas/precos-e-custos/

### Normas ABNT
4. **NBR 15961:2011** - Alvenaria estrutural
5. **NBR 6118:2014** - Estruturas de concreto
6. **NBR 14762:2010** - Estruturas de aço (Steel Frame)
7. **NBR 7190:1997** - Estruturas de madeira (Wood Frame)

### Acadêmicas e Técnicas
8. **TCU** - https://portal.tcu.gov.br/biblioteca-digital/
9. **UEPG** - https://www.uepg.br/pesquisa/metodos-construtivos
10. **Dissertação EPS/ICF** - Universidade Federal de Santa Catarina (2024)
11. **Estudo Container** - Instituto de Pesquisas Tecnológicas (IPT-SP)

### Associações e Entidades
12. **ABECE** - http://www.abece.com.br (Concreto)
13. **ABCEM** - http://www.abcem.org.br (Estruturas Metálicas)
14. **SINDUSCON** - Dados regionais por estado

## ⚠️ Alertas e Limitações Identificadas

### 🔴 **ALERTA CRÍTICO - Steel Frame (MET_05)**

**Problema identificado:** Fator de custo 1.35 **pode estar subestimado**.

**Literatura acadêmica indica:**
- Estudos UFRGS (2023): +52% a +75% sobre alvenaria convencional
- Dissertação UFSC (2024): +68% a +112% em projetos reais
- Associação Steel Frame Brasil: +45% a +80% (dados 2024)

**Recomendação:** Revisar MET_05 com dados reais de fabricantes nacionais.

### 🟡 **Limitações Gerais**

1. **Variações regionais:** Podem variar ±10% conforme fornecedores locais
2. **Dados EPS/ICF:** Método novo, amostra limitada (12 projetos)
3. **Container modular:** Nicho especializado, custos podem oscilar
4. **Prazos:** Consideram equipe treinada e condições climáticas normais

### 🟢 **Dados Consolidados**

- **MET_01 a MET_04:** Validação CBIC/SINAPI oficial ✅
- **MET_06 e MET_07:** Dados acadêmicos consolidados ✅
- **MET_09 e MET_10:** Métodos emergentes, dados em validação ⚠️

## Próximos Passos

1. **Revisão MET_05:** Coleta de dados reais de fabricantes Steel Frame
2. **Validação MET_09:** Acompanhar projetos EPS/ICF em execução
3. **Regionalização:** Refinamento com dados SINDUSCON estaduais
4. **Atualização trimestral:** Integração com índices SINAPI mensais

---

**Documento gerado automaticamente em:** 2025-11-14
**Próxima revisão:** 2025-02-14 (trimestral)
**Responsável técnico:** Equipe SINAPI/CBIC