# Dados Iniciais - Tabelas Dimensionais

## Fontes e Premissas

### 1. dim_series - Metadados das Séries BCB

**Fonte:** Banco Central do Brasil (SGS - Sistema Gerenciador de Séries Temporais)
**URL Base:** https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados

**Premissas:**
- Séries coletadas do arquivo `configs/maps_sgs.csv`
- Frequência mensal para todas as séries econômicas
- Status "ativa" para séries em uso contínuo
- Categorias baseadas em natureza dos indicadores

**Dados:**

| series_id | nome | categoria | fonte | freq | unidade | status |
|-----------|------|-----------|-------|------|---------|--------|
| BCB_SGS_432 | Taxa Selic | Juros | BCB_SGS | mensal | % a.a. | ativa |
| BCB_SGS_226 | Taxa Referencial (TR) | Juros | BCB_SGS | mensal | % a.m. | ativa |
| BCB_SGS_1 | Taxa de Câmbio USD/BRL | Câmbio | BCB_SGS | mensal | R$/USD | ativa |
| BCB_SGS_433 | IPCA | Inflação | BCB_SGS | mensal | índice | ativa |
| BCB_SGS_189 | IGP-M | Inflação | BCB_SGS | mensal | índice | ativa |
| BCB_SGS_7478 | Poupança | Juros | BCB_SGS | mensal | % a.a. | ativa |
| BCB_SGS_4189 | INPC | Inflação | BCB_SGS | mensal | índice | ativa |
| BCB_SGS_4390 | Crédito Pessoa Física | Crédito | BCB_SGS | mensal | R$ milhões | ativa |
| BCB_SGS_1207 | Produção Construção Civil | Produção | BCB_SGS | mensal | índice | ativa |
| BCB_SGS_24364 | Estoque Crédito Habitacional | Crédito | BCB_SGS | mensal | R$ milhões | ativa |

---

### 2. dim_topografia - Tipos de Terreno

**Fonte:** Práticas de mercado da construção civil brasileira
**Referências:** 
- TCPO (Tabela de Composições de Preços para Orçamentos)
- SINAPI (Sistema Nacional de Pesquisa de Custos e Índices da Construção Civil)
- Experiência de engenheiros civis e arquitetos

**Premissas:**
- Fator de custo: multiplicador sobre custo base (1.00 = referência)
- Fator de prazo: multiplicador sobre prazo base (1.00 = referência)
- Fatores baseados em acréscimos médios observados no mercado

**Dados:**

| id_topografia | desc_topografia | fator_custo | fator_prazo | requer_contencao |
|---------------|-----------------|-------------|-------------|------------------|
| TOPO_01 | Plano | 1.00 | 1.00 | FALSE |
| TOPO_02 | Aclive Leve (até 10%) | 1.08 | 1.05 | TRUE |
| TOPO_03 | Aclive Moderado (10-20%) | 1.15 | 1.10 | TRUE |
| TOPO_04 | Aclive Acentuado (>20%) | 1.25 | 1.15 | TRUE |
| TOPO_05 | Declive Leve (até 10%) | 1.10 | 1.08 | TRUE |
| TOPO_06 | Declive Moderado (10-20%) | 1.18 | 1.12 | TRUE |
| TOPO_07 | Declive Acentuado (>20%) | 1.28 | 1.18 | TRUE |
| TOPO_08 | Irregular/Ondulado | 1.20 | 1.15 | TRUE |
| TOPO_09 | Aterro/Várzea | 1.35 | 1.25 | TRUE |
| TOPO_10 | Rochoso | 1.40 | 1.20 | TRUE |

**Justificativas:**
- **Plano:** Base de referência sem acréscimos
- **Aclives/Declives:** Acréscimos por movimentação de terra, contenções, dificuldade de acesso
- **Irregular:** Múltiplas soluções de fundação e terraplanagem
- **Aterro:** Necessidade de consolidação do solo e fundações especiais
- **Rochoso:** Escavação mecânica complexa, explosivos em alguns casos

---

### 3. dim_metodo - Métodos Construtivos

**Fonte:** Mercado brasileiro de construção civil
**Referências:**
- CBIC (Câmara Brasileira da Indústria da Construção)
- ABNT (Associação Brasileira de Normas Técnicas)
- Catálogos de fabricantes (Steel Frame, Wood Frame)
- Estudos comparativos de construtoras

**Premissas:**
- Custos relativos ao método convencional (alvenaria)
- Prazos considerando mão de obra especializada disponível
- Limitações técnicas conforme normas ABNT

**Dados:**

| id_metodo | nome_metodo | fator_custo | fator_prazo | limitacao_pavimentos |
|-----------|-------------|-------------|-------------|----------------------|
| MET_01 | Alvenaria Convencional | 1.00 | 1.00 | até 4 pavimentos |
| MET_02 | Alvenaria Estrutural | 0.92 | 0.85 | até 15 pavimentos |
| MET_03 | Concreto Armado | 1.15 | 0.90 | sem limite |
| MET_04 | Concreto Protendido | 1.45 | 0.85 | sem limite |
| MET_05 | Steel Frame | 1.35 | 0.70 | até 6 pavimentos |
| MET_06 | Wood Frame | 1.20 | 0.75 | até 3 pavimentos |
| MET_07 | Pré-Moldado | 1.25 | 0.80 | até 12 pavimentos |
| MET_08 | Alvenaria + Estrutura Metálica | 1.30 | 0.88 | até 10 pavimentos |

**Justificativas:**
- **Alvenaria Convencional:** Base de referência, mais comum no Brasil
- **Alvenaria Estrutural:** Economia por eliminação de pilares/vigas, prazo menor por simplificação
- **Concreto Armado:** Custo maior por materiais e formas, mas maior flexibilidade
- **Concreto Protendido:** Tecnologia avançada, vãos maiores, menor prazo em grandes obras
- **Steel Frame:** Custo maior por importação de perfis, prazo muito reduzido (industrialização)
- **Wood Frame:** Emergente no Brasil, prazo reduzido, limitado por normas de segurança
- **Pré-Moldado:** Economia de escala em obras repetitivas, prazo reduzido
- **Alvenaria + Metálica:** Híbrido comum em galpões e edifícios comerciais

---

### 4. dim_projetos - Projetos Complementares

**Fonte:** Mercado brasileiro de arquitetura e engenharia
**Referências:**
- CAU (Conselho de Arquitetura e Urbanismo) - Tabela de honorários
- CREA (Conselho Regional de Engenharia e Agronomia) - Referencial de preços
- Sindicatos de arquitetos e engenheiros (SINAENCO)
- Código de Obras municipal (Florianópolis, São Paulo, etc.)

**Premissas:**
- Custos em R$/m² para obra padrão médio
- Obrigatórios: exigidos por lei (Código de Obras + Corpo de Bombeiros)
- Opcionais: agregam valor mas não são mandatórios
- Valores baseados em médias de mercado 2024

**Dados:**

| id_projeto | nome_projeto | custo_base_m2 | is_obrigatorio |
|------------|--------------|---------------|----------------|
| PROJ_01 | Arquitetônico | 25.00 | TRUE |
| PROJ_02 | Estrutural | 18.00 | TRUE |
| PROJ_03 | Hidrossanitário | 12.00 | TRUE |
| PROJ_04 | Elétrico | 15.00 | TRUE |
| PROJ_05 | Prevenção e Combate a Incêndio | 10.00 | TRUE |
| PROJ_06 | SPDA (Para-raios) | 8.00 | TRUE |
| PROJ_07 | Fundações | 20.00 | TRUE |
| PROJ_08 | Ar Condicionado e Ventilação | 14.00 | FALSE |
| PROJ_09 | Paisagismo | 12.00 | FALSE |
| PROJ_10 | Acústica | 15.00 | FALSE |
| PROJ_11 | Automação Predial | 18.00 | FALSE |
| PROJ_12 | Eficiência Energética | 16.00 | FALSE |

**Justificativas:**
- **Arquitetônico:** Maior custo por abrangência e complexidade criativa
- **Estrutural:** Responsabilidade técnica alta, cálculos complexos
- **Hidrossanitário:** Rede completa de água, esgoto e águas pluviais
- **Elétrico:** Dimensionamento, quadros, proteções, iluminação
- **Incêndio:** Obrigatório por lei (NBR 9077, Instruções Técnicas do Corpo de Bombeiros)
- **SPDA:** Exigido pela NBR 5419 para proteção contra descargas atmosféricas
- **Fundações:** Sondagem do solo + dimensionamento específico
- **Ar Condicionado:** Opcional, comum em edifícios comerciais/alto padrão
- **Paisagismo:** Agrega valor, não obrigatório (exceto em alguns condomínios de luxo)
- **Acústica:** Específico para teatros, estúdios, ou apartamentos alto padrão
- **Automação:** Tendência crescente, não obrigatório
- **Eficiência Energética:** Certificações (LEED, Procel), opcional mas valorizado

---

## Notas de Implementação

### Validações Implementadas
- IDs únicos para evitar duplicatas
- Fatores sempre > 0
- Custos em valores realistas (R$ 8-25/m²)
- Flags booleanos para campos lógicos

### Manutenção Futura
- **dim_series:** Adicionar novas séries conforme necessidade do pipeline
- **dim_topografia:** Considerar adicionar classificações regionais (ex: topografia de SC vs. SP)
- **dim_metodo:** Atualizar fatores conforme evolução de tecnologias (ex: industrialização)
- **dim_projetos:** Revisar custos anualmente (inflação + tabela sindical)

### Referências Adicionais
- TCPO: https://www.pini.com.br/produtos-e-servicos/livros-tecnicos/tcpo
- SINAPI: https://www.caixa.gov.br/poder-publico/modernizacao-gestao/sinapi
- CBIC: https://cbic.org.br/
- CAU: https://www.caubr.gov.br/
