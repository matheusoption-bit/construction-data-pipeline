# construction-data-pipeline

[![Daily BCB Data Ingestion](https://github.com/matheusoption-bit/construction-data-pipeline/actions/workflows/daily-ingestion.yml/badge.svg)](https://github.com/matheusoption-bit/construction-data-pipeline/actions/workflows/daily-ingestion.yml)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Centro de Inteligência da Construção Civil - Pipeline de Dados

Pipeline automatizado de ingestão e análise de séries temporais econômicas do Banco Central do Brasil (BCB) com foco em indicadores relevantes para o setor da construção civil.

## 📊 Séries Coletadas

- **Selic** (432) - Taxa básica de juros
- **TR** (226) - Taxa Referencial
- **USD/BRL** (1) - Taxa de câmbio
- **IPCA** (433) - Inflação (IBGE)
- **IGP-M** (189) - Inflação (FGV)
- **Poupança** (7478) - Rendimento da poupança
- **INPC** (4189) - Inflação (IBGE)
- **Crédito PF** (4390) - Volume de crédito pessoa física
- **Produção Construção** (1207) - Índice de produção da construção
- **Estoque Crédito Habitacional** (24364) - Volume de crédito imobiliário

## 🚀 Execução Local

### 1. Instalar Dependências

```bash
pip install -r requirements.txt
```

### 2. Configurar Credenciais Google

a) Criar projeto no [Google Cloud Console](https://console.cloud.google.com/)

b) Habilitar APIs:
   - Google Sheets API
   - Google Drive API

c) Criar Service Account e baixar `credentials.json`

d) Colocar `credentials.json` na raiz do projeto

e) Compartilhar sua planilha Google Sheets com o email do Service Account

### 3. Configurar Variáveis de Ambiente

```bash
# Copiar arquivo de exemplo
cp .env.example .env

# Editar .env e configurar:
# - GOOGLE_SPREADSHEET_ID (ID da sua planilha)
# - GOOGLE_CREDENTIALS_PATH=credentials.json
```

### 4. Executar Job de Ingestão

```bash
python -m src.jobs.daily_bcb
```

O job irá:
- Buscar dados das 10 séries do BCB (últimos 12 meses)
- Calcular variações MoM (mês a mês) e YoY (ano a ano)
- Executar validações de qualidade (outliers, variações anormais)
- Escrever dados em `fact_series` no Google Sheets
- Registrar flags de qualidade em `_quality_flags`
- Registrar log de execução em `_ingestion_log`

## 📁 Estrutura do Projeto

```
construction-data-pipeline/
├── src/
│   ├── clients/          # Clientes de APIs externas
│   │   └── bcb.py       # Cliente Banco Central
│   ├── etl/             # ETL e processamento
│   │   ├── sheets.py    # Loader Google Sheets
│   │   └── quality.py   # Validações de qualidade
│   ├── jobs/            # Jobs de ingestão
│   │   └── daily_bcb.py # Job diário BCB
│   └── utils/           # Utilitários
│       ├── config.py    # Configurações
│       └── logger.py    # Logging estruturado
├── .github/workflows/   # GitHub Actions
│   └── daily-ingestion.yml
├── configs/             # Configurações
│   └── maps_sgs.csv    # Mapeamento de séries SGS
├── tests/               # Testes automatizados
│   └── test_clients.py
├── requirements.txt     # Dependências Python
├── .env.example        # Exemplo de variáveis de ambiente
├── .gitignore          # Arquivos ignorados pelo Git
└── pyproject.toml      # Configuração do projeto
```

## 🧪 Executar Testes

```bash
# Executar todos os testes
pytest tests/ -v

# Executar com cobertura
pytest tests/ --cov=src --cov-report=html

# Executar testes específicos
pytest tests/test_clients.py -v
```

## 🤖 Automação (GitHub Actions)

O workflow `.github/workflows/daily-ingestion.yml` executa automaticamente:

- **Agendamento**: Todos os dias às 06:05 BRT (09:05 UTC)
- **Execução manual**: Via GitHub Actions UI
- **Timeout**: 30 minutos
- **Notificações**: Comentário na issue #1 em caso de falha

### Secrets Necessários

Configure no repositório GitHub (Settings > Secrets):

- `GOOGLE_CREDENTIALS` - Conteúdo do credentials.json em base64
- `GOOGLE_SPREADSHEET_ID` - ID da planilha Google Sheets

Para gerar o secret em base64:
```bash
# Linux/Mac
base64 -w 0 credentials.json

# Windows (PowerShell)
[Convert]::ToBase64String([IO.File]::ReadAllBytes("credentials.json"))
```

## 📊 Estrutura das Abas no Google Sheets

### `fact_series`
Tabela de fatos com séries temporais:
- `id_fato` - Chave primária (series_id + data)
- `series_id` - Identificador da série
- `data_referencia` - Data no formato YYYY-MM-DD
- `valor` - Valor da série
- `variacao_mom` - Variação mês sobre mês (%)
- `variacao_yoy` - Variação ano sobre ano (%)
- `fonte_original` - Fonte (bcb_sgs)
- `created_at` - Timestamp de criação

### `_quality_flags`
Flags de qualidade detectados:
- `series_id` - Identificador da série
- `data_referencia` - Data do problema
- `tipo_flag` - Tipo (outlier_zscore, high_mom_variation, negative_value)
- `severidade` - Severidade (high, medium, low)
- `valor_observado` - Valor que gerou o flag
- `desvio_padrao` - Desvio padrão da série
- `detalhes` - Informações adicionais

### `_ingestion_log`
Log de execuções:
- `exec_id` - ID único da execução
- `timestamp` - Data/hora da execução
- `fonte` - Fonte dos dados (bcb_daily)
- `status` - Status (success, partial, error)
- `linhas_processadas` - Total de linhas processadas
- `erros` - Mensagens de erro (se houver)

## 🔧 Tecnologias

- **Python 3.11+**
- **pandas** - Manipulação de dados
- **gspread** + **oauth2client** - Integração Google Sheets
- **requests** - Cliente HTTP
- **structlog** - Logging estruturado em JSON
- **scipy** - Cálculos estatísticos (z-score)
- **pytest** - Testes automatizados

## 📝 Logs

Os logs são estruturados em JSON para fácil parsing e análise:

```json
{
  "event": "series_processed_successfully",
  "series_id": "BCB_SGS_433",
  "rows_written": 12,
  "quality_flags": 0,
  "timestamp": "2025-11-07T09:15:32.123456Z",
  "level": "info"
}
```

## 🤝 Contribuindo

1. Fork o projeto
2. Crie uma branch para sua feature (`git checkout -b feature/AmazingFeature`)
3. Commit suas mudanças (`git commit -m 'Add some AmazingFeature'`)
4. Push para a branch (`git push origin feature/AmazingFeature`)
5. Abra um Pull Request

## 📄 Licença

Este projeto está sob a licença MIT. Veja o arquivo `LICENSE` para mais detalhes.

## 👥 Autores

- **Matheus** - [matheusoption-bit](https://github.com/matheusoption-bit)

## 🔗 Links Úteis

- [API BCB - Documentação](https://dadosabertos.bcb.gov.br/)
- [Google Sheets API](https://developers.google.com/sheets/api)
- [Sistema Gerenciador de Séries Temporais (SGS)](https://www3.bcb.gov.br/sgspub/)
