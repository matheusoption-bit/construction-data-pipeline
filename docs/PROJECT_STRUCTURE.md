# Construction Data Pipeline - Estrutura do Projeto
# PARTE 8: Documentação da Estrutura

## 📂 Estrutura do Projeto

```
construction-data-pipeline/
├── src/
│   └── scripts/
│       └── update_dim_metodo_complete.py    # Script principal (2.462 linhas)
├── configs/
│   ├── dim_metodo_v2.csv                    # Dados dos 10 métodos (26 colunas)
│   └── google_sheets_credentials.json       # Credenciais Google Sheets (criar)
├── docs/
│   ├── nota_tecnica_dim_metodo.md          # Documentação técnica (5.441 bytes)
│   └── PROJECT_STRUCTURE.md               # Este arquivo
├── .env.template                          # Template de variáveis de ambiente
├── .env                                   # Variáveis de ambiente (criar)
├── requirements.txt                       # Dependências Python
└── README.md                             # Documentação principal
```

## 🚀 Instalação e Configuração

### 1. Instalação das Dependências
```bash
pip install -r requirements.txt
```

### 2. Configuração do Ambiente
```bash
# Copiar template das variáveis de ambiente
cp .env.template .env

# Editar .env com suas configurações
# - GOOGLE_SHEETS_CREDENTIALS_PATH
# - GOOGLE_SHEETS_SPREADSHEET_ID  
# - GOOGLE_SHEETS_WORKSHEET_NAME
```

### 3. Configuração Google Sheets API
1. Acesse [Google Cloud Console](https://console.cloud.google.com/)
2. Crie um projeto ou selecione existente
3. Ative a Google Sheets API
4. Crie credenciais (Service Account)
5. Baixe o arquivo JSON para `configs/google_sheets_credentials.json`
6. Compartilhe a planilha com o email do Service Account

## 💻 Uso do Sistema

### Comando Básico
```bash
python src/scripts/update_dim_metodo_complete.py
```

### Comandos Avançados
```bash
# Simulação (não altera dados reais)
python src/scripts/update_dim_metodo_complete.py --dry-run

# Modo verboso (logs detalhados)
python src/scripts/update_dim_metodo_complete.py --verbose

# Pular enriquecimento CBIC
python src/scripts/update_dim_metodo_complete.py --skip-cbic

# Combinação de argumentos
python src/scripts/update_dim_metodo_complete.py --dry-run --verbose --skip-cbic
```

## 🏗️ Arquitetura do Sistema

O sistema é dividido em **7 PARTES principais**:

1. **PARTE 1**: Estrutura de 26 colunas para métodos construtivos
2. **PARTE 2**: Validação rigorosa do CSV (12 verificações)
3. **PARTE 3**: Enriquecimento com dados CBIC reais
4. **PARTE 4**: Integração e backup no Google Sheets
5. **PARTE 5**: Formatação profissional (cores por método)
6. **PARTE 6**: Documentação técnica automática
7. **PARTE 7**: Orquestração e CLI com argumentos

## 📊 Dados do Sistema

- **Métodos**: 10 métodos construtivos
- **Colunas**: 26 campos por método
- **Total**: 260 células de dados
- **Fontes**: fact_cub_por_uf (4.598 linhas), fact_cub_detalhado (18.059 linhas)
- **CBIC**: Dados reais de custo m² por estado

## ⚙️ Variáveis de Ambiente

| Variável | Descrição | Exemplo |
|----------|-----------|---------|
| `GOOGLE_SHEETS_CREDENTIALS_PATH` | Caminho para credenciais JSON | `./configs/credentials.json` |
| `GOOGLE_SHEETS_SPREADSHEET_ID` | ID da planilha Google Sheets | `1ABC123XYZ789...` |
| `GOOGLE_SHEETS_WORKSHEET_NAME` | Nome da aba a atualizar | `dim_metodo` |
| `CBIC_BASE_URL` | URL base CBIC (opcional) | `https://cbic.org.br/cub` |
| `LOG_LEVEL` | Nível de log (opcional) | `INFO` |

## 📝 Status do Desenvolvimento

✅ **Completo** - 7/7 partes implementadas (100%)
- Sistema operacional e pronto para uso
- 2.462 linhas de código
- 11/11 componentes funcionais
- CLI com todos os argumentos
- Documentação técnica completa

## 🔧 Manutenção

### Atualizações de Dados
- CSV: Edite `configs/dim_metodo_v2.csv`
- Estrutura: Modifique `HEADER_DIM_METODO` no script
- CBIC: Dados atualizados automaticamente a cada execução

### Logs e Debug  
- Use `--verbose` para logs detalhados
- Use `--dry-run` para testes sem alterações
- Logs estruturados com `structlog`

### Performance
- Sistema processa 10 métodos em ~30-60 segundos
- CBIC: ~2-3 segundos por consulta (opcional com --skip-cbic)
- Google Sheets: Backup + atualização + formatação ~10-20 segundos