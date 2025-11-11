# 📊 Upload to Google Sheets

Script para upload automático de dados corrigidos do Sistema CUB para Google Sheets.

## 🎯 Funcionalidades

- ✅ Upload automático de DataFrames para Google Sheets
- ✅ Autenticação via Service Account (seguro)
- ✅ Substituição completa do conteúdo da aba
- ✅ Formatação automática de números (2 casas decimais)
- ✅ Criação automática de abas (se não existirem)
- ✅ Modo dry-run para simulação
- ✅ Interface CLI com argumentos flexíveis
- ✅ Logging estruturado com structlog
- ✅ Tratamento robusto de erros

## 📋 Pré-requisitos

### 1. Instalar dependências

```bash
pip install gspread>=6.0.0 gspread-dataframe>=4.0.0 pandas>=2.0.0 python-dotenv>=1.0.0 structlog>=24.0.0
```

Ou use o requirements.txt do projeto:

```bash
pip install -r requirements.txt
```

### 2. Configurar Service Account do Google

1. Acesse [Google Cloud Console](https://console.cloud.google.com/)
2. Crie um novo projeto ou selecione um existente
3. Ative as APIs:
   - Google Sheets API
   - Google Drive API
4. Crie uma Service Account:
   - IAM & Admin → Service Accounts → Create Service Account
   - Nome: `construction-data-uploader` (ou outro)
   - Role: Não precisa de roles no projeto
5. Crie uma chave JSON:
   - Clique na Service Account criada
   - Keys → Add Key → Create New Key → JSON
   - Salve o arquivo baixado em `config/google_credentials.json`

### 3. Compartilhar planilha Google Sheets

1. Abra sua planilha no Google Sheets
2. Clique em "Compartilhar"
3. Adicione o **email da Service Account** (encontrado no JSON ou na console)
   - Exemplo: `construction-data-uploader@projeto-123456.iam.gserviceaccount.com`
4. Permissão: **Editor**
5. Clique em "Enviar"

### 4. Configurar variáveis de ambiente

Copie `.env.example` para `.env`:

```bash
cp .env.example .env
```

Edite `.env` e configure:

```bash
GOOGLE_SHEETS_CREDENTIALS_PATH=config/google_credentials.json
GOOGLE_SHEETS_SPREADSHEET_ID=1QhLqfPB_yJDipDfDg1-2zPj9cEHCcWi9glv-fFOO_B8
```

**Como encontrar o SPREADSHEET_ID:**
- URL da planilha: `https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/edit`
- Copie o ID entre `/d/` e `/edit`

## 🚀 Uso

### Uso básico (padrões do .env)

```bash
python -m src.scripts.upload_to_google_sheets
```

Isso vai:
- Carregar `docs/fact_cub_detalhado_CORRIGIDO_V3.md`
- Fazer upload para a aba `fact_cub_detalhado`
- Usar credenciais e ID do `.env`

### Simular upload (dry-run)

```bash
python -m src.scripts.upload_to_google_sheets --dry-run
```

Útil para testar sem modificar a planilha.

### Upload de arquivo customizado

```bash
python -m src.scripts.upload_to_google_sheets --file docs/custom_data.md
```

### Upload para aba diferente

```bash
python -m src.scripts.upload_to_google_sheets --tab-name "fact_cub_2024"
```

### Especificar planilha diferente

```bash
python -m src.scripts.upload_to_google_sheets --sheet-id "OUTRO_SPREADSHEET_ID"
```

### Combinar argumentos

```bash
python -m src.scripts.upload_to_google_sheets \
  --file docs/fact_cub_detalhado_CORRIGIDO_V2.md \
  --tab-name "fact_cub_v2" \
  --sheet-id "1abc123xyz..." \
  --dry-run
```

## 📊 Output esperado

### Sucesso

```
================================================================================
  📊 UPLOAD PARA GOOGLE SHEETS
================================================================================

📊 Carregando dados de docs/fact_cub_detalhado_CORRIGIDO_V3.md...
  ✅ 18,059 linhas carregadas
  ✅ 6 colunas: ['id_fato', 'data_referencia', 'uf', 'tipo_cub', 'valor', 'created_at']

🔐 Autenticando Google Sheets...
  ✅ Autenticação bem-sucedida!

📤 Preparando upload para aba 'fact_cub_detalhado'...
  ✅ Aba 'fact_cub_detalhado' encontrada
  🧹 Limpando aba 'fact_cub_detalhado'...
  📤 Enviando 18,059 linhas...
  ✅ Upload concluído em 12.34s

  🎨 Formatando coluna 'valor'...
  ✅ Coluna 'valor' formatada

================================================================================
  ✅ UPLOAD CONCLUÍDO COM SUCESSO!
================================================================================

📊 Estatísticas:
  • Linhas enviadas: 18,059
  • Colunas: 6
  • Tempo decorrido: 12.34s
  • Planilha: https://docs.google.com/spreadsheets/d/1QhLqfPB_yJDipDfDg1-2zPj9cEHCcWi9glv-fFOO_B8/edit
  • Aba: fact_cub_detalhado
```

### Dry-run

```
================================================================================
  📊 UPLOAD PARA GOOGLE SHEETS
================================================================================

🔍 MODO DRY RUN - Nenhuma modificação será feita

📊 Carregando dados de docs/fact_cub_detalhado_CORRIGIDO_V3.md...
  ✅ 18,059 linhas carregadas
  ✅ 6 colunas: ['id_fato', 'data_referencia', 'uf', 'tipo_cub', 'valor', 'created_at']

🔐 Autenticando Google Sheets...
  ✅ Autenticação bem-sucedida!

📤 Preparando upload para aba 'fact_cub_detalhado'...
  ✅ Aba 'fact_cub_detalhado' encontrada

🔍 [DRY RUN] Simulando upload...
  📊 Linhas a enviar: 18,059
  📊 Colunas: 6
  📊 Colunas: ['id_fato', 'data_referencia', 'uf', 'tipo_cub', 'valor', 'created_at']
  📊 Tipos de dados:
     • id_fato: object (18,059 não-nulos)
     • data_referencia: object (18,059 não-nulos)
     • uf: object (18,059 não-nulos)
     • tipo_cub: object (18,059 não-nulos)
     • valor: float64 (17,912 não-nulos)
     • created_at: object (18,059 não-nulos)

================================================================================
  ✅ UPLOAD CONCLUÍDO COM SUCESSO!
================================================================================

📊 Estatísticas:
  • Linhas enviadas: 18,059
  • Colunas: 6
  • Modo: DRY RUN (simulação)
```

## 🔧 Troubleshooting

### Erro: "Arquivo de credenciais não encontrado"

```
❌ ERRO: Arquivo de credenciais não encontrado: config/google_credentials.json
```

**Solução:**
1. Verifique se o arquivo `config/google_credentials.json` existe
2. Verifique o caminho no `.env` (`GOOGLE_SHEETS_CREDENTIALS_PATH`)
3. Certifique-se de ter baixado o JSON do Google Cloud Console

### Erro: "Planilha não encontrada"

```
❌ ERRO: Planilha não encontrada: 1QhLqfPB_yJDipDfDg1-2zPj9cEHCcWi9glv-fFOO_B8
```

**Solução:**
1. Verifique se o ID da planilha está correto no `.env`
2. Certifique-se de ter compartilhado a planilha com o email da Service Account
3. Verifique se a Service Account tem permissão de **Editor**

### Erro: "Permission denied"

```
❌ ERRO FATAL: The caller does not have permission
```

**Solução:**
1. Compartilhe a planilha com o email da Service Account
2. Dê permissão de **Editor** (não apenas Viewer)
3. Aguarde alguns minutos para as permissões propagarem

### Erro: "API not enabled"

```
❌ ERRO: Google Sheets API has not been used in project...
```

**Solução:**
1. Acesse [Google Cloud Console](https://console.cloud.google.com/)
2. Ative as APIs:
   - Google Sheets API
   - Google Drive API

## 📁 Estrutura de arquivos

```
construction-data-pipeline/
├── src/
│   └── scripts/
│       └── upload_to_google_sheets.py  # Script principal
├── config/
│   └── google_credentials.json         # Credenciais Service Account (não commitar!)
├── docs/
│   ├── fact_cub_detalhado_CORRIGIDO_V3.md  # Dados corrigidos
│   └── upload_to_google_sheets.md      # Esta documentação
├── .env                                 # Variáveis de ambiente (não commitar!)
├── .env.example                         # Exemplo de configuração
└── requirements.txt                     # Dependências Python
```

## 🔒 Segurança

- ⚠️ **NUNCA commitar** `config/google_credentials.json` no git
- ⚠️ **NUNCA commitar** `.env` no git
- ✅ Adicione ao `.gitignore`:
  ```
  .env
  config/google_credentials.json
  ```
- ✅ Use Service Account (não OAuth de usuário)
- ✅ Compartilhe planilhas apenas com Service Account necessária
- ✅ Use permissões mínimas (Editor apenas para planilhas específicas)

## 🔗 Links úteis

- [Google Sheets API Documentation](https://developers.google.com/sheets/api)
- [gspread Documentation](https://docs.gspread.org/)
- [Service Account Setup Guide](https://cloud.google.com/iam/docs/service-accounts)
- [Google Cloud Console](https://console.cloud.google.com/)

## 📝 Licença

Este script faz parte do projeto Construction Data Pipeline.

---

**Desenvolvido para o Sistema CUB - Melhor BI de Construção Civil do Brasil** 🏗️🇧🇷
