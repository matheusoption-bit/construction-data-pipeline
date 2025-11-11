# 🔐 Configuração do Google Sheets API

Guia completo passo-a-passo para configurar autenticação Google Sheets API e habilitar upload automático de dados do Sistema CUB.

---

## 📋 Pré-requisitos

Antes de começar, certifique-se de ter:

- ✅ Conta Google ativa
- ✅ Acesso ao [Google Cloud Console](https://console.cloud.google.com/)
- ✅ Python 3.10 ou superior instalado
- ✅ Projeto `construction-data-pipeline` clonado localmente

---

## 🚀 Passo 1: Criar Projeto no Google Cloud

### 1.1 Acessar Google Cloud Console

1. Acesse: [https://console.cloud.google.com/](https://console.cloud.google.com/)
2. Faça login com sua conta Google

### 1.2 Criar Novo Projeto

1. Clique no **seletor de projetos** no topo da página (ao lado do logo "Google Cloud")
2. Clique em **"NEW PROJECT"** (Novo Projeto)
3. Preencha os campos:
   - **Project name:** `construction-data-pipeline`
   - **Organization:** (deixe em branco se for conta pessoal)
   - **Location:** (deixe em branco se for conta pessoal)
4. Clique em **"CREATE"** (Criar)
5. Aguarde alguns segundos até o projeto ser criado
6. Selecione o projeto recém-criado no seletor de projetos

**📸 Visual esperado:**
```
┌─────────────────────────────────────────┐
│  Google Cloud                          │
│  ┌─────────────────────────────────┐   │
│  │ construction-data-pipeline      │   │
│  └─────────────────────────────────┘   │
└─────────────────────────────────────────┘
```

---

## 🔌 Passo 2: Habilitar Google Sheets API

### 2.1 Acessar APIs & Services

1. No menu lateral esquerdo (☰), clique em **"APIs & Services"**
2. Clique em **"Library"** (Biblioteca)

### 2.2 Buscar e Habilitar Google Sheets API

1. Na barra de busca, digite: `Google Sheets API`
2. Clique no resultado **"Google Sheets API"**
3. Clique no botão azul **"ENABLE"** (Ativar)
4. Aguarde alguns segundos até a API ser habilitada

### 2.3 Habilitar Google Drive API (Também necessária)

1. Volte para **"APIs & Services" → "Library"**
2. Busque: `Google Drive API`
3. Clique no resultado **"Google Drive API"**
4. Clique em **"ENABLE"** (Ativar)

**✅ Confirmação:**
Você verá uma página com métricas e status "API enabled" para ambas as APIs.

---

## 👤 Passo 3: Criar Service Account

### 3.1 Acessar Service Accounts

1. No menu lateral, vá para **"IAM & Admin"** → **"Service Accounts"**
2. Clique em **"+ CREATE SERVICE ACCOUNT"** (Criar conta de serviço)

### 3.2 Configurar Service Account

**Etapa 1: Service account details**
- **Service account name:** `sheets-uploader`
- **Service account ID:** `sheets-uploader` (será preenchido automaticamente)
- **Service account description:** `Service account para upload automático de dados CUB para Google Sheets`
- Clique em **"CREATE AND CONTINUE"**

**Etapa 2: Grant this service account access to project** (Opcional)
- **Pule esta etapa** - não precisa de roles no projeto
- Clique em **"CONTINUE"**

**Etapa 3: Grant users access to this service account** (Opcional)
- **Pule esta etapa** - não precisa dar acesso a outros usuários
- Clique em **"DONE"**

### 3.3 Criar Chave JSON

1. Na lista de Service Accounts, encontre **"sheets-uploader"**
2. Clique nos **três pontos** (⋮) à direita
3. Selecione **"Manage keys"** (Gerenciar chaves)
4. Clique em **"ADD KEY"** → **"Create new key"**
5. Selecione o tipo **"JSON"**
6. Clique em **"CREATE"**

**🔽 Download automático:**
Um arquivo JSON será baixado automaticamente para seu computador. O nome será algo como:
```
construction-data-pipeline-abc123xyz789.json
```

**⚠️ IMPORTANTE:**
- Este arquivo contém credenciais sensíveis
- **NUNCA compartilhe este arquivo** publicamente
- **NUNCA commite este arquivo** no GitHub

### 3.4 Anotar Email do Service Account

Abra o arquivo JSON baixado e encontre o campo `"client_email"`:

```json
{
  "type": "service_account",
  "project_id": "construction-data-pipeline-123456",
  "private_key_id": "abc123...",
  "private_key": "-----BEGIN PRIVATE KEY-----\n...",
  "client_email": "sheets-uploader@construction-data-pipeline-123456.iam.gserviceaccount.com",
  ...
}
```

**📝 Copie o email:** `sheets-uploader@construction-data-pipeline-123456.iam.gserviceaccount.com`

Você usará este email no Passo 5.

---

## 📁 Passo 4: Configurar Credenciais no Projeto

### 4.1 Criar Pasta config/

No terminal, dentro da pasta do projeto:

```bash
# Criar pasta config (se não existir)
mkdir config
```

### 4.2 Mover Arquivo JSON

Mova o arquivo JSON baixado para a pasta `config/` e renomeie:

**Windows (PowerShell):**
```powershell
Move-Item "C:\Users\SEU_USUARIO\Downloads\construction-data-pipeline-*.json" "config\google_credentials.json"
```

**Linux/Mac:**
```bash
mv ~/Downloads/construction-data-pipeline-*.json config/google_credentials.json
```

**Ou manualmente:**
1. Copie o arquivo JSON da pasta Downloads
2. Cole em `construction-data-pipeline/config/`
3. Renomeie para `google_credentials.json`

### 4.3 Verificar Estrutura de Arquivos

Sua estrutura deve estar assim:

```
construction-data-pipeline/
├── config/
│   └── google_credentials.json  ← Novo arquivo
├── src/
├── docs/
├── .env
└── .gitignore
```

### 4.4 Adicionar ao .gitignore

Verifique se `.gitignore` contém:

```gitignore
# Credenciais Google
config/google_credentials.json
config/*.json

# Environment
.env
```

Se não contiver, adicione essas linhas ao arquivo `.gitignore`.

### 4.5 Configurar Variável de Ambiente

Edite o arquivo `.env` na raiz do projeto:

```bash
# Configurações Google Sheets
GOOGLE_SHEETS_CREDENTIALS_PATH=config/google_credentials.json
GOOGLE_SHEETS_SPREADSHEET_ID=1QhLqfPB_yJDipDfDg1-2zPj9cEHCcWi9glv-fFOO_B8
```

**Como encontrar o SPREADSHEET_ID:**
- Abra sua planilha no Google Sheets
- A URL será: `https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/edit`
- Copie o ID entre `/d/` e `/edit`

**Exemplo:**
```
URL: https://docs.google.com/spreadsheets/d/1QhLqfPB_yJDipDfDg1-2zPj9cEHCcWi9glv-fFOO_B8/edit#gid=0
ID:  1QhLqfPB_yJDipDfDg1-2zPj9cEHCcWi9glv-fFOO_B8
```

---

## 📊 Passo 5: Compartilhar Planilha com Service Account

### 5.1 Abrir Google Sheets

1. Acesse sua planilha no Google Sheets
2. URL: `https://docs.google.com/spreadsheets/d/1QhLqfPB_yJDipDfDg1-2zPj9cEHCcWi9glv-fFOO_B8/edit`

### 5.2 Compartilhar com Service Account

1. Clique no botão **"Share"** (Compartilhar) no canto superior direito
2. No campo **"Add people and groups"**, cole o **email do service account** anotado no Passo 3.4:
   ```
   sheets-uploader@construction-data-pipeline-123456.iam.gserviceaccount.com
   ```
3. Altere a permissão para **"Editor"** (não apenas "Viewer")
4. **Desmarque** a opção "Notify people" (não precisa enviar email)
5. Clique em **"Share"** ou **"Done"**

**✅ Confirmação:**
O email do service account deve aparecer na lista de pessoas com acesso à planilha.

**⚠️ IMPORTANTE:**
- A permissão deve ser **"Editor"** (não "Viewer")
- Sem esta permissão, o upload falhará com erro "Permission denied"

---

## 📦 Passo 6: Instalar Dependências

### 6.1 Ativar Ambiente Virtual (se houver)

```bash
# Windows
.venv\Scripts\activate

# Linux/Mac
source .venv/bin/activate
```

### 6.2 Instalar Pacotes Necessários

```bash
pip install gspread>=6.0.0 gspread-dataframe>=4.0.0 python-dotenv>=1.0.0
```

Ou instale todas as dependências do projeto:

```bash
pip install -r requirements.txt
```

### 6.3 Verificar Instalação

```bash
python -c "import gspread; import gspread_dataframe; print('✅ Bibliotecas instaladas com sucesso!')"
```

**Output esperado:**
```
✅ Bibliotecas instaladas com sucesso!
```

---

## 🧪 Passo 7: Testar Conexão

### 7.1 Executar Teste em Modo Dry-Run

```bash
python -m src.scripts.upload_to_google_sheets --dry-run
```

**Output esperado (sucesso):**

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

================================================================================
  ✅ UPLOAD CONCLUÍDO COM SUCESSO!
================================================================================

📊 Estatísticas:
  • Linhas enviadas: 18,059
  • Colunas: 6
  • Modo: DRY RUN (simulação)
```

### 7.2 Executar Upload Real (Opcional)

Se o teste dry-run passou, você pode fazer o upload real:

```bash
python -m src.scripts.upload_to_google_sheets
```

Isso irá **substituir completamente** o conteúdo da aba `fact_cub_detalhado` na planilha.

---

## 🔧 Troubleshooting

### ❌ Erro: "Arquivo de credenciais não encontrado"

**Erro completo:**
```
❌ ERRO: Arquivo de credenciais não encontrado: config/google_credentials.json
```

**Soluções:**

1. Verifique se o arquivo existe:
   ```bash
   # Windows
   dir config\google_credentials.json
   
   # Linux/Mac
   ls -la config/google_credentials.json
   ```

2. Verifique o caminho no `.env`:
   ```bash
   GOOGLE_SHEETS_CREDENTIALS_PATH=config/google_credentials.json
   ```

3. Certifique-se de estar na raiz do projeto ao executar o script

---

### ❌ Erro: "Permission denied" ou "The caller does not have permission"

**Erro completo:**
```
❌ ERRO FATAL: The caller does not have permission
gspread.exceptions.APIError: {'code': 403, 'message': 'The caller does not have permission', ...}
```

**Soluções:**

1. **Verifique o compartilhamento da planilha:**
   - Abra a planilha no Google Sheets
   - Clique em "Share"
   - Certifique-se de que o email do service account está na lista
   - Exemplo: `sheets-uploader@construction-data-pipeline-123456.iam.gserviceaccount.com`

2. **Verifique a permissão:**
   - A permissão deve ser **"Editor"** (não "Viewer")
   - Clique no dropdown ao lado do email e mude para "Editor"

3. **Aguarde alguns minutos:**
   - Permissões podem levar até 5 minutos para propagar
   - Tente novamente após aguardar

---

### ❌ Erro: "API has not been used in project"

**Erro completo:**
```
❌ ERRO: Google Sheets API has not been used in project construction-data-pipeline-123456 
before or it is disabled.
```

**Soluções:**

1. **Verifique se as APIs estão habilitadas:**
   - Acesse [Google Cloud Console](https://console.cloud.google.com/)
   - Vá para "APIs & Services" → "Library"
   - Busque "Google Sheets API" e verifique se está "Enabled"
   - Busque "Google Drive API" e verifique se está "Enabled"

2. **Habilite as APIs:**
   - Se não estiverem habilitadas, clique em "Enable" para cada uma

3. **Aguarde alguns minutos:**
   - Pode levar até 5 minutos para as APIs serem ativadas

4. **Verifique o projeto correto:**
   - Certifique-se de estar usando o projeto correto no Google Cloud Console
   - O nome do projeto deve aparecer no topo da tela

---

### ❌ Erro: "Invalid credentials" ou "Could not automatically determine credentials"

**Erro completo:**
```
❌ ERRO: Falha na autenticação: Could not automatically determine credentials.
```

**Soluções:**

1. **Verifique o conteúdo do arquivo JSON:**
   - Abra `config/google_credentials.json`
   - Certifique-se de que é um JSON válido
   - Deve conter campos: `type`, `project_id`, `private_key`, `client_email`

2. **Verifique se não há espaços extras no caminho:**
   ```bash
   # No .env, NÃO use aspas ou espaços
   ✅ Correto:   GOOGLE_SHEETS_CREDENTIALS_PATH=config/google_credentials.json
   ❌ Incorreto: GOOGLE_SHEETS_CREDENTIALS_PATH="config/google_credentials.json"
   ❌ Incorreto: GOOGLE_SHEETS_CREDENTIALS_PATH=config/google credentials.json
   ```

3. **Recrie a chave:**
   - Se o problema persistir, delete a chave antiga no Google Cloud Console
   - Crie uma nova chave JSON (Passo 3.3)
   - Substitua o arquivo `config/google_credentials.json`

---

### ❌ Erro: "Planilha não encontrada"

**Erro completo:**
```
❌ ERRO: Planilha não encontrada: 1QhLqfPB_yJDipDfDg1-2zPj9cEHCcWi9glv-fFOO_B8
```

**Soluções:**

1. **Verifique o SPREADSHEET_ID:**
   - Abra a planilha no Google Sheets
   - Copie o ID da URL: `https://docs.google.com/spreadsheets/d/{ID}/edit`
   - Atualize no `.env`: `GOOGLE_SHEETS_SPREADSHEET_ID={ID}`

2. **Verifique se a planilha está compartilhada:**
   - A planilha DEVE estar compartilhada com o service account
   - Siga o Passo 5 para compartilhar

3. **Verifique se a planilha não foi deletada:**
   - Tente acessar a URL da planilha no navegador
   - Se retornar erro 404, a planilha foi deletada

---

### ❌ Erro: "Aba não encontrada" (será criada automaticamente)

**Mensagem esperada:**
```
⚠️  Aba 'fact_cub_detalhado' não existe, criando...
✅ Aba criada com sucesso
```

Isso **não é um erro**. O script cria automaticamente a aba se ela não existir.

---

## 🔒 Segurança e Boas Práticas

### ⚠️ NUNCA COMMITAR CREDENCIAIS

**Arquivos que NUNCA devem ser commitados:**
```
❌ config/google_credentials.json
❌ .env
❌ Qualquer arquivo com chaves ou secrets
```

### ✅ Verificar .gitignore

Seu `.gitignore` DEVE conter:

```gitignore
# Credenciais Google
config/google_credentials.json
config/*.json

# Environment variables
.env
.env.local

# Python
__pycache__/
*.pyc
.venv/
venv/
```

### 🔑 Rotação de Chaves

**Recomendação:** Rotacione as chaves do service account a cada 90 dias.

**Como rotacionar:**

1. Acesse Google Cloud Console → IAM & Admin → Service Accounts
2. Clique no service account `sheets-uploader`
3. Vá para "Keys"
4. Clique "Add Key" → "Create new key" → JSON
5. Baixe a nova chave
6. Substitua `config/google_credentials.json`
7. **Delete a chave antiga** na interface do Google Cloud

### 🛡️ Princípio do Menor Privilégio

- ✅ **Use:** Service Account dedicada para cada aplicação
- ✅ **Compartilhe:** Apenas as planilhas necessárias com a Service Account
- ✅ **Permissão:** Use "Editor" apenas se precisar escrever, caso contrário use "Viewer"
- ❌ **Evite:** Dar acesso de "Owner" para service accounts
- ❌ **Evite:** Reutilizar a mesma service account em múltiplos projetos

### 📋 Checklist de Segurança

Antes de fazer deploy:

- [ ] `.env` está no `.gitignore`
- [ ] `config/*.json` está no `.gitignore`
- [ ] Não há credenciais hardcoded no código
- [ ] Service Account tem apenas as permissões necessárias
- [ ] Planilha está compartilhada apenas com quem precisa
- [ ] Chaves têm menos de 90 dias

---

## 📚 Recursos Adicionais

### Links Úteis

- 📖 [Google Sheets API Documentation](https://developers.google.com/sheets/api)
- 📖 [gspread Documentation](https://docs.gspread.org/)
- 📖 [Service Accounts Guide](https://cloud.google.com/iam/docs/service-accounts)
- 📖 [Google Cloud Console](https://console.cloud.google.com/)
- 📖 [Best Practices for API Keys](https://cloud.google.com/docs/authentication/api-keys)

### Scripts Relacionados

- `src/scripts/upload_to_google_sheets.py` - Script principal de upload
- `docs/upload_to_google_sheets.md` - Documentação de uso do script

### Suporte

Se encontrar problemas não cobertos neste guia:

1. Verifique os logs do script (structlog)
2. Consulte a [documentação do gspread](https://docs.gspread.org/)
3. Abra uma issue no GitHub do projeto

---

## ✅ Checklist de Configuração Completa

Use este checklist para verificar se tudo está configurado:

- [ ] **Passo 1:** Projeto criado no Google Cloud Console
- [ ] **Passo 2:** Google Sheets API e Google Drive API habilitadas
- [ ] **Passo 3:** Service Account `sheets-uploader` criada
- [ ] **Passo 3:** Chave JSON baixada
- [ ] **Passo 4:** Arquivo `config/google_credentials.json` criado
- [ ] **Passo 4:** Variáveis configuradas no `.env`
- [ ] **Passo 4:** `config/*.json` adicionado ao `.gitignore`
- [ ] **Passo 5:** Planilha compartilhada com service account
- [ ] **Passo 5:** Permissão "Editor" concedida
- [ ] **Passo 6:** Dependências instaladas (`gspread`, `gspread-dataframe`)
- [ ] **Passo 7:** Teste dry-run executado com sucesso
- [ ] **Segurança:** Verificado que credenciais não estão no git

---

## 🎉 Pronto!

Sua configuração do Google Sheets API está completa! 

Agora você pode:

✅ Fazer upload automático de dados para Google Sheets  
✅ Integrar o pipeline com planilhas existentes  
✅ Compartilhar dados com stakeholders de forma segura  

**Próximo passo:** Execute o upload real:

```bash
python -m src.scripts.upload_to_google_sheets
```

---

**📅 Última atualização:** 11 de novembro de 2025  
**🏗️ Desenvolvido para:** Sistema CUB - Melhor BI de Construção Civil do Brasil 🇧🇷
