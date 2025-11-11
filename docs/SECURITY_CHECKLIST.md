# 🔒 Checklist de Segurança - Remoção de Credenciais do Git

## ⚠️ SITUAÇÃO CRÍTICA

**Arquivo comprometido:** `credentials.json`  
**Tipo:** Google Service Account Key  
**Email:** `pipeline-service@golden-rush-477522-r6.iam.gserviceaccount.com`  
**Projeto:** `golden-rush-477522-r6`  
**Status:** 🔴 **EXPOSTO NO REPOSITÓRIO PÚBLICO**

---

## 📋 Checklist de Remediação Completa

### Fase 1: Preparação (5 minutos)

- [ ] **1.1** Ler completamente este documento antes de iniciar
- [ ] **1.2** Ter acesso ao Google Cloud Console
- [ ] **1.3** Ter acesso às configurações do GitHub
- [ ] **1.4** Fazer backup do repositório local
  ```bash
  cp -r construction-data-pipeline construction-data-pipeline-backup-$(date +%Y%m%d)
  ```
- [ ] **1.5** Notificar equipe sobre a manutenção (se aplicável)

---

### Fase 2: Remover do Histórico Git (10 minutos)

#### 2.1 Instalar git-filter-repo

**macOS/Linux:**
```bash
pip3 install git-filter-repo
```

**Windows (PowerShell):**
```powershell
pip install git-filter-repo
```

**Ou baixar manualmente:**
- https://github.com/newren/git-filter-repo/releases

- [ ] **2.1.1** git-filter-repo instalado e funcionando

#### 2.2 Executar Script de Limpeza

```bash
cd construction-data-pipeline
chmod +x scripts/remove_credentials_from_git.sh
./scripts/remove_credentials_from_git.sh
```

**OU no Windows PowerShell:**
```powershell
# Executar comandos manualmente (git-filter-repo funciona no Windows)
git rm --cached credentials.json
git commit -m "security: remove credentials.json from tracking"
git filter-repo --path credentials.json --invert-paths --force
```

- [ ] **2.2.1** Script executado sem erros
- [ ] **2.2.2** Backup criado automaticamente
- [ ] **2.2.3** .gitignore atualizado

#### 2.3 Verificar Remoção

```bash
# Verificar se arquivo aparece no histórico
git log --all --full-history -- credentials.json

# Deve retornar vazio ou "no matches"
```

- [ ] **2.3.1** Nenhum commit contendo credentials.json encontrado
- [ ] **2.3.2** Arquivo não existe no working directory
- [ ] **2.3.3** git status mostra working tree limpo

---

### Fase 3: Rotacionar Credenciais Google Cloud (5 minutos)

#### 3.1 Acessar Google Cloud Console

1. Acesse: https://console.cloud.google.com/iam-admin/serviceaccounts
2. Selecione projeto: **golden-rush-477522-r6**
3. Localize service account: **pipeline-service@golden-rush-477522-r6.iam.gserviceaccount.com**

- [ ] **3.1.1** Console acessado com sucesso

#### 3.2 Desabilitar Chave Antiga

1. Clique na service account
2. Vá para aba **"KEYS" / "CHAVES"**
3. Localize a chave com ID: **86f872c946284540d74821f6e265dcef02a5d721**
4. Clique nos 3 pontos → **"Desativar" / "Disable"**
5. **NÃO DELETE AINDA** (aguarde 24-48h para garantir que não há uso ativo)

- [ ] **3.2.1** Chave antiga DESABILITADA (não deletada ainda)
- [ ] **3.2.2** Data de desabilitação anotada: _______________

#### 3.3 Criar Nova Chave

1. Clique em **"ADD KEY" / "ADICIONAR CHAVE"**
2. Selecione **"Create new key" / "Criar nova chave"**
3. Escolha tipo: **JSON**
4. Clique em **"CREATE" / "CRIAR"**
5. Arquivo `golden-rush-477522-r6-XXXXXXXX.json` será baixado automaticamente

- [ ] **3.3.1** Nova chave JSON criada
- [ ] **3.3.2** Arquivo baixado salvo em local seguro
- [ ] **3.3.3** Novo Key ID anotado: _______________

#### 3.4 Renomear e Guardar Nova Chave

```bash
# Renomear para credentials.json
mv ~/Downloads/golden-rush-477522-r6-*.json credentials.json

# Mover para diretório do projeto (NÃO VAI SER COMMITADO - está no .gitignore)
mv credentials.json /caminho/para/construction-data-pipeline/
```

- [ ] **3.4.1** Arquivo renomeado para credentials.json
- [ ] **3.4.2** Movido para diretório do projeto
- [ ] **3.4.3** Confirmado que está no .gitignore

---

### Fase 4: Atualizar GitHub Secrets (5 minutos)

#### 4.1 Codificar Nova Credencial em Base64

**macOS/Linux:**
```bash
cd construction-data-pipeline
python3 -c "import base64; print(base64.b64encode(open('credentials.json', 'rb').read()).decode())"
```

**Windows PowerShell:**
```powershell
cd construction-data-pipeline
python -c "import base64; print(base64.b64encode(open('credentials.json', 'rb').read()).decode())"
```

- [ ] **4.1.1** String base64 gerada
- [ ] **4.1.2** String copiada para clipboard (Ctrl+C)

#### 4.2 Atualizar Secret GOOGLE_CREDENTIALS

1. Acesse: https://github.com/matheusoption-bit/construction-data-pipeline/settings/secrets/actions
2. Localize secret: **GOOGLE_CREDENTIALS**
3. Clique em **"Update" / "Atualizar"**
4. Cole a nova string base64
5. Clique em **"Update secret"**

- [ ] **4.2.1** Secret GOOGLE_CREDENTIALS atualizado
- [ ] **4.2.2** Confirmação de atualização recebida

#### 4.3 Testar Nova Credencial Localmente

```bash
# Testar autenticação
python -c "
from oauth2client.service_account import ServiceAccountCredentials
import gspread

creds = ServiceAccountCredentials.from_json_keyfile_name(
    'credentials.json',
    ['https://www.googleapis.com/auth/spreadsheets']
)
client = gspread.authorize(creds)
print('✅ Autenticação bem-sucedida!')
"
```

- [ ] **4.3.1** Teste local passou sem erros
- [ ] **4.3.2** Mensagem de sucesso exibida

---

### Fase 5: Force Push e Limpeza (10 minutos)

#### 5.1 Force Push para GitHub

⚠️ **ATENÇÃO: Isso reescreve o histórico no GitHub!**

```bash
# Verificar remote
git remote -v

# Force push
git push origin main --force
```

**Mensagem esperada:**
```
+ refs/heads/main:refs/heads/main (forced update)
```

- [ ] **5.1.1** Force push executado com sucesso
- [ ] **5.1.2** Nenhum erro de permissão
- [ ] **5.1.3** Histórico reescrito no GitHub

#### 5.2 Verificar GitHub

1. Acesse: https://github.com/matheusoption-bit/construction-data-pipeline/commits/main
2. Clique em cada commit recente
3. Procure por `credentials.json` nos arquivos alterados

- [ ] **5.2.1** Nenhum commit contém credentials.json
- [ ] **5.2.2** .gitignore contém credentials.json
- [ ] **5.2.3** Histórico limpo confirmado

#### 5.3 Testar GitHub Actions

1. Acesse: https://github.com/matheusoption-bit/construction-data-pipeline/actions
2. Clique em **"Daily BCB Data Ingestion"**
3. Clique em **"Run workflow"** → **"Run workflow"**
4. Aguarde execução (2-5 minutos)

- [ ] **5.3.1** Workflow iniciou sem erros
- [ ] **5.3.2** Autenticação com nova credencial funcionou
- [ ] **5.3.3** Job completou com sucesso

---

### Fase 6: Deletar Chave Antiga (Após 24-48h)

⏰ **AGUARDAR 24-48 HORAS** antes desta etapa

#### 6.1 Confirmar Nenhum Uso da Chave Antiga

1. Google Cloud Console → **Logging**
2. Filtrar por: `protoPayload.authenticationInfo.principalEmail="pipeline-service@golden-rush-477522-r6.iam.gserviceaccount.com"`
3. Verificar últimas 48h de atividade
4. Confirmar que apenas nova chave está sendo usada

- [ ] **6.1.1** Logs revisados
- [ ] **6.1.2** Nenhuma atividade com chave antiga
- [ ] **6.1.3** Data da verificação: _______________

#### 6.2 Deletar Chave Antiga Permanentemente

1. Console → Service Accounts → pipeline-service
2. Aba "KEYS"
3. Localize chave com ID: **86f872c946284540d74821f6e265dcef02a5d721**
4. Clique nos 3 pontos → **"Delete" / "Excluir"**
5. Confirme a exclusão

- [ ] **6.2.1** Chave antiga DELETADA permanentemente
- [ ] **6.2.2** Data da exclusão: _______________
- [ ] **6.2.3** Screenshot salvo como evidência

---

### Fase 7: Prevenir Futuros Incidentes (15 minutos)

#### 7.1 Instalar Pre-commit Hooks

```bash
# Instalar pre-commit
pip install pre-commit

# Instalar hooks do projeto
cd construction-data-pipeline
pre-commit install
```

- [ ] **7.1.1** pre-commit instalado
- [ ] **7.1.2** Hooks instalados no repositório
- [ ] **7.1.3** Teste executado: `pre-commit run --all-files`

#### 7.2 Configurar Alertas GitHub

1. Acesse: https://github.com/matheusoption-bit/construction-data-pipeline/settings/security_analysis
2. Habilite: **"Dependabot alerts"**
3. Habilite: **"Secret scanning"**
4. Habilite: **"Code scanning"** (GitHub Advanced Security)

- [ ] **7.2.1** Dependabot habilitado
- [ ] **7.2.2** Secret scanning habilitado
- [ ] **7.2.3** Code scanning configurado (se disponível)

#### 7.3 Adicionar Arquivo de Segurança

```bash
# Criar SECURITY.md
touch SECURITY.md
```

Conteúdo em `SECURITY.md`:
```markdown
# Security Policy

## Reporting a Vulnerability

Se você descobrir uma vulnerabilidade de segurança, por favor:

1. **NÃO** abra uma issue pública
2. Envie email para: [seu-email@exemplo.com]
3. Inclua detalhes da vulnerabilidade
4. Aguarde resposta em até 48 horas

## Security Best Practices

- Nunca commite credentials.json
- Sempre use GitHub Secrets para credenciais
- Rotacione credenciais a cada 90 dias
- Use pre-commit hooks para validação
```

- [ ] **7.3.1** SECURITY.md criado
- [ ] **7.3.2** Arquivo commitado
- [ ] **7.3.3** Email de contato atualizado

#### 7.4 Documentar no README

Adicionar seção de segurança no README.md:

```markdown
## 🔒 Segurança

Este projeto utiliza:
- ✅ GitHub Secrets para credenciais sensíveis
- ✅ Pre-commit hooks para prevenir commits acidentais
- ✅ Secret scanning habilitado
- ✅ Rotação de credenciais a cada 90 dias

**NUNCA** commite arquivos de credenciais:
- `credentials.json`
- `.env`
- `*-key.json`
```

- [ ] **7.4.1** Seção de segurança adicionada ao README
- [ ] **7.4.2** Mudanças commitadas

---

### Fase 8: Notificar Equipe (Se Aplicável)

#### 8.1 Notificação Imediata

**Template de Email/Slack:**

```
🔴 ALERTA DE SEGURANÇA - AÇÃO NECESSÁRIA

Pessoal,

Detectamos que credenciais do Google Cloud foram expostas no repositório Git.
As credenciais foram ROTACIONADAS e o histórico foi LIMPO.

AÇÃO NECESSÁRIA:
1. DELETE seu repositório local: rm -rf construction-data-pipeline
2. Clone novamente: git clone https://github.com/matheusoption-bit/construction-data-pipeline.git
3. NÃO tente fazer git pull (não vai funcionar devido ao force push)

As credenciais antigas foram desabilitadas e serão deletadas em 48h.

Qualquer dúvida, entre em contato.
```

- [ ] **8.1.1** Email/mensagem enviada
- [ ] **8.1.2** Confirmação de recebimento
- [ ] **8.1.3** Data do envio: _______________

---

## 📊 Checklist de Verificação Final

### Segurança

- [ ] ✅ credentials.json removido do Git (local e remoto)
- [ ] ✅ Histórico Git limpo e verificado
- [ ] ✅ Chave antiga desabilitada no Google Cloud
- [ ] ✅ Nova chave criada e testada
- [ ] ✅ GitHub Secrets atualizado
- [ ] ✅ Force push realizado com sucesso
- [ ] ✅ Pre-commit hooks instalados
- [ ] ✅ Secret scanning habilitado
- [ ] ✅ .gitignore atualizado e validado

### Funcionalidade

- [ ] ✅ Pipeline local funciona com nova credencial
- [ ] ✅ GitHub Actions funciona com nova credencial
- [ ] ✅ Planilha Google Sheets acessível
- [ ] ✅ Logs não mostram erros de autenticação
- [ ] ✅ Job diário agendado funcionando

### Documentação

- [ ] ✅ SECURITY.md criado
- [ ] ✅ README atualizado com seção de segurança
- [ ] ✅ .pre-commit-config.yaml configurado
- [ ] ✅ Equipe notificada (se aplicável)
- [ ] ✅ Incident report documentado

---

## 🔖 Lições Aprendidas

### O que deu errado?
1. credentials.json foi commitado acidentalmente
2. Não havia pre-commit hooks para prevenir
3. Secret scanning não estava habilitado

### Como prevenir no futuro?
1. ✅ Pre-commit hooks instalados
2. ✅ .gitignore robusto
3. ✅ Secret scanning habilitado
4. ✅ Documentação de segurança clara
5. ✅ Rotação regular de credenciais (90 dias)

### Próximos passos?
1. Agendar rotação de credenciais: **__ / __ / ____** (90 dias)
2. Revisar acessos no Google Cloud mensalmente
3. Auditar logs de acesso semanalmente

---

## 📞 Contatos de Emergência

**Google Cloud Support:**  
https://cloud.google.com/support

**GitHub Support:**  
https://support.github.com

**Segurança da Equipe:**  
[Adicionar contato aqui]

---

## ✅ Assinaturas

**Executado por:** _______________  
**Data:** __ / __ / ____  
**Hora início:** __:__  
**Hora fim:** __:__  
**Duração total:** _____ minutos

**Revisado por:** _______________  
**Data:** __ / __ / ____  

---

**Versão:** 1.0  
**Última atualização:** 2025-11-10  
**Status:** 🟢 Ativo
