#!/bin/bash

################################################################################
# Script de Remoção de Credenciais do Histórico Git
# 
# Este script remove completamente o arquivo credentials.json do histórico
# do Git usando git-filter-repo (método mais seguro e rápido que filter-branch)
#
# ATENÇÃO: Este script reescreve o histórico do Git!
# - Faça backup do repositório antes de executar
# - Todos os colaboradores precisarão fazer git clone novamente
# - Force push será necessário
#
# Uso: ./scripts/remove_credentials_from_git.sh
################################################################################

set -e  # Exit on error
set -u  # Exit on undefined variable

# Cores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Função para logging
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Banner
echo "################################################################################"
echo "#                                                                              #"
echo "#           🔒 REMOVER CREDENCIAIS DO HISTÓRICO GIT 🔒                        #"
echo "#                                                                              #"
echo "################################################################################"
echo ""

# 1. Verificar se estamos no diretório correto
log_info "Verificando diretório do repositório..."
if [ ! -d ".git" ]; then
    log_error "Este script deve ser executado na raiz do repositório Git!"
    exit 1
fi
log_success "Repositório Git encontrado"

# 2. Verificar se git-filter-repo está instalado
log_info "Verificando se git-filter-repo está instalado..."
if ! command -v git-filter-repo &> /dev/null; then
    log_warning "git-filter-repo não encontrado. Instalando..."
    
    # Tentar instalar via pip
    if command -v pip3 &> /dev/null; then
        pip3 install git-filter-repo
    elif command -v pip &> /dev/null; then
        pip install git-filter-repo
    else
        log_error "pip não encontrado. Instale git-filter-repo manualmente:"
        log_error "  - macOS/Linux: pip3 install git-filter-repo"
        log_error "  - Windows: pip install git-filter-repo"
        log_error "  - Ou baixe de: https://github.com/newren/git-filter-repo"
        exit 1
    fi
fi
log_success "git-filter-repo está disponível"

# 3. Backup do repositório
log_info "Criando backup do repositório..."
BACKUP_DIR="../construction-data-pipeline-backup-$(date +%Y%m%d_%H%M%S)"
if [ -d "$BACKUP_DIR" ]; then
    log_warning "Backup já existe, pulando..."
else
    cp -r . "$BACKUP_DIR"
    log_success "Backup criado em: $BACKUP_DIR"
fi

# 4. Verificar se há mudanças não commitadas
log_info "Verificando mudanças não commitadas..."
if ! git diff-index --quiet HEAD --; then
    log_warning "Há mudanças não commitadas. Faça commit ou stash antes de continuar."
    read -p "Deseja fazer stash das mudanças? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        git stash save "Backup antes de remover credentials"
        log_success "Mudanças salvas em stash"
    else
        log_error "Cancele as mudanças ou faça commit antes de continuar"
        exit 1
    fi
fi

# 5. Atualizar .gitignore
log_info "Atualizando .gitignore..."
if ! grep -q "^credentials\.json$" .gitignore 2>/dev/null; then
    cat >> .gitignore << 'EOF'

# ============================================
# SEGURANÇA: Credenciais e Secrets
# ============================================
credentials.json
service-account.json
*-key.json
*.pem
*.p12
.env
.env.local
.env.*.local

# Arquivos de backup de credenciais
credentials.json.backup
credentials-*.json

# Tokens e API keys
token.json
*.key
*.secret
EOF
    log_success ".gitignore atualizado"
else
    log_success ".gitignore já contém credentials.json"
fi

# 6. Remover credentials.json do working directory
log_info "Removendo credentials.json do working directory..."
if [ -f "credentials.json" ]; then
    rm credentials.json
    log_success "credentials.json removido do working directory"
else
    log_info "credentials.json não encontrado no working directory"
fi

# 7. Remover do staging area
log_info "Removendo credentials.json do staging area..."
git rm --cached credentials.json 2>/dev/null || log_info "Arquivo não estava no staging area"

# 8. Commit das mudanças no .gitignore
log_info "Commitando .gitignore atualizado..."
git add .gitignore
git commit -m "security: update .gitignore to prevent credentials commit" || log_info "Nada para commitar"

# 9. REMOVER DO HISTÓRICO usando git-filter-repo
log_warning "============================================"
log_warning "ATENÇÃO: O HISTÓRICO DO GIT SERÁ REESCRITO!"
log_warning "============================================"
log_warning "Isso irá:"
log_warning "  - Remover credentials.json de TODOS os commits"
log_warning "  - Alterar todos os commit hashes"
log_warning "  - Requerer force push para o GitHub"
log_warning "  - Exigir que colaboradores façam git clone novamente"
echo ""
read -p "Deseja continuar? (y/n) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    log_error "Operação cancelada pelo usuário"
    exit 1
fi

log_info "Removendo credentials.json do histórico Git..."
git-filter-repo --path credentials.json --invert-paths --force

log_success "credentials.json removido do histórico Git!"

# 10. Verificar se o arquivo foi removido
log_info "Verificando se credentials.json ainda existe no histórico..."
if git log --all --full-history -- credentials.json | grep -q "commit"; then
    log_error "FALHA: credentials.json ainda aparece no histórico!"
    exit 1
else
    log_success "✓ credentials.json completamente removido do histórico"
fi

# 11. Limpar refs e garbage collection
log_info "Limpando referências antigas e executando garbage collection..."
git reflog expire --expire=now --all
git gc --prune=now --aggressive
log_success "Limpeza completa"

# 12. Reconectar remote (git-filter-repo remove remotes)
log_info "Reconectando remote origin..."
REPO_URL="https://github.com/matheusoption-bit/construction-data-pipeline.git"
git remote add origin "$REPO_URL"
log_success "Remote origin reconectado"

# 13. Instruções finais
echo ""
echo "################################################################################"
echo "#                                                                              #"
echo "#                   ✅ REMOÇÃO CONCLUÍDA COM SUCESSO! ✅                       #"
echo "#                                                                              #"
echo "################################################################################"
echo ""
log_success "credentials.json foi completamente removido do histórico Git"
echo ""
log_warning "PRÓXIMOS PASSOS OBRIGATÓRIOS:"
echo ""
echo "1️⃣  ROTACIONAR CREDENCIAIS NO GOOGLE CLOUD:"
echo "   - Acesse: https://console.cloud.google.com/iam-admin/serviceaccounts"
echo "   - Selecione: pipeline-service@golden-rush-477522-r6"
echo "   - Clique em 'Chaves' → 'Adicionar chave' → 'Criar nova chave'"
echo "   - Escolha JSON → Baixe o novo credentials.json"
echo "   - IMPORTANTE: Delete a chave antiga comprometida!"
echo ""
echo "2️⃣  ATUALIZAR GITHUB SECRETS:"
echo "   - Encode nova credencial: python -c \"import base64; print(base64.b64encode(open('credentials.json', 'rb').read()).decode())\""
echo "   - Acesse: https://github.com/matheusoption-bit/construction-data-pipeline/settings/secrets/actions"
echo "   - Atualize GOOGLE_CREDENTIALS com o novo base64"
echo ""
echo "3️⃣  FORCE PUSH PARA GITHUB (⚠️  ATENÇÃO ⚠️):"
echo "   git push origin main --force"
echo ""
echo "4️⃣  NOTIFICAR COLABORADORES:"
echo "   - Todos devem deletar o repositório local"
echo "   - Fazer git clone novamente"
echo "   - NÃO fazer git pull (não vai funcionar)"
echo ""
echo "5️⃣  VERIFICAR SEGURANÇA:"
echo "   - Acesse: https://github.com/matheusoption-bit/construction-data-pipeline/commits/main"
echo "   - Confirme que credentials.json não aparece em nenhum commit"
echo ""
log_info "Backup do repositório original salvo em: $BACKUP_DIR"
echo ""
echo "################################################################################"
