# Construction Data Pipeline - Exemplos de Uso CLI
# PARTE 8: Guia de Comandos

## 🎯 Cenários de Uso Comuns

### 1. Primeira Execução (Recomendado)
```bash
# Testar configuração sem alterar dados
python src/scripts/update_dim_metodo_complete.py --dry-run --verbose

# Resultado esperado:
# ✅ SUCESSO: Todas as 7 PARTES estao implementadas!
# ✅ CSV validado: 10 métodos, 26 colunas
# ✅ CBIC conectado: Dados disponíveis
# ✅ Google Sheets conectado: Planilha acessível
# ✅ STATUS FINAL: 11/11 componentes (100.0%)
```

### 2. Execução de Produção
```bash
# Execução completa com todos os recursos
python src/scripts/update_dim_metodo_complete.py

# Com logs detalhados
python src/scripts/update_dim_metodo_complete.py --verbose
```

### 3. Execução Rápida (Sem CBIC)
```bash
# Pular consultas CBIC para execução mais rápida
python src/scripts/update_dim_metodo_complete.py --skip-cbic

# Resultado: ~15-20 segundos vs ~45-60 segundos
```

### 4. Debug e Desenvolvimento
```bash
# Máximo de informações para debug
python src/scripts/update_dim_metodo_complete.py --dry-run --verbose --skip-cbic

# Para testar apenas validação CSV
python src/scripts/update_dim_metodo_complete.py --dry-run --skip-cbic
```

## 📋 Argumentos da CLI

| Argumento | Descrição | Impacto |
|-----------|-----------|---------|
| `--dry-run` | Simula execução sem alterar dados | Apenas validação e testes |
| `--verbose` | Logs detalhados de cada operação | Máxima visibilidade |  
| `--skip-cbic` | Pula enriquecimento CBIC | Execução 2-3x mais rápida |

## ⏱️ Tempo de Execução

```bash
# Execução completa: ~45-60 segundos
python src/scripts/update_dim_metodo_complete.py

# Execução rápida: ~15-20 segundos  
python src/scripts/update_dim_metodo_complete.py --skip-cbic

# Dry run: ~5-10 segundos
python src/scripts/update_dim_metodo_complete.py --dry-run --skip-cbic
```

## 🔍 Interpretando a Saída

### Saída de Sucesso
```
✅ PARTE 1: Estrutura de 26 colunas definida
✅ PARTE 2: CSV validado (10 métodos, 26 colunas)
✅ PARTE 3: Dados CBIC enriquecidos (10/10 métodos)
✅ PARTE 4: Backup criado: dim_metodo_backup_20241114_143022
✅ PARTE 5: Planilha formatada com cores por método
✅ PARTE 6: Documentação técnica gerada (9 seções)
✅ PARTE 7: Orquestração completa

🎉 SISTEMA ATUALIZADO COM SUCESSO!
📊 Total: 10 métodos × 26 colunas = 260 dados atualizados
⏱️ Tempo de execução: 47.3 segundos
```

### Saída de Erro Comum
```
❌ ERRO: Arquivo CSV não encontrado
📁 Esperado: c:\Users\...\configs\dim_metodo_v2.csv
💡 Solução: Verifique se o arquivo existe e tem as permissões corretas

❌ ERRO: Credenciais Google Sheets inválidas  
🔐 Esperado: c:\Users\...\configs\google_sheets_credentials.json
💡 Solução: Configure as credenciais conforme PROJECT_STRUCTURE.md
```

## 🚨 Troubleshooting

### Problema: ModuleNotFoundError
```bash
# Solução: Instalar dependências
pip install -r requirements.txt
```

### Problema: Credenciais Google Sheets
```bash
# Verificar arquivo de credenciais
ls configs/google_sheets_credentials.json

# Verificar variáveis de ambiente
cat .env | grep GOOGLE_SHEETS
```

### Problema: CSV não encontrado
```bash
# Verificar estrutura de arquivos
ls configs/dim_metodo_v2.csv

# Verificar conteúdo do CSV
head -n 5 configs/dim_metodo_v2.csv
```

### Problema: Timeout CBIC
```bash
# Usar --skip-cbic para contornar
python src/scripts/update_dim_metodo_complete.py --skip-cbic

# Ou configurar timeout no .env
echo "CBIC_TIMEOUT=60" >> .env
```

## 📅 Execução Programada

### Windows Task Scheduler
```bash
# Comando para agendamento
powershell.exe -Command "cd 'C:\Users\mathe\Desktop\construction-data-pipeline'; python src/scripts/update_dim_metodo_complete.py"
```

### Cron (Linux/Mac)
```bash
# Executar diariamente às 8:00
0 8 * * * cd /path/to/construction-data-pipeline && python src/scripts/update_dim_metodo_complete.py
```

## 🎯 Fluxo de Trabalho Recomendado

1. **Setup Inicial**:
   ```bash
   pip install -r requirements.txt
   cp .env.template .env
   # Editar .env com suas configurações
   ```

2. **Teste de Configuração**:
   ```bash
   python src/scripts/update_dim_metodo_complete.py --dry-run --verbose
   ```

3. **Primeira Execução**:
   ```bash
   python src/scripts/update_dim_metodo_complete.py --verbose
   ```

4. **Execução Regular**:
   ```bash
   python src/scripts/update_dim_metodo_complete.py
   ```

5. **Execução Rápida** (quando CBIC não é crítico):
   ```bash
   python src/scripts/update_dim_metodo_complete.py --skip-cbic
   ```