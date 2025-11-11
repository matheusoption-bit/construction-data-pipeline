# 🚨 RELATÓRIO DE PROBLEMAS CRÍTICOS - BCBClient

**Data:** 2025-11-11 12:47 UTC  
**Analista:** GitHub Copilot  
**Repository:** matheusoption-bit/construction-data-pipeline

---

## 📊 RESUMO EXECUTIVO

**Status:** 🔴 CRÍTICO  
**Linhas Afetadas:** 903/1050 (86% dos dados corrompidos)  
**Causa Raiz:** Lógica de datas incorreta + Falta de validação

---

## ❌ PROBLEMAS IDENTIFICADOS

### 1. Valores Vazios/Nulos (903 linhas)

| Série | Linhas Vazias | Período Afetado |
|-------|---------------|-----------------|
| BCB_SGS_226 (TR) | 363 | 2024-11-13 até 2025-11-10 |
| BCB_SGS_1 (Câmbio) | 249 | 2024-11-13 até 2025-11-10 |
| BCB_SGS_432 (Selic) | 219 | 2024-11-12 até 2025-06-18 |
| BCB_SGS_4390 (Crédito PF) | 13 | 2024-11-01 até 2025-11-01 |
| BCB_SGS_4189 (INPC) | 13 | 2024-11-01 até 2025-11-01 |
| BCB_SGS_433 (IPCA) | 12 | 2024-11-01 até 2025-10-01 |
| BCB_SGS_189 (IGP-M) | 12 | 2024-11-01 até 2025-10-01 |
| BCB_SGS_7478 (Poupança) | 12 | 2024-11-01 até 2025-10-01 |
| BCB_SGS_24364 (Créd. Hab.) | 10 | 2024-11-01 até 2025-08-01 |

**Causa:** Datas não disponíveis na API (futuras ou sem divulgação ainda)

---

### 2. Valor Absurdo (1 linha)

```
id_fato: BCB_SGS_1207_2024-01-01
valor: 11.744.710.041.819,00 (11 trilhões!)
data: 2024-01-01
```

**Causa:** Erro na conversão ou dado corrompido da API

---

### 3. Valores Fixos Incorretos - Selic

```
Série: BCB_SGS_432 (Selic)
Valor fixo: 15.0
Repetições: 146/146 (100%)
Período: 2025-06-19 até 2025-11-11
```

**Descoberta Chocante:** 
- ✅ API do BCB **ESTÁ RETORNANDO** dados diários para Selic
- ❌ Mas a série 432 **É MENSAL**, não diária!
- ❌ Job está buscando dados com `end_date=datetime.now()` (hoje)
- ❌ Para séries mensais, isso pega dados **dentro do mês atual** (não divulgados ainda)
- ❌ API retorna valor padrão `15.0` para datas sem divulgação

---

## 🔍 CAUSA RAIZ

### Arquivo: `src/jobs/daily_bcb.py` (linha 97)

```python
# ❌ CÓDIGO PROBLEMÁTICO
def process_series(...):
    # Calcular datas (últimos N meses)
    end_date = datetime.now()  # ← PROBLEMA AQUI!
    start_date = end_date - timedelta(days=months_back * 30)
    
    start_date_str = start_date.strftime("%d/%m/%Y")
    end_date_str = end_date.strftime("%d/%m/%Y")  # 11/11/2025
```

**Problema:**
1. `end_date = datetime.now()` usa **data de hoje** (11/11/2025)
2. Para séries **MENSAIS**, dados de novembro/2025 **NÃO FORAM DIVULGADOS**
3. API retorna:
   - Séries diárias: últimos valores disponíveis (OK)
   - Séries mensais: valores **futuros/vazios** ou **valor padrão** (ERRO)

---

## ✅ SOLUÇÕES

### Solução 1: Corrigir Lógica de Datas

```python
# ✅ CÓDIGO CORRIGIDO
def process_series(...):
    # Usar último dia do mês ANTERIOR para séries mensais
    hoje = datetime.now()
    
    # Para séries mensais: usar último dia do mês anterior
    if freq == "mensal":
        # Primeiro dia do mês atual
        primeiro_dia_mes = hoje.replace(day=1)
        # Último dia do mês anterior
        end_date = primeiro_dia_mes - timedelta(days=1)
    else:
        # Para séries diárias: usar ontem (evita fim de semana)
        end_date = hoje - timedelta(days=1)
    
    start_date = end_date - timedelta(days=months_back * 30)
    
    start_date_str = start_date.strftime("%d/%m/%Y")
    end_date_str = end_date.strftime("%d/%m/%Y")
```

### Solução 2: Adicionar Validação no BCBClient

```python
# Adicionar em src/clients/bcb.py

def _validate_date_range(self, series_id: int, start_date: str, end_date: str) -> tuple:
    """
    Valida e ajusta range de datas para evitar dados futuros.
    
    Returns:
        (start_date_adjusted, end_date_adjusted)
    """
    hoje = datetime.now().date()
    
    # Converter datas
    end_dt = datetime.strptime(end_date, "%d/%m/%Y").date()
    
    # Se data final é futura, ajustar para ontem
    if end_dt > hoje:
        logger.warning(
            "future_date_detected",
            series_id=series_id,
            requested_date=end_date,
            adjusted_to="ontem"
        )
        end_dt = hoje - timedelta(days=1)
        end_date = end_dt.strftime("%d/%m/%Y")
    
    return start_date, end_date
```

### Solução 3: Filtrar Valores Inválidos

```python
# Adicionar em src/etl/sheets.py

def _validate_before_write(self, df: pd.DataFrame, series_id: str) -> pd.DataFrame:
    """
    Valida dados antes de escrever, removendo valores inválidos.
    """
    df = df.copy()
    
    # Converter valor para numérico
    df['valor_num'] = pd.to_numeric(df['valor'], errors='coerce')
    
    # Filtrar:
    # 1. Valores nulos
    # 2. Valores zero (suspeitos)
    # 3. Valores absurdos (> 1 milhão ou < -1000)
    
    df_valid = df[
        (df['valor_num'].notna()) &
        (df['valor_num'] != 0) &
        (df['valor_num'] < 1_000_000) &
        (df['valor_num'] > -1000)
    ].copy()
    
    removed = len(df) - len(df_valid)
    
    if removed > 0:
        logger.warning(
            "invalid_values_filtered",
            series_id=series_id,
            removed_count=removed,
            original_count=len(df)
        )
    
    return df_valid.drop(columns=['valor_num'])
```

### Solução 4: Limpar Dados Corrompidos

```python
# Script: scripts/fix_fact_series_corruption.py

def clean_corrupted_data():
    """
    Remove dados corrompidos de fact_series.
    """
    loader = SheetsLoader()
    df = loader.read_fact_series()
    
    # Converter valor para numérico
    df['valor_num'] = pd.to_numeric(df['valor'], errors='coerce')
    
    # Filtrar dados válidos
    df_clean = df[
        (df['valor_num'].notna()) &  # Não nulo
        (df['valor_num'] != 0) &      # Não zero
        (df['valor_num'] < 1_000_000) &  # Não absurdo
        (df['valor_num'] > -1000)
    ].copy()
    
    # Remover coluna temporária
    df_clean = df_clean.drop(columns=['valor_num'])
    
    # Reescrever aba
    worksheet = loader._get_spreadsheet().worksheet("fact_series")
    worksheet.clear()
    
    headers = [list(df_clean.columns)]
    rows = df_clean.values.tolist()
    
    all_data = headers + rows
    worksheet.update(values=all_data, range_name='A1')
    
    logger.info(
        "fact_series_cleaned",
        removed=len(df) - len(df_clean),
        remaining=len(df_clean)
    )
```

---

## 📋 PLANO DE AÇÃO

### Prioridade ALTA (Executar Imediatamente)

- [x] **Etapa 1:** Diagnóstico completo (DONE)
- [ ] **Etapa 2:** Corrigir `daily_bcb.py` (lógica de datas)
- [ ] **Etapa 3:** Adicionar validação em `BCBClient`
- [ ] **Etapa 4:** Adicionar filtro em `write_fact_series()`
- [ ] **Etapa 5:** Criar script de limpeza
- [ ] **Etapa 6:** Executar limpeza em produção
- [ ] **Etapa 7:** Re-executar job com correções
- [ ] **Etapa 8:** Validar dados limpos

### Prioridade MÉDIA (Próximos Dias)

- [ ] Adicionar testes unitários para validação de datas
- [ ] Criar alerta automático para dados suspeitos
- [ ] Documentar frequência de atualização de cada série
- [ ] Implementar cache de última data disponível por série

---

## 🎯 MÉTRICAS DE SUCESSO

**Antes:**
- ❌ 903/1050 linhas corrompidas (86%)
- ❌ Valores vazios não filtrados
- ❌ Valores absurdos não detectados
- ❌ Séries com valores fixos incorretos

**Depois (Esperado):**
- ✅ 0 linhas corrompidas
- ✅ Validação automática de datas
- ✅ Filtro de valores inválidos
- ✅ Dados 100% confiáveis

---

## 📝 NOTAS TÉCNICAS

### Frequências das Séries BCB

| Série | Nome | Frequência | Divulgação |
|-------|------|------------|------------|
| 432 | Selic | **Mensal** | 1ª semana do mês seguinte |
| 226 | TR | **Diária** | D+1 |
| 1 | Câmbio | **Diária** | D+1 (exceto fim de semana) |
| 433 | IPCA | **Mensal** | ~10 dias após fim do mês |
| 189 | IGP-M | **Mensal** | ~último dia do mês |
| 7478 | Poupança | **Mensal** | 1ª semana do mês seguinte |
| 4189 | INPC | **Mensal** | ~10 dias após fim do mês |
| 4390 | Crédito PF | **Mensal** | ~25 dias após fim do mês |
| 1207 | Produção CC | **Mensal** | ~40 dias após fim do mês |
| 24364 | Créd. Hab. | **Mensal** | ~25 dias após fim do mês |

**Conclusão:** Nunca buscar dados do **mês atual** para séries mensais!

---

**Status:** 🟡 Aguardando correções  
**Próxima Ação:** Implementar Etapa 2 (corrigir daily_bcb.py)
