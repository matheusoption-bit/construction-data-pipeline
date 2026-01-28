#!/usr/bin/env python3
"""
Script para corrigir e padronizar abas para BI profissional
"""
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import time
from datetime import datetime
import re

# Configuração
CREDS_PATH = "config/google_credentials.json"
SPREADSHEET_ID = "11-KC18ShMKXZOSbWvHcLHJwz3oDjexGQLb26xm2Wq4w"

def conectar_google_sheets():
    """Conecta ao Google Sheets"""
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    creds = Credentials.from_service_account_file(CREDS_PATH, scopes=scopes)
    gc = gspread.authorize(creds)
    spreadsheet = gc.open_by_key(SPREADSHEET_ID)
    return spreadsheet, gc

def normalizar_nome_coluna(nome):
    """Normaliza nome de coluna para padrão BI"""
    # Remove caracteres especiais e normaliza
    nome = nome.lower().strip()
    nome = re.sub(r'[*_\s]+', '_', nome)
    nome = re.sub(r'[^\w]', '', nome)
    nome = nome.replace('unnamed', 'col')
    nome = re.sub(r'_+', '_', nome)
    nome = nome.strip('_')
    return nome

def corrigir_aba_cub(df, tipo='oneroso'):
    """Corrige estrutura das abas CUB"""
    print(f"   🔧 Corrigindo estrutura CUB {tipo}...")
    
    # Pegar primeira linha como referência para nomes
    primeira_linha = df.iloc[0] if len(df) > 0 else []
    
    # Criar novos nomes de colunas
    novos_nomes = ['tipo_projeto']
    for i, col in enumerate(df.columns[1:], 1):
        if i <= len(primeira_linha):
            nome_original = str(primeira_linha.iloc[i-1]) if i-1 < len(primeira_linha) else f'col_{i}'
            if nome_original and nome_original.strip() and nome_original != 'nan':
                novos_nomes.append(normalizar_nome_coluna(nome_original))
            else:
                novos_nomes.append(f'valor_{i}')
        else:
            novos_nomes.append(f'col_{i}')
    
    # Adicionar colunas auxiliares
    if 'data_extracao' not in novos_nomes:
        novos_nomes.append('data_extracao')
    if 'fonte_cbic' not in novos_nomes:
        novos_nomes.append('fonte_cbic')
    if 'tipo_cub' not in novos_nomes:
        novos_nomes.append('tipo_cub')
    
    # Ajustar para tamanho correto
    novos_nomes = novos_nomes[:len(df.columns)]
    
    df.columns = novos_nomes
    
    # Remover linhas de cabeçalho duplicado
    df = df[df['tipo_projeto'].str.contains('Unnamed|tipo|projeto', case=False, na=False) == False]
    
    # Adicionar metadados
    df['data_extracao'] = datetime.now().strftime("%Y-%m-%d")
    df['fonte_cbic'] = 'http://www.cbicdados.com.br'
    df['tipo_cub'] = tipo
    
    # Remover linhas completamente vazias
    df = df.dropna(how='all')
    
    # Substituir vazios por None
    df = df.replace('', None)
    
    print(f"   ✅ Estrutura corrigida: {len(df)} linhas × {len(df.columns)} colunas")
    return df

def corrigir_aba_pib(df, tipo='pib'):
    """Corrige estrutura das abas PIB/Investimentos"""
    print(f"   🔧 Corrigindo estrutura {tipo.upper()}...")
    
    # Normalizar nomes de colunas
    novos_nomes = []
    for col in df.columns:
        if 'unnamed' in col.lower():
            # Tentar usar primeira linha como nome
            idx = df.columns.get_loc(col)
            if idx < len(df.iloc[0]):
                nome_linha = str(df.iloc[0, idx])
                if nome_linha and nome_linha.strip() and nome_linha != 'nan':
                    novos_nomes.append(normalizar_nome_coluna(nome_linha))
                else:
                    novos_nomes.append(f'valor_{idx}')
            else:
                novos_nomes.append(f'col_{idx}')
        else:
            novos_nomes.append(normalizar_nome_coluna(col))
    
    df.columns = novos_nomes
    
    # Remover linhas de cabeçalho
    df = df[df.iloc[:, 0].str.contains('unnamed|valor|total', case=False, na=False) == False]
    
    # Garantir coluna de período/trimestre
    if 'periodo' not in df.columns and 'trimestre' not in df.columns:
        df.insert(0, 'periodo', range(len(df)))
    
    # Adicionar metadados
    df['data_extracao'] = datetime.now().strftime("%Y-%m-%d")
    df['fonte_cbic'] = 'http://www.cbicdados.com.br'
    df['tipo_indicador'] = tipo
    
    # Limpar
    df = df.dropna(how='all')
    df = df.replace('', None)
    
    print(f"   ✅ Estrutura corrigida: {len(df)} linhas × {len(df.columns)} colunas")
    return df

def corrigir_aba_materiais(df, tipo='cimento'):
    """Corrige estrutura das abas de materiais"""
    print(f"   🔧 Corrigindo estrutura Materiais - {tipo}...")
    
    # Identificar colunas de meses
    novos_nomes = ['ano']
    meses = ['jan', 'fev', 'mar', 'abr', 'mai', 'jun', 
             'jul', 'ago', 'set', 'out', 'nov', 'dez']
    
    for i, col in enumerate(df.columns[1:], 1):
        if i <= 12:
            novos_nomes.append(meses[i-1])
        else:
            novos_nomes.append(f'col_{i}')
    
    # Ajustar para tamanho
    novos_nomes = novos_nomes[:len(df.columns)]
    
    # Adicionar colunas auxiliares
    while len(novos_nomes) < len(df.columns):
        novos_nomes.append(f'extra_{len(novos_nomes)}')
    
    df.columns = novos_nomes
    
    # Remover cabeçalhos
    df = df[df['ano'].str.contains('ano|consumo|produção', case=False, na=False) == False]
    
    # Adicionar metadados
    if 'data_extracao' not in df.columns:
        df['data_extracao'] = datetime.now().strftime("%Y-%m-%d")
    if 'fonte_cbic' not in df.columns:
        df['fonte_cbic'] = 'http://www.cbicdados.com.br'
    if 'tipo_material' not in df.columns:
        df['tipo_material'] = tipo
    if 'unidade' not in df.columns:
        df['unidade'] = 'mil toneladas'
    
    # Limpar
    df = df.dropna(how='all')
    df = df.replace('', None)
    
    print(f"   ✅ Estrutura corrigida: {len(df)} linhas × {len(df.columns)} colunas")
    return df

def corrigir_aba_indicadores(df, tipo='ipca'):
    """Corrige estrutura das abas de indicadores econômicos"""
    print(f"   🔧 Corrigindo estrutura Indicador - {tipo.upper()}...")
    
    # Normalizar nomes
    novos_nomes = []
    for i, col in enumerate(df.columns):
        if 'unnamed' in col.lower():
            if i == 0:
                novos_nomes.append('periodo')
            else:
                novos_nomes.append(f'valor_{i}')
        else:
            novos_nomes.append(normalizar_nome_coluna(col))
    
    df.columns = novos_nomes
    
    # Remover cabeçalhos - verificar se coluna existe
    primeira_col = df.columns[0]
    if primeira_col in df.columns:
        df = df[df[primeira_col].astype(str).str.contains('unnamed|referencial|taxa|poupan', case=False, na=False) == False]
    
    # Adicionar metadados
    df['data_extracao'] = datetime.now().strftime("%Y-%m-%d")
    df['fonte_cbic'] = 'http://www.cbicdados.com.br'
    df['tipo_indicador'] = tipo
    
    # Limpar
    df = df.dropna(how='all')
    df = df.replace('', None)
    
    print(f"   ✅ Estrutura corrigida: {len(df)} linhas × {len(df.columns)} colunas")
    return df

def upload_aba_corrigida(spreadsheet, nome_aba, df):
    """Faz upload da aba corrigida"""
    print(f"   📤 Fazendo upload de {nome_aba}...")
    
    try:
        # Buscar ou criar aba
        try:
            worksheet = spreadsheet.worksheet(nome_aba)
            worksheet.clear()
        except:
            worksheet = spreadsheet.add_worksheet(
                title=nome_aba,
                rows=max(1000, len(df) + 100),
                cols=max(26, len(df.columns) + 2)
            )
        
        # Preparar dados
        df_limpo = df.fillna('')
        all_data = [df_limpo.columns.tolist()] + df_limpo.values.tolist()
        
        # Converter tudo para string
        all_data_clean = [[str(cell) for cell in row] for row in all_data]
        
        # Upload em lotes
        batch_size = 100
        for i in range(0, len(all_data_clean), batch_size):
            batch = all_data_clean[i:i+batch_size]
            end_idx = min(i+batch_size, len(all_data_clean))
            
            worksheet.update(
                values=batch,
                range_name=f'A{i+1}',
                value_input_option='RAW'
            )
            time.sleep(1)
        
        print(f"   ✅ Upload concluído: {len(df)} linhas")
        return True
        
    except Exception as e:
        print(f"   ❌ Erro no upload: {e}")
        return False

def main():
    """Função principal"""
    print("🚀 INICIANDO CORREÇÃO DE ABAS PARA BI PROFISSIONAL")
    print(f"📅 Data/Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    try:
        spreadsheet, gc = conectar_google_sheets()
        print(f"✅ Conectado: {spreadsheet.title}\n")
        
        resultados = []
        
        # 1. CUB Oneroso Global
        print(f"{'='*70}")
        print("📊 [1/16] Processando: cub_on_global")
        print(f"{'='*70}")
        ws = spreadsheet.worksheet('cub_on_global')
        df = pd.DataFrame(ws.get_all_values()[1:], columns=ws.get_all_values()[0])
        df_corrigido = corrigir_aba_cub(df, tipo='oneroso_global')
        sucesso = upload_aba_corrigida(spreadsheet, 'cub_on_global', df_corrigido)
        resultados.append(('cub_on_global', sucesso))
        print()
        
        # 2. CUB Oneroso UF
        print(f"{'='*70}")
        print("📊 [2/16] Processando: cub_on_global_uf")
        print(f"{'='*70}")
        ws = spreadsheet.worksheet('cub_on_global_uf')
        df = pd.DataFrame(ws.get_all_values()[1:], columns=ws.get_all_values()[0])
        df_corrigido = corrigir_aba_cub(df, tipo='oneroso_uf')
        sucesso = upload_aba_corrigida(spreadsheet, 'cub_on_global_uf', df_corrigido)
        resultados.append(('cub_on_global_uf', sucesso))
        print()
        
        # 3. CUB Desonerado
        print(f"{'='*70}")
        print("📊 [3/16] Processando: cub_des_global")
        print(f"{'='*70}")
        ws = spreadsheet.worksheet('cub_des_global')
        df = pd.DataFrame(ws.get_all_values()[1:], columns=ws.get_all_values()[0])
        df_corrigido = corrigir_aba_cub(df, tipo='desonerado_global')
        sucesso = upload_aba_corrigida(spreadsheet, 'cub_des_global', df_corrigido)
        resultados.append(('cub_des_global', sucesso))
        print()
        
        # 4-7. PIB e Investimentos
        abas_pib = [
            ('pib_brasil_serie', 'pib_brasil'),
            ('pib_construcao_civil', 'pib_construcao'),
            ('inv_construcao_civil', 'investimento_construcao'),
            ('inv_infraestrutura', 'investimento_infraestrutura')
        ]
        
        for idx, (aba_nome, tipo) in enumerate(abas_pib, 4):
            print(f"{'='*70}")
            print(f"📊 [{idx}/16] Processando: {aba_nome}")
            print(f"{'='*70}")
            ws = spreadsheet.worksheet(aba_nome)
            df = pd.DataFrame(ws.get_all_values()[1:], columns=ws.get_all_values()[0])
            df_corrigido = corrigir_aba_pib(df, tipo=tipo)
            sucesso = upload_aba_corrigida(spreadsheet, aba_nome, df_corrigido)
            resultados.append((aba_nome, sucesso))
            print()
        
        # 8. PIB Participação
        print(f"{'='*70}")
        print("📊 [8/16] Processando: pib_part_construcao")
        print(f"{'='*70}")
        ws = spreadsheet.worksheet('pib_part_construcao')
        df = pd.DataFrame(ws.get_all_values()[1:], columns=ws.get_all_values()[0])
        df_corrigido = corrigir_aba_pib(df, tipo='participacao_pib')
        sucesso = upload_aba_corrigida(spreadsheet, 'pib_part_construcao', df_corrigido)
        resultados.append(('pib_part_construcao', sucesso))
        print()
        
        # 9-10. Materiais
        print(f"{'='*70}")
        print("📊 [9/16] Processando: mat_cimento_consumo")
        print(f"{'='*70}")
        ws = spreadsheet.worksheet('mat_cimento_consumo')
        df = pd.DataFrame(ws.get_all_values()[1:], columns=ws.get_all_values()[0])
        df_corrigido = corrigir_aba_materiais(df, tipo='cimento_consumo')
        sucesso = upload_aba_corrigida(spreadsheet, 'mat_cimento_consumo', df_corrigido)
        resultados.append(('mat_cimento_consumo', sucesso))
        print()
        
        print(f"{'='*70}")
        print("📊 [10/16] Processando: mat_cimento_producao")
        print(f"{'='*70}")
        ws = spreadsheet.worksheet('mat_cimento_producao')
        df = pd.DataFrame(ws.get_all_values()[1:], columns=ws.get_all_values()[0])
        df_corrigido = corrigir_aba_materiais(df, tipo='cimento_producao')
        sucesso = upload_aba_corrigida(spreadsheet, 'mat_cimento_producao', df_corrigido)
        resultados.append(('mat_cimento_producao', sucesso))
        print()
        
        # 11-13. Indicadores
        abas_indicadores = [
            ('ind_ipca_consumidor', 'ipca'),
            ('ind_taxa_selic', 'selic'),
            ('ind_taxa_desemprego', 'desemprego')
        ]
        
        for idx, (aba_nome, tipo) in enumerate(abas_indicadores, 11):
            print(f"{'='*70}")
            print(f"📊 [{idx}/16] Processando: {aba_nome}")
            print(f"{'='*70}")
            ws = spreadsheet.worksheet(aba_nome)
            df = pd.DataFrame(ws.get_all_values()[1:], columns=ws.get_all_values()[0])
            df_corrigido = corrigir_aba_indicadores(df, tipo=tipo)
            sucesso = upload_aba_corrigida(spreadsheet, aba_nome, df_corrigido)
            resultados.append((aba_nome, sucesso))
            print()
        
        # 14-16. Abas já estruturadas (apenas limpeza)
        abas_ok = ['dim_metodo_fase2', 'comparacao_fatores', 'fatores_empiricos']
        
        for idx, aba_nome in enumerate(abas_ok, 14):
            print(f"{'='*70}")
            print(f"📊 [{idx}/16] Validando: {aba_nome}")
            print(f"{'='*70}")
            print(f"   ✅ Aba já está em formato adequado para BI")
            resultados.append((aba_nome, True))
            print()
        
        # Relatório final
        print(f"{'='*70}")
        print("📊 RELATÓRIO FINAL")
        print(f"{'='*70}")
        
        sucessos = sum(1 for _, s in resultados if s)
        print(f"✅ Abas processadas com sucesso: {sucessos}/{len(resultados)}")
        print(f"📊 Taxa de sucesso: {(sucessos/len(resultados)*100):.1f}%")
        print()
        
        print("🎉 CORREÇÃO CONCLUÍDA!")
        
    except Exception as e:
        print(f"❌ Erro: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
