#!/usr/bin/env python3
"""
Script para revisar e ajustar abas do Google Sheets para BI profissional
"""
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import numpy as np
from datetime import datetime
import json

# Configuração
CREDS_PATH = "config/google_credentials.json"
SPREADSHEET_ID = "11-KC18ShMKXZOSbWvHcLHJwz3oDjexGQLb26xm2Wq4w"

# Abas para revisar
ABAS_PARA_REVISAR = [
    'cub_on_global',
    'cub_on_global_uf', 
    'cub_des_global',
    'pib_brasil_serie',
    'pib_construcao_civil',
    'inv_construcao_civil',
    'inv_infraestrutura',
    'pib_part_construcao',
    'mat_cimento_consumo',
    'mat_cimento_producao',
    'ind_ipca_consumidor',
    'ind_taxa_selic',
    'dim_metodo_fase2',
    'ind_taxa_desemprego',
    'comparacao_fatores',
    'fatores_empiricos'
]

def conectar_google_sheets():
    """Conecta ao Google Sheets"""
    print("🔗 Conectando ao Google Sheets...")
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    creds = Credentials.from_service_account_file(CREDS_PATH, scopes=scopes)
    gc = gspread.authorize(creds)
    spreadsheet = gc.open_by_key(SPREADSHEET_ID)
    print(f"✅ Conectado: {spreadsheet.title}\n")
    return spreadsheet

def analisar_aba(worksheet, nome_aba):
    """Analisa uma aba e retorna diagnóstico"""
    print(f"{'='*70}")
    print(f"📊 ANALISANDO: {nome_aba}")
    print(f"{'='*70}")
    
    # Obter dados
    dados = worksheet.get_all_values()
    
    if not dados or len(dados) <= 1:
        return {
            'nome': nome_aba,
            'status': 'VAZIA',
            'problemas': ['Aba vazia ou apenas com header'],
            'df': None
        }
    
    # Criar DataFrame
    headers = dados[0]
    rows = dados[1:]
    df = pd.DataFrame(rows, columns=headers)
    
    # Análise
    diagnostico = {
        'nome': nome_aba,
        'linhas': len(df),
        'colunas': len(df.columns),
        'colunas_lista': list(df.columns),
        'problemas': [],
        'sugestoes': [],
        'df': df
    }
    
    print(f"📏 Dimensões: {len(df)} linhas × {len(df.columns)} colunas")
    print(f"📋 Colunas: {', '.join(df.columns[:5])}{'...' if len(df.columns) > 5 else ''}")
    
    # Verificar valores vazios
    vazios_por_coluna = df.apply(lambda x: (x == '').sum())
    colunas_com_vazios = vazios_por_coluna[vazios_por_coluna > 0]
    
    if len(colunas_com_vazios) > 0:
        diagnostico['problemas'].append(f"Valores vazios em {len(colunas_com_vazios)} colunas")
        print(f"⚠️  Valores vazios encontrados em {len(colunas_com_vazios)} colunas")
    
    # Verificar colunas duplicadas
    colunas_duplicadas = df.columns[df.columns.duplicated()].tolist()
    if colunas_duplicadas:
        diagnostico['problemas'].append(f"Colunas duplicadas: {colunas_duplicadas}")
        print(f"❌ Colunas duplicadas: {colunas_duplicadas}")
    
    # Verificar se há colunas de data
    colunas_possiveis_data = [col for col in df.columns if any(
        termo in col.lower() for termo in ['data', 'date', 'mes', 'ano', 'trimestre', 'período']
    )]
    
    if colunas_possiveis_data:
        print(f"📅 Colunas de data detectadas: {', '.join(colunas_possiveis_data)}")
        diagnostico['sugestoes'].append(f"Padronizar formato de datas: {', '.join(colunas_possiveis_data)}")
    
    # Verificar colunas numéricas
    for col in df.columns:
        # Tentar converter para numérico
        try:
            valores_numericos = pd.to_numeric(df[col], errors='coerce')
            nao_nulos = valores_numericos.notna().sum()
            
            if nao_nulos > len(df) * 0.5:  # Se mais de 50% são números
                print(f"   🔢 {col}: coluna numérica ({nao_nulos}/{len(df)} valores)")
        except:
            pass
    
    print()
    return diagnostico

def gerar_relatorio_completo(diagnosticos):
    """Gera relatório completo da análise"""
    print(f"\n{'='*70}")
    print("📊 RELATÓRIO COMPLETO DE ANÁLISE")
    print(f"{'='*70}\n")
    
    relatorio = {
        'data_analise': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'total_abas': len(diagnosticos),
        'abas_analisadas': []
    }
    
    for diag in diagnosticos:
        print(f"📄 {diag['nome']}")
        print(f"   Dimensões: {diag['linhas']} linhas × {diag['colunas']} colunas")
        
        if diag['problemas']:
            print(f"   ⚠️  Problemas: {len(diag['problemas'])}")
            for prob in diag['problemas']:
                print(f"      - {prob}")
        else:
            print(f"   ✅ Sem problemas críticos")
        
        if diag['sugestoes']:
            print(f"   💡 Sugestões: {len(diag['sugestoes'])}")
            for sug in diag['sugestoes']:
                print(f"      - {sug}")
        
        print()
        
        relatorio['abas_analisadas'].append({
            'nome': diag['nome'],
            'linhas': diag['linhas'],
            'colunas': diag['colunas'],
            'problemas': diag['problemas'],
            'sugestoes': diag['sugestoes']
        })
    
    # Salvar relatório JSON
    with open('configs/relatorio_analise_abas_bi.json', 'w', encoding='utf-8') as f:
        json.dump(relatorio, f, indent=2, ensure_ascii=False)
    
    print(f"{'='*70}")
    print("💾 Relatório salvo: configs/relatorio_analise_abas_bi.json")
    print(f"{'='*70}\n")
    
    return relatorio

def main():
    """Função principal"""
    print("🚀 INICIANDO REVISÃO DE ABAS PARA BI PROFISSIONAL")
    print(f"📅 Data/Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📊 Total de abas: {len(ABAS_PARA_REVISAR)}\n")
    
    try:
        # Conectar
        spreadsheet = conectar_google_sheets()
        
        # Analisar cada aba
        diagnosticos = []
        
        for nome_aba in ABAS_PARA_REVISAR:
            try:
                worksheet = spreadsheet.worksheet(nome_aba)
                diag = analisar_aba(worksheet, nome_aba)
                diagnosticos.append(diag)
            except gspread.exceptions.WorksheetNotFound:
                print(f"❌ Aba '{nome_aba}' não encontrada!")
                diagnosticos.append({
                    'nome': nome_aba,
                    'status': 'NÃO ENCONTRADA',
                    'problemas': ['Aba não existe'],
                    'df': None
                })
            except Exception as e:
                print(f"❌ Erro ao analisar '{nome_aba}': {e}")
        
        # Gerar relatório
        relatorio = gerar_relatorio_completo(diagnosticos)
        
        print("✅ ANÁLISE CONCLUÍDA!")
        
        return diagnosticos
        
    except Exception as e:
        print(f"❌ Erro: {e}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    diagnosticos = main()
