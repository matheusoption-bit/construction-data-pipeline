#!/usr/bin/env python3
"""
UPLOAD FASE 2 - Google Sheets Integration
=========================================

Upload dos resultados da Fase 2 (fatores empíricos CBIC) para Google Sheets
com formatação avançada e análises visuais.

ABAS CRIADAS:
1. dim_metodo_fase2: Dataset principal com fatores empíricos
2. comparacao_fatores: Análise teórico vs empírico
3. dashboard_insights: Resumo executivo visual
4. fatores_por_regiao: Análise regional agregada

Autor: matheusoption-bit
Data: 2025-11-14
"""

import os
import sys
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
from typing import Dict, List, Any

# Constantes
SPREADSHEET_ID = "11-KC18ShMKXZOSbWvHcLHJwz3oDjexGQLb26xm2Wq4w"
CREDENTIALS_PATH = "config/google_credentials.json"

def setup_google_sheets():
    """Configura conexão com Google Sheets."""
    print("🔗 Conectando ao Google Sheets...")
    
    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    
    creds = Credentials.from_service_account_file(CREDENTIALS_PATH, scopes=scope)
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_key(SPREADSHEET_ID)
    
    print("✅ Conexão estabelecida com sucesso")
    return spreadsheet

def create_or_update_worksheet(spreadsheet, sheet_name: str, data: pd.DataFrame):
    """Cria ou atualiza uma aba no Google Sheets."""
    print(f"📝 Processando aba: {sheet_name}")
    
    try:
        # Tenta acessar aba existente
        worksheet = spreadsheet.worksheet(sheet_name)
        worksheet.clear()
        print(f"🔄 Aba '{sheet_name}' limpa")
    except gspread.WorksheetNotFound:
        # Cria nova aba
        worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=1000, cols=30)
        print(f"➕ Nova aba '{sheet_name}' criada")
    
    # Converte DataFrame para lista de listas
    headers = data.columns.tolist()
    values = [headers] + data.fillna('').astype(str).values.tolist()
    
    # Upload dos dados
    worksheet.update(values=values, range_name="A1")
    
    # Formatação básica
    worksheet.format("A1:Z1", {
        "backgroundColor": {"red": 0.2, "green": 0.4, "blue": 0.8},
        "textFormat": {"foregroundColor": {"red": 1, "green": 1, "blue": 1}, "bold": True}
    })
    
    print(f"✅ Aba '{sheet_name}' atualizada: {len(data)} linhas")
    return worksheet

def format_comparacao_sheet(worksheet, data: pd.DataFrame):
    """Aplica formatação especial na aba de comparação."""
    print("🎨 Aplicando formatação especial na comparação...")
    
    # Encontra colunas relevantes
    discrepancia_col = None
    diferenca_col = None
    recomendacao_col = None
    
    for i, col in enumerate(data.columns, 1):
        col_letter = chr(64 + i) if i <= 26 else f"A{chr(64 + i - 26)}"
        if 'discrepancia_significativa' in col.lower():
            discrepancia_col = col_letter
        elif 'diferenca_percentual' in col.lower():
            diferenca_col = col_letter
        elif 'recomendacao' in col.lower():
            recomendacao_col = col_letter
    
    # Formatação condicional para discrepâncias
    if discrepancia_col:
        try:
            worksheet.format(f"{discrepancia_col}2:{discrepancia_col}{len(data)+1}", {
                "backgroundColor": {"red": 1, "green": 0.8, "blue": 0.8}
            })
        except:
            pass
    
    # Formatação para recomendações
    if recomendacao_col:
        try:
            for i, row in enumerate(data.itertuples(), 2):
                if hasattr(row, 'recomendacao'):
                    if row.recomendacao == "REVISAO_NECESSARIA":
                        worksheet.format(f"{recomendacao_col}{i}", {
                            "backgroundColor": {"red": 1, "green": 0.6, "blue": 0.6}
                        })
                    elif row.recomendacao == "AJUSTE_LEVE":
                        worksheet.format(f"{recomendacao_col}{i}", {
                            "backgroundColor": {"red": 1, "green": 1, "blue": 0.6}
                        })
                    elif row.recomendacao == "MANTER_ATUAL":
                        worksheet.format(f"{recomendacao_col}{i}", {
                            "backgroundColor": {"red": 0.6, "green": 1, "blue": 0.6}
                        })
        except Exception as e:
            print(f"⚠️ Erro na formatação condicional: {e}")

def create_dashboard_data() -> pd.DataFrame:
    """Cria dados para dashboard de insights."""
    print("📊 Criando dados do dashboard...")
    
    # Lê arquivo de comparação mais recente
    import glob
    comparacao_files = glob.glob("configs/relatorio_comparacao_fatores_*.csv")
    
    if not comparacao_files:
        print("⚠️  Arquivo de comparação não encontrado")
        return pd.DataFrame()
    
    latest_file = max(comparacao_files)
    comparacao = pd.read_csv(latest_file)
    
    # Métricas principais
    dashboard_data = []
    
    # Estatísticas gerais
    dashboard_data.append({
        'Métrica': 'Estados Analisados',
        'Valor': len(comparacao),
        'Categoria': 'Geral'
    })
    
    dashboard_data.append({
        'Métrica': 'Discrepâncias Significativas (>5%)',
        'Valor': len(comparacao[comparacao['discrepancia_significativa'] == True]),
        'Categoria': 'Geral'
    })
    
    # Por região
    try:
        regioes_stats = comparacao.groupby('regiao').agg({
            'diferenca_absoluta': 'mean',
            'discrepancia_significativa': 'sum'
        }).round(3)
        
        for regiao, stats in regioes_stats.iterrows():
            dashboard_data.append({
                'Métrica': f'{regiao} - Diferença Média',
                'Valor': f"{stats['diferenca_absoluta']:+.3f}",
                'Categoria': 'Regional'
            })
            
            dashboard_data.append({
                'Métrica': f'{regiao} - Discrepâncias',
                'Valor': int(stats['discrepancia_significativa']),
                'Categoria': 'Regional'
            })
    except Exception as e:
        print(f"⚠️ Erro no processamento regional: {e}")
    
    # Top 5 maiores discrepâncias
    try:
        top_discrepancias = comparacao.nlargest(5, 'diferenca_absoluta', key=abs)
        for i, (_, row) in enumerate(top_discrepancias.iterrows(), 1):
            dashboard_data.append({
                'Métrica': f'Top {i} Discrepância - {row["uf"]}',
                'Valor': f"{row['diferenca_percentual']:+.1f}%",
                'Categoria': 'Top Discrepâncias'
            })
    except Exception as e:
        print(f"⚠️ Erro no top discrepâncias: {e}")
    
    return pd.DataFrame(dashboard_data)

def create_regional_analysis() -> pd.DataFrame:
    """Cria análise agregada por região."""
    print("🗺️  Criando análise regional...")
    
    import glob
    comparacao_files = glob.glob("configs/relatorio_comparacao_fatores_*.csv")
    
    if not comparacao_files:
        return pd.DataFrame()
    
    latest_file = max(comparacao_files)
    comparacao = pd.read_csv(latest_file)
    
    try:
        # Análise por região
        regional = comparacao.groupby('regiao').agg({
            'fator_regional_teorico': ['mean', 'min', 'max'],
            'fator_regional_real': ['mean', 'min', 'max'],
            'diferenca_absoluta': ['mean', 'std'],
            'diferenca_percentual': ['mean', 'std'],
            'discrepancia_significativa': 'sum',
            'uf': 'count'
        }).round(4)
        
        # Achatar colunas multi-level
        regional.columns = [
            'Fator_Teorico_Medio', 'Fator_Teorico_Min', 'Fator_Teorico_Max',
            'Fator_Real_Medio', 'Fator_Real_Min', 'Fator_Real_Max',
            'Diferenca_Absoluta_Media', 'Diferenca_Absoluta_StdDev',
            'Diferenca_Percentual_Media', 'Diferenca_Percentual_StdDev',
            'Discrepancias_Significativas', 'Num_Estados'
        ]
        
        regional = regional.reset_index()
        
        # Adiciona classificação de precisão
        def classificar_regiao(row):
            if row['Diferenca_Absoluta_Media'] <= 0.02:
                return "ALTA_PRECISAO"
            elif row['Diferenca_Absoluta_Media'] <= 0.05:
                return "PRECISAO_MEDIA"
            else:
                return "BAIXA_PRECISAO"
        
        regional['Classificacao_Precisao'] = regional.apply(classificar_regiao, axis=1)
        
        return regional
        
    except Exception as e:
        print(f"⚠️ Erro na análise regional: {e}")
        return pd.DataFrame()

def main():
    """Função principal do upload Fase 2."""
    print("🚀 INICIANDO UPLOAD FASE 2 PARA GOOGLE SHEETS")
    
    try:
        # Setup Google Sheets
        spreadsheet = setup_google_sheets()
        
        # Busca arquivos mais recentes da Fase 2
        import glob
        
        # 1. Dataset principal (dim_metodo atualizado)
        dim_metodo_files = glob.glob("configs/dim_metodo_regional_FASE2_*.csv")
        if dim_metodo_files:
            latest_dim_metodo = max(dim_metodo_files)
            dim_metodo_data = pd.read_csv(latest_dim_metodo)
            worksheet1 = create_or_update_worksheet(spreadsheet, "dim_metodo_fase2", dim_metodo_data)
            print(f"📊 Dim_metodo Fase 2 carregado: {latest_dim_metodo}")
        else:
            print("⚠️ Arquivo dim_metodo_regional_FASE2 não encontrado")
        
        # 2. Comparação fatores
        comparacao_files = glob.glob("configs/relatorio_comparacao_fatores_*.csv")
        if comparacao_files:
            latest_comparacao = max(comparacao_files)
            comparacao_data = pd.read_csv(latest_comparacao)
            worksheet2 = create_or_update_worksheet(spreadsheet, "comparacao_fatores", comparacao_data)
            format_comparacao_sheet(worksheet2, comparacao_data)
            print(f"📋 Comparação carregada: {latest_comparacao}")
        else:
            print("⚠️ Arquivo relatorio_comparacao_fatores não encontrado")
        
        # 3. Dashboard insights
        dashboard_data = create_dashboard_data()
        if not dashboard_data.empty:
            create_or_update_worksheet(spreadsheet, "dashboard_insights", dashboard_data)
        else:
            print("⚠️ Dashboard data vazio - pulando")
        
        # 4. Análise regional
        regional_data = create_regional_analysis()
        if not regional_data.empty:
            create_or_update_worksheet(spreadsheet, "fatores_por_regiao", regional_data)
        else:
            print("⚠️ Análise regional vazia - pulando")
        
        print("🎉 UPLOAD FASE 2 CONCLUÍDO COM SUCESSO!")
        print(f"🔗 Acesse: https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}")
        
    except Exception as e:
        print(f"❌ Erro no upload Fase 2: {str(e)}")
        raise

if __name__ == "__main__":
    main()