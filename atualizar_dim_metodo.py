#!/usr/bin/env python3
"""
Script para atualizar a aba dim_metodo original com a versão LATEST
"""
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import time

def atualizar_dim_metodo_original():
    """Atualiza a aba dim_metodo com a versão LATEST"""
    
    # Configuração
    creds_path = "config/google_credentials.json"
    spreadsheet_id = "11-KC18ShMKXZOSbWvHcLHJwz3oDjexGQLb26xm2Wq4w"
    csv_path = "configs/dim_metodo_regional_completo_LATEST.csv"
    aba_name = "dim_metodo"
    
    print("🔄 ATUALIZANDO ABA dim_metodo COM VERSÃO LATEST")
    print("=" * 70)
    
    try:
        # Carregar CSV
        print(f"📁 Carregando: {csv_path}")
        df = pd.read_csv(csv_path)
        df = df.fillna('')
        print(f"✅ Dados carregados: {len(df)} linhas × {len(df.columns)} colunas")
        print(f"   📍 {df['uf'].nunique()} UF × {df['id_metodo'].nunique()} métodos")
        
        # Conectar Google Sheets
        print("🔗 Conectando ao Google Sheets...")
        scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        creds = Credentials.from_service_account_file(creds_path, scopes=scopes)
        gc = gspread.authorize(creds)
        spreadsheet = gc.open_by_key(spreadsheet_id)
        
        # Atualizar aba
        print(f"🔄 Atualizando aba '{aba_name}'...")
        worksheet = spreadsheet.worksheet(aba_name)
        worksheet.clear()
        
        # Preparar dados
        all_data = [df.columns.tolist()] + df.values.tolist()
        
        # Converter para lista limpa
        all_data_clean = [[str(cell) if pd.notna(cell) else '' for cell in row] for row in all_data]
        
        # Upload em lotes
        print(f"📤 Fazendo upload de {len(all_data_clean)} linhas...")
        batch_size = 100
        
        for i in range(0, len(all_data_clean), batch_size):
            batch = all_data_clean[i:i+batch_size]
            end_idx = min(i+batch_size, len(all_data_clean))
            print(f"   📦 Lote {i//batch_size + 1}: linhas {i+1}-{end_idx}")
            
            worksheet.update(
                values=batch,
                range_name=f'A{i+1}:Z{end_idx}',
                value_input_option='RAW'
            )
            time.sleep(1)
        
        print(f"✅ Aba '{aba_name}' atualizada com sucesso!")
        print()
        print("=" * 70)
        print("🎉 ATUALIZAÇÃO CONCLUÍDA!")
        print("=" * 70)
        print(f"📊 Linhas atualizadas: {len(df)}")
        print(f"📋 Colunas: {len(df.columns)}")
        print(f"🔗 Planilha: https://docs.google.com/spreadsheets/d/{spreadsheet_id}")
        print("=" * 70)
        
    except Exception as e:
        print(f"❌ Erro: {e}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    atualizar_dim_metodo_original()
