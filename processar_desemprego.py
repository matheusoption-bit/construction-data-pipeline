#!/usr/bin/env python3
"""
Script para processar apenas a fonte DESEMPREGO que falhou
"""
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import time
from datetime import datetime

def processar_desemprego():
    """Processa especificamente a fonte de taxa de desemprego"""
    
    # Configuração
    creds_path = "config/google_credentials.json"
    spreadsheet_id = "11-KC18ShMKXZOSbWvHcLHJwz3oDjexGQLb26xm2Wq4w"
    url = "http://www.cbicdados.com.br/media/anexos/tabela_09.B.06_n_595.xls"
    aba_name = "ind_taxa_desemprego"
    
    print("📊 PROCESSANDO TAXA DE DESEMPREGO")
    print("=" * 60)
    
    try:
        # 1. Extrair dados
        print(f"📥 Extraindo dados de: {url}")
        df = pd.read_excel(url, engine='xlrd')
        print(f"✅ Extraídos: {len(df)} registros, {len(df.columns)} colunas")
        
        # Limpar NaN
        df = df.fillna('')
        print(f"🧹 Valores NaN removidos")
        
        # 2. Conectar Google Sheets
        print("🔗 Conectando ao Google Sheets...")
        scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        creds = Credentials.from_service_account_file(creds_path, scopes=scopes)
        gc = gspread.authorize(creds)
        spreadsheet = gc.open_by_key(spreadsheet_id)
        
        # 3. Criar ou limpar aba
        try:
            worksheet = spreadsheet.worksheet(aba_name)
            print(f"🧹 Limpando aba existente '{aba_name}'...")
            worksheet.clear()
        except gspread.exceptions.WorksheetNotFound:
            print(f"➕ Criando nova aba '{aba_name}'...")
            worksheet = spreadsheet.add_worksheet(title=aba_name, rows=1000, cols=20)
        
        # 4. Preparar dados
        all_data = [df.columns.tolist()] + df.values.tolist()
        
        # 5. Upload em lotes
        print(f"📤 Fazendo upload de {len(all_data)} linhas...")
        batch_size = 100
        for i in range(0, len(all_data), batch_size):
            batch = all_data[i:i+batch_size]
            end_idx = min(i+batch_size, len(all_data))
            print(f"   📦 Lote {i//batch_size + 1}: linhas {i+1}-{end_idx}")
            
            worksheet.update(
                f'A{i+1}:Z{end_idx}',
                batch,
                value_input_option='RAW'
            )
            time.sleep(1)  # Rate limiting
        
        print(f"✅ Upload concluído: {len(df)} linhas em '{aba_name}'")
        print()
        print("=" * 60)
        print("🎉 FONTE DESEMPREGO PROCESSADA COM SUCESSO!")
        print("=" * 60)
        
    except Exception as e:
        print(f"❌ Erro: {e}")
        raise

if __name__ == "__main__":
    processar_desemprego()
