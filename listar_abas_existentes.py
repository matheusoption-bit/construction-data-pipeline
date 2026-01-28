#!/usr/bin/env python3
"""
Script para listar todas as abas existentes no Google Sheets
"""
import gspread
from google.oauth2.service_account import Credentials

def listar_abas_existentes():
    """Lista todas as abas do Google Sheets sem modificar nada"""
    
    # Configuração
    creds_path = "config/google_credentials.json"
    spreadsheet_id = "11-KC18ShMKXZOSbWvHcLHJwz3oDjexGQLb26xm2Wq4w"
    
    print("📋 LISTANDO ABAS EXISTENTES NO GOOGLE SHEETS")
    print("=" * 60)
    
    try:
        # Conectar Google Sheets
        print("🔗 Conectando ao Google Sheets...")
        scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        creds = Credentials.from_service_account_file(creds_path, scopes=scopes)
        gc = gspread.authorize(creds)
        
        spreadsheet = gc.open_by_key(spreadsheet_id)
        print(f"✅ Conectado à planilha: {spreadsheet.title}")
        print()
        
        # Listar todas as abas
        worksheets = spreadsheet.worksheets()
        
        print(f"📊 TOTAL DE ABAS: {len(worksheets)}")
        print()
        
        for idx, ws in enumerate(worksheets, 1):
            print(f"{idx:2d}. 📄 {ws.title:<35} | {ws.row_count:>6} linhas × {ws.col_count:>3} colunas")
        
        print()
        print("=" * 60)
        print("✅ Listagem concluída!")
        
        return [ws.title for ws in worksheets]
        
    except Exception as e:
        print(f"❌ Erro: {e}")
        raise

if __name__ == "__main__":
    abas = listar_abas_existentes()
    print(f"\n📋 Abas para preservar: {', '.join(abas)}")
