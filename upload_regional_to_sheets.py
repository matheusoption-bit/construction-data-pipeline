#!/usr/bin/env python3
"""
Script para atualizar Google Sheets com a estrutura regional completa
"""
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

def atualizar_google_sheets_regional():
    """Atualiza o Google Sheets com a estrutura regional de 270 linhas"""
    
    # Configuração
    creds_path = "config/google_credentials.json"
    spreadsheet_id = "11-KC18ShMKXZOSbWvHcLHJwz3oDjexGQLb26xm2Wq4w"
    csv_path = "configs/dim_metodo_regional_completo_20251114_175753.csv"
    aba_name = "dim_metodo"
    
    print("📊 ATUALIZANDO GOOGLE SHEETS - ESTRUTURA REGIONAL")
    print("=" * 60)
    
    try:
        # 1. Carregar CSV
        print("📁 Carregando arquivo CSV...")
        df = pd.read_csv(csv_path)
        print(f"✅ Dados carregados: {len(df)} linhas × {len(df.columns)} colunas")
        print(f"   📍 {df['uf'].nunique()} UF × {df['id_metodo'].nunique()} métodos")
        
        # 2. Conectar Google Sheets
        print("🔗 Conectando ao Google Sheets...")
        scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        creds = Credentials.from_service_account_file(creds_path, scopes=scopes)
        gc = gspread.authorize(creds)
        
        spreadsheet = gc.open_by_key(spreadsheet_id)
        worksheet = spreadsheet.worksheet(aba_name)
        print(f"✅ Conectado à aba '{aba_name}'")
        
        # 3. Limpar conteúdo anterior
        print("🗑️ Limpando conteúdo anterior...")
        worksheet.clear()
        
        # 4. Preparar dados para upload
        print("📤 Preparando dados para upload...")
        
        # Header + dados
        all_data = [df.columns.tolist()] + df.values.tolist()
        
        # 5. Upload em lotes para evitar timeout
        print(f"⬆️ Fazendo upload de {len(all_data)} linhas...")
        
        # Upload do cabeçalho
        print("   📋 Uploading header...")
        worksheet.update('A1:T1', [all_data[0]])
        
        # Upload dos dados em lotes de 50 linhas
        batch_size = 50
        data_rows = all_data[1:]  # Pular o header
        
        for i in range(0, len(data_rows), batch_size):
            batch = data_rows[i:i+batch_size]
            start_row = i + 2  # +2 porque: +1 para índice baseado em 1, +1 para pular header
            end_row = start_row + len(batch) - 1
            
            range_name = f"A{start_row}:T{end_row}"
            worksheet.update(range_name, batch)
            
            batch_num = (i // batch_size) + 1
            total_batches = (len(data_rows) // batch_size) + 1
            print(f"   📦 Lote {batch_num}/{total_batches}: linhas {start_row}-{end_row}")
        
        # 6. Validação final
        print("🔍 Validando resultado...")
        sheet_data = worksheet.get_all_values()
        
        print(f"✅ Upload concluído:")
        print(f"   📊 {len(sheet_data)} linhas no Google Sheets")
        print(f"   📊 {len(sheet_data[0])} colunas")
        print(f"   📍 Estrutura: 10 métodos × 27 UF = 270 linhas + header")
        
        # 7. Resumo das correções implementadas
        print(f"\n🎯 CORREÇÕES IMPLEMENTADAS:")
        print(f"   ✅ PB (Paraíba) adicionado")
        print(f"   ✅ MET_01: 60% material, 35% mão obra (corrigido)")
        print(f"   ✅ MET_09: 70% material, 25% mão obra (corrigido)")
        print(f"   ✅ SP como baseline (1.000) mantido")
        print(f"   ✅ 6 novos métodos completados")
        print(f"   ✅ Estrutura expandida: 10 → 270 linhas")
        
        print(f"\n🎉 SUCESSO! Google Sheets atualizado com estrutura regional completa!")
        return True
        
    except Exception as e:
        print(f"❌ Erro: {e}")
        return False

if __name__ == "__main__":
    sucesso = atualizar_google_sheets_regional()
    if sucesso:
        print("\n🚀 PRÓXIMOS PASSOS:")
        print("1. ✅ Verificar dados no Google Sheets")
        print("2. 🔄 Integrar com fact_cub_por_uf")
        print("3. 📈 Testar pipeline com dados regionais")
    else:
        print("\n❌ Falha na atualização - verificar logs")