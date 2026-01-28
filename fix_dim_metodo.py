#!/usr/bin/env python3
"""
Script simplificado para atualizar apenas a estrutura da aba dim_metodo
"""
import os
import sys
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

def fix_dim_metodo_structure():
    """Corrige a estrutura da aba dim_metodo para 26 colunas"""
    
    # Configuração
    creds_path = "config/google_credentials.json"
    spreadsheet_id = "11-KC18ShMKXZOSbWvHcLHJwz3oDjexGQLb26xm2Wq4w"
    csv_path = "configs/dim_metodo_v2.csv"
    
    print("🔧 CORRIGINDO ESTRUTURA DA ABA dim_metodo")
    print("=" * 50)
    
    try:
        # 1. Carregar CSV
        print("📊 Carregando CSV...")
        df = pd.read_csv(csv_path)
        print(f"✅ CSV: {len(df)} linhas × {len(df.columns)} colunas")
        
        # 2. Conectar Google Sheets
        print("🔗 Conectando Google Sheets...")
        scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        creds = Credentials.from_service_account_file(creds_path, scopes=scopes)
        gc = gspread.authorize(creds)
        
        spreadsheet = gc.open_by_key(spreadsheet_id)
        sheet = spreadsheet.worksheet("dim_metodo")
        print("✅ Conexão estabelecida")
        
        # 3. Estrutura de 26 colunas (conforme HEADER_DIM_METODO)
        header_26 = [
            "id_metodo", "nome_metodo", "tipo_cub_sinapi",
            "fator_custo_base", "fator_prazo_base", "limitacao_pavimentos", 
            "percentual_material", "percentual_mao_obra", "percentual_admin_equip",
            "tempo_execucao_dias_padrao", "custo_inicial_m2_sudeste", "data_atualizacao_cub",
            "fonte_primaria", "fonte_secundaria", "status_validacao",
            "aplicavel_residencial", "aplicavel_comercial", "aplicavel_industrial",
            "mao_obra_especializada_requerida", "tamanho_projeto_minimo_m2", 
            "faixa_altura_pavimentos_recom", "url_referencia", "nota_importante",
            "validado_por", "data_criacao", "data_atualizacao"
        ]
        
        print(f"📋 Header de 26 colunas preparado")
        
        # 4. Preparar dados alinhados com header
        print("🔄 Alinhando dados...")
        data_rows = []
        
        for idx, row in df.iterrows():
            metodo_data = []
            for col in header_26:
                if col in df.columns:
                    metodo_data.append(str(row[col]) if pd.notna(row[col]) else "")
                else:
                    # Valores padrão para colunas faltantes
                    if col == "data_atualizacao":
                        metodo_data.append("2025-11-14")
                    elif col == "data_criacao":
                        metodo_data.append("2025-11-14")
                    elif col == "validado_por":
                        metodo_data.append("Sistema")
                    elif col in ["aplicavel_residencial", "aplicavel_comercial", "aplicavel_industrial"]:
                        metodo_data.append("SIM")
                    elif col == "mao_obra_especializada_requerida":
                        metodo_data.append("NAO")
                    else:
                        metodo_data.append("N/A")
            data_rows.append(metodo_data)
        
        # 5. Atualizar aba
        print("📤 Atualizando aba...")
        
        # Limpar e inserir header
        sheet.clear()
        sheet.update("A1:Z1", [header_26])
        
        # Inserir dados
        if data_rows:
            range_name = f"A2:Z{len(data_rows) + 1}"
            sheet.update(range_name, data_rows)
        
        print(f"✅ Aba atualizada!")
        print(f"📊 Estrutura final: 26 colunas × {len(data_rows)} linhas")
        
        # 6. Verificar resultado
        all_values = sheet.get_all_values()
        print(f"✅ Verificação: {len(all_values)} linhas total na planilha")
        print(f"📋 Colunas na planilha: {len(all_values[0])}")
        
        if len(all_values[0]) == 26 and len(all_values) > 1:
            print("🎉 SUCESSO! Aba dim_metodo atualizada para 26 colunas!")
            return True
        else:
            print("⚠️ Algo deu errado na atualização")
            return False
            
    except Exception as e:
        print(f"❌ Erro: {e}")
        return False

if __name__ == "__main__":
    success = fix_dim_metodo_structure()
    sys.exit(0 if success else 1)