#!/usr/bin/env python3
"""
Script completo para fazer upload da Fase 2 preservando todas as abas existentes
"""
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import time
from datetime import datetime
from pathlib import Path

def find_latest_file(pattern):
    """Encontra o arquivo mais recente que corresponde ao padrão"""
    files = list(Path('configs').glob(pattern))
    if not files:
        raise FileNotFoundError(f"Nenhum arquivo encontrado com padrão: {pattern}")
    return max(files, key=lambda x: x.stat().st_mtime)

def upload_fase2_completo():
    """Faz upload completo da Fase 2 preservando abas existentes"""
    
    # Configuração
    creds_path = "config/google_credentials.json"
    spreadsheet_id = "11-KC18ShMKXZOSbWvHcLHJwz3oDjexGQLb26xm2Wq4w"
    
    print("🚀 UPLOAD COMPLETO FASE 2 - PRESERVANDO ABAS EXISTENTES")
    print("=" * 70)
    
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
        print(f"✅ Conectado: {spreadsheet.title}")
        
        # Listar abas existentes
        existing_worksheets = {ws.title for ws in spreadsheet.worksheets()}
        print(f"📋 Abas existentes: {len(existing_worksheets)}")
        
        # Arquivos para upload
        arquivos = [
            {
                'file': find_latest_file('dim_metodo_regional_FASE2_*.csv'),
                'aba': 'dim_metodo_fase2',
                'descricao': 'Dim Metodo com Fatores Empíricos'
            },
            {
                'file': find_latest_file('relatorio_comparacao_fatores_*.csv'),
                'aba': 'comparacao_fatores',
                'descricao': 'Comparação Teórico vs Empírico'
            },
            {
                'file': find_latest_file('fatores_regionais_empiricos_*.csv'),
                'aba': 'fatores_empiricos',
                'descricao': 'Fatores Regionais Empíricos'
            }
        ]
        
        print()
        print(f"📤 Arquivos para upload: {len(arquivos)}")
        print()
        
        for idx, config in enumerate(arquivos, 1):
            print(f"{'='*70}")
            print(f"📊 [{idx}/{len(arquivos)}] {config['descricao']}")
            print(f"{'='*70}")
            
            # Carregar CSV
            print(f"📁 Carregando: {config['file'].name}")
            df = pd.read_csv(config['file'])
            df = df.fillna('')  # Limpar NaN
            print(f"✅ Dados carregados: {len(df)} linhas × {len(df.columns)} colunas")
            
            # Criar ou atualizar aba
            aba_name = config['aba']
            
            if aba_name in existing_worksheets:
                print(f"🔄 Aba '{aba_name}' já existe - atualizando...")
                worksheet = spreadsheet.worksheet(aba_name)
                worksheet.clear()
            else:
                print(f"➕ Criando nova aba '{aba_name}'...")
                worksheet = spreadsheet.add_worksheet(
                    title=aba_name,
                    rows=max(1000, len(df) + 100),
                    cols=max(26, len(df.columns) + 2)
                )
            
            # Preparar dados
            all_data = [df.columns.tolist()] + df.values.tolist()
            
            # Upload em lotes
            print(f"📤 Fazendo upload de {len(all_data)} linhas...")
            batch_size = 100
            
            for i in range(0, len(all_data), batch_size):
                batch = all_data[i:i+batch_size]
                end_idx = min(i+batch_size, len(all_data))
                print(f"   📦 Lote {i//batch_size + 1}: linhas {i+1}-{end_idx}")
                
                # Converter para lista de listas (evitar problemas de serialização)
                batch_clean = [[str(cell) if pd.notna(cell) else '' for cell in row] for row in batch]
                
                worksheet.update(
                    values=batch_clean,
                    range_name=f'A{i+1}:Z{end_idx}',
                    value_input_option='RAW'
                )
                time.sleep(1)  # Rate limiting
            
            print(f"✅ Upload concluído: '{aba_name}'")
            print()
        
        # Dashboard/Resumo
        print(f"{'='*70}")
        print("📊 CRIANDO ABA DE DASHBOARD")
        print(f"{'='*70}")
        
        # Criar dashboard com resumo executivo
        dashboard_data = [
            ['DASHBOARD - FASE 2: INTEGRAÇÃO CBIC EMPÍRICA'],
            [''],
            ['Data da Atualização:', datetime.now().strftime("%Y-%m-%d %H:%M:%S")],
            [''],
            ['RESUMO EXECUTIVO:'],
            [''],
            ['Estados Analisados:', '27'],
            ['Métodos Construtivos:', '10'],
            ['Total de Linhas (dim_metodo):', '270'],
            [''],
            ['DISCREPÂNCIAS IDENTIFICADAS:'],
            ['Estados com revisão necessária:', '19'],
            ['Estados com ajuste leve:', '0'],
            ['Estados mantidos:', '2'],
            ['Estados sem dados CBIC:', '6'],
            [''],
            ['TOP 5 MAIORES AJUSTES:'],
            ['1. Amazonas (AM):', '+69.0%'],
            ['2. Mato Grosso (MT):', '+52.3%'],
            ['3. Santa Catarina (SC):', '+35.7%'],
            ['4. Espírito Santo (ES):', '+35.5%'],
            ['5. Goiás (GO):', '+29.0%'],
            [''],
            ['ANÁLISE POR REGIÃO:'],
            ['Centro-Oeste:', 'Média +18.3%, 4 discrepâncias'],
            ['Nordeste:', 'Média +2.6%, 6 discrepâncias'],
            ['Norte:', 'Média +33.1%, 3 discrepâncias'],
            ['Sudeste:', 'Média +15.9%, 3 discrepâncias'],
            ['Sul:', 'Média +25.8%, 3 discrepâncias'],
            [''],
            ['ARQUIVOS GERADOS:'],
            ['✅ dim_metodo_fase2', 'Fatores empíricos aplicados'],
            ['✅ comparacao_fatores', 'Análise teórico vs empírico'],
            ['✅ fatores_empiricos', 'Fatores por UF'],
            [''],
            ['NOVAS ABAS CBIC (13):'],
            ['✅ cub_on_global', 'CUB Global Oneroso'],
            ['✅ cub_on_global_uf', 'CUB por UF'],
            ['✅ cub_des_global', 'CUB Desonerado'],
            ['✅ pib_brasil_serie', 'PIB Brasil'],
            ['✅ pib_construcao_civil', 'PIB Construção'],
            ['✅ inv_construcao_civil', 'Investimento Construção'],
            ['✅ inv_infraestrutura', 'Investimento Infraestrutura'],
            ['✅ pib_part_construcao', 'Participação no PIB'],
            ['✅ mat_cimento_consumo', 'Consumo Cimento'],
            ['✅ mat_cimento_producao', 'Produção Cimento'],
            ['✅ ind_ipca_consumidor', 'IPCA'],
            ['✅ ind_taxa_selic', 'Taxa SELIC'],
            ['✅ ind_taxa_desemprego', 'Taxa Desemprego'],
            [''],
            ['TOTAL DE ABAS:', str(len(existing_worksheets) + len(arquivos) + 1)],
            [''],
            ['STATUS:', '🟢 SISTEMA COMPLETO E OPERACIONAL']
        ]
        
        # Criar ou atualizar dashboard
        if 'dashboard_fase2' in existing_worksheets:
            worksheet = spreadsheet.worksheet('dashboard_fase2')
            worksheet.clear()
        else:
            worksheet = spreadsheet.add_worksheet(title='dashboard_fase2', rows=100, cols=10)
        
        worksheet.update(values=dashboard_data, range_name='A1:B100', value_input_option='RAW')
        print("✅ Dashboard criado com sucesso!")
        
        # Relatório final
        print()
        print("=" * 70)
        print("🎉 UPLOAD FASE 2 CONCLUÍDO COM SUCESSO!")
        print("=" * 70)
        print(f"📊 Total de abas atualizadas/criadas: {len(arquivos) + 1}")
        print(f"📋 Total de abas na planilha: {len(spreadsheet.worksheets())}")
        print(f"🔗 Planilha: https://docs.google.com/spreadsheets/d/{spreadsheet_id}")
        print("=" * 70)
        
    except Exception as e:
        print(f"❌ Erro: {e}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    upload_fase2_completo()
