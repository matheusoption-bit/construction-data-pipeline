"""
Script para verificar e validar dados de municípios em dim_geo.

Verifica:
- Municípios com área = 0 ou vazia
- Municípios com população = 0 ou vazia  
- Consistência dos dados

Autor: Sistema de ETL - Construction Data Pipeline
Data: 2025-11-13
"""

import os
import sys
from dotenv import load_dotenv
import gspread
from google.oauth2.service_account import Credentials

# Configurar encoding para Windows
if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# Carregar variáveis de ambiente
load_dotenv()


def main():
    print("═" * 70)
    print("🔍 VERIFICAÇÃO DE DADOS - dim_geo")
    print("═" * 70)
    
    # Conectar ao Google Sheets
    print("\n📡 Conectando ao Google Sheets...")
    
    creds_path = os.getenv("GOOGLE_SHEETS_CREDENTIALS_PATH")
    sheet_id = os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID")
    
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
    ]
    
    credentials = Credentials.from_service_account_file(creds_path, scopes=scopes)
    client = gspread.authorize(credentials)
    spreadsheet = client.open_by_key(sheet_id)
    worksheet = spreadsheet.worksheet("dim_geo")
    
    print("   ✅ Conectado!\n")
    
    # Buscar dados
    all_data = worksheet.get_all_values()
    header = all_data[0]
    rows = all_data[1:]
    
    # Índices
    idx_cod_ibge = header.index("cod_ibge")
    idx_nome = header.index("nome_municipio")
    idx_pop = header.index("populacao_2022")
    idx_area = header.index("area_km2")
    
    print(f"📊 Total de municípios: {len(rows)}\n")
    
    # Estatísticas
    sem_area = []
    sem_populacao = []
    area_zero = []
    pop_zero = []
    
    # Analisar
    for row in rows:
        cod_ibge = row[idx_cod_ibge]
        nome = row[idx_nome]
        pop = row[idx_pop]
        area = row[idx_area]
        
        # Verificar área
        if not area or area.strip() == "":
            sem_area.append((cod_ibge, nome))
        elif area in ("0", "0.0", "0.000"):
            area_zero.append((cod_ibge, nome))
        
        # Verificar população
        if not pop or pop.strip() == "":
            sem_populacao.append((cod_ibge, nome))
        elif pop == "0":
            pop_zero.append((cod_ibge, nome))
    
    # Relatório
    print("━" * 70)
    print("\n📈 RELATÓRIO DE VALIDAÇÃO:\n")
    
    print(f"✅ Municípios com área preenchida: {len(rows) - len(sem_area) - len(area_zero)}")
    print(f"✅ Municípios com população preenchida: {len(rows) - len(sem_populacao) - len(pop_zero)}")
    
    if sem_area:
        print(f"\n⚠️  Municípios SEM área: {len(sem_area)}")
        for cod, nome in sem_area[:5]:
            print(f"   • {nome} ({cod})")
        if len(sem_area) > 5:
            print(f"   ... e mais {len(sem_area) - 5}")
    
    if area_zero:
        print(f"\n⚠️  Municípios com área = 0: {len(area_zero)}")
        for cod, nome in area_zero[:5]:
            print(f"   • {nome} ({cod})")
        if len(area_zero) > 5:
            print(f"   ... e mais {len(area_zero) - 5}")
    
    if sem_populacao:
        print(f"\n⚠️  Municípios SEM população: {len(sem_populacao)}")
        for cod, nome in sem_populacao[:5]:
            print(f"   • {nome} ({cod})")
        if len(sem_populacao) > 5:
            print(f"   ... e mais {len(sem_populacao) - 5}")
    
    if pop_zero:
        print(f"\n⚠️  Municípios com população = 0: {len(pop_zero)}")
        for cod, nome in pop_zero[:5]:
            print(f"   • {nome} ({cod})")
        if len(pop_zero) > 5:
            print(f"   ... e mais {len(pop_zero) - 5}")
    
    # Estatísticas gerais
    print("\n" + "━" * 70)
    print("\n📊 ESTATÍSTICAS GERAIS:\n")
    
    # Calcular totais
    populacao_total = 0
    area_total = 0.0
    
    for row in rows:
        try:
            pop = int(row[idx_pop]) if row[idx_pop] else 0
            populacao_total += pop
        except ValueError:
            pass
        
        try:
            area = float(row[idx_area].replace(",", ".")) if row[idx_area] else 0.0
            area_total += area
        except ValueError:
            pass
    
    print(f"   População total SC (Censo 2022): {populacao_total:,} habitantes")
    print(f"   Área total SC: {area_total:,.2f} km²")
    
    # Maiores municípios
    print("\n🏆 TOP 5 MUNICÍPIOS POR POPULAÇÃO:")
    rows_com_pop = [(row[idx_nome], int(row[idx_pop]) if row[idx_pop] else 0) for row in rows]
    rows_com_pop.sort(key=lambda x: x[1], reverse=True)
    
    for i, (nome, pop) in enumerate(rows_com_pop[:5], 1):
        print(f"   {i}. {nome}: {pop:,} hab")
    
    print("\n🏆 TOP 5 MUNICÍPIOS POR ÁREA:")
    rows_com_area = []
    for row in rows:
        try:
            area = float(row[idx_area].replace(",", ".")) if row[idx_area] else 0.0
            rows_com_area.append((row[idx_nome], area))
        except ValueError:
            rows_com_area.append((row[idx_nome], 0.0))
    
    rows_com_area.sort(key=lambda x: x[1], reverse=True)
    
    for i, (nome, area) in enumerate(rows_com_area[:5], 1):
        print(f"   {i}. {nome}: {area:,.2f} km²")
    
    # Conclusão
    print("\n" + "═" * 70)
    
    if not sem_area and not area_zero and not sem_populacao and not pop_zero:
        print("\n✅ VALIDAÇÃO COMPLETA: Todos os dados estão preenchidos!")
    else:
        print("\n⚠️  ATENÇÃO: Existem campos vazios ou zerados que precisam ser corrigidos.")
    
    print("\n" + "═" * 70)


if __name__ == "__main__":
    main()
