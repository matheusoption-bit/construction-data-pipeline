"""
Update missing areas for Pescaria Brava and Balneário Rincão
Dados oficiais: https://cidades.ibge.gov.br/
"""
import gspread
from google.oauth2.service_account import Credentials
import structlog

# Logger
log = structlog.get_logger()

# Dados manuais verificados no IBGE Cidades
AREAS_MANUAIS = {
    "4212650": 218.330,  # Pescaria Brava - https://cidades.ibge.gov.br/brasil/sc/pescaria-brava/panorama
    "4220000": 150.028,  # Balneário Rincão - https://cidades.ibge.gov.br/brasil/sc/balneario-rincao/panorama
}

def update_missing_areas():
    """Atualiza áreas faltantes no dim_geo"""
    print("\n" + "="*70)
    print("📊 UPDATE ÁREAS FALTANTES - Municípios Novos SC")
    print("="*70 + "\n")
    
    # Conectar ao Google Sheets
    print("🔍 Conectando ao Google Sheets...")
    creds = Credentials.from_service_account_file(
        "config/google_credentials.json",
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    client = gspread.authorize(creds)
    
    # Abrir planilha
    spreadsheet = client.open_by_key("11-KC18ShMKXZOSbWvHcLHJwz3oDjexGQLb26xm2Wq4w")
    worksheet = spreadsheet.worksheet("dim_geo")
    print("   ✅ Conectado!\n")
    
    # Buscar todos os registros
    records = worksheet.get_all_records()
    
    # Preparar updates
    updates = []
    for idx, record in enumerate(records, start=2):  # Linha 2 = primeira linha de dados
        cod_ibge = str(record.get("cod_ibge", ""))
        nome = record.get("nome_municipio", "")
        
        if cod_ibge in AREAS_MANUAIS:
            area = AREAS_MANUAIS[cod_ibge]
            updates.append({
                "range": f"G{idx}",  # Coluna G = area_km2
                "values": [[area]]
            })
            print(f"   ✓ {nome} ({cod_ibge}): {area} km²")
    
    if updates:
        print(f"\n💾 Aplicando {len(updates)} atualizações...")
        worksheet.batch_update(updates)
        log.info("updates_applied", count=len(updates))
        print("   ✅ Atualizações concluídas!\n")
    else:
        print("⚠️  Nenhuma atualização necessária.\n")
    
    print("="*70)
    print("✅ Processo finalizado!")
    print("="*70 + "\n")

if __name__ == "__main__":
    update_missing_areas()
