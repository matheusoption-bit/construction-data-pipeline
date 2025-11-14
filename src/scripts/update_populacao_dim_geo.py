"""
Script para atualizar população 2022 e área dos municípios em dim_geo.

Busca dados via API IBGE alternativa (mais estável) e atualiza Google Sheets.

Autor: Sistema de ETL - Construction Data Pipeline
Data: 2025-11-13
"""

import argparse
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List

import gspread
import requests
import structlog
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from tqdm import tqdm

# Configurar logger estruturado
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.add_log_level,
        structlog.processors.JSONRenderer(),
    ]
)
logger = structlog.get_logger()

# Carregar variáveis de ambiente
load_dotenv()


def get_municipio_data_ibge(cod_ibge: str) -> Dict[str, any]:
    """
    Buscar dados completos de um município via API IBGE.
    
    Usa endpoint individual que é mais estável que agregados.
    
    Args:
        cod_ibge: Código IBGE do município (7 dígitos)
        
    Returns:
        Dict com populacao_2022 e area_km2
    """
    # Endpoint mais estável: detalhes do município
    url = f"https://servicodados.ibge.gov.br/api/v1/localidades/municipios/{cod_ibge}"
    
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        
        # Extrair área territorial
        area_km2 = float(data.get("area", {}).get("territorial", 0))
        
        # Para população, usar estimativa 2022 do endpoint de projeções
        pop_url = f"https://servicodados.ibge.gov.br/api/v3/agregados/6579/periodos/2022/variaveis/9324?localidades=N6[{cod_ibge}]"
        
        pop_response = requests.get(pop_url, timeout=10)
        populacao = 0
        
        if pop_response.status_code == 200:
            pop_data = pop_response.json()
            
            if pop_data and len(pop_data) > 0:
                resultados = pop_data[0].get("resultados", [])
                
                if resultados:
                    series = resultados[0].get("series", [])
                    
                    if series:
                        valor_pop = series[0].get("serie", {}).get("2022", "0")
                        
                        # Remover pontos de milhar
                        valor_pop = valor_pop.replace(".", "")
                        populacao = int(valor_pop) if valor_pop.isdigit() else 0
        
        return {
            "populacao_2022": populacao,
            "area_km2": round(area_km2, 3),
        }
        
    except Exception as e:
        logger.warning(
            "failed_to_fetch_municipio_data",
            cod_ibge=cod_ibge,
            error=str(e),
        )
        return {
            "populacao_2022": 0,
            "area_km2": 0.0,
        }


def connect_sheets() -> gspread.Spreadsheet:
    """
    Conectar ao Google Sheets usando service account.

    Returns:
        Objeto da planilha conectada

    Raises:
        ValueError: Se credenciais não configuradas
        gspread.exceptions.APIError: Se falhar ao conectar
    """
    creds_path = os.getenv("GOOGLE_SHEETS_CREDENTIALS_PATH")
    sheet_id = os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID")

    if not creds_path or not sheet_id:
        raise ValueError(
            "Variáveis GOOGLE_SHEETS_CREDENTIALS_PATH e GOOGLE_SHEETS_SPREADSHEET_ID "
            "devem estar definidas no arquivo .env"
        )

    logger.info("connecting_to_sheets", credentials_path=creds_path)

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.file",
    ]

    credentials = Credentials.from_service_account_file(creds_path, scopes=scopes)
    client = gspread.authorize(credentials)

    spreadsheet = client.open_by_key(sheet_id)

    logger.info("sheets_connected", spreadsheet_title=spreadsheet.title)

    return spreadsheet


def update_populacao_dim_geo(
    spreadsheet: gspread.Spreadsheet,
    dry_run: bool = False,
    verbose: bool = False,
) -> Dict[str, int]:
    """
    Atualizar população 2022 e área em dim_geo.
    
    Args:
        spreadsheet: Planilha conectada
        dry_run: Se True, não faz alterações
        verbose: Se True, mostra detalhes
        
    Returns:
        Dict com estatísticas da atualização
    """
    worksheet = spreadsheet.worksheet("dim_geo")
    
    logger.info("fetching_dim_geo_data")
    
    # Pegar todos os dados
    all_data = worksheet.get_all_values()
    
    if not all_data:
        logger.error("empty_worksheet")
        raise ValueError("Worksheet dim_geo está vazia")
    
    # Header na primeira linha
    header = all_data[0]
    rows = all_data[1:]
    
    # Identificar índices das colunas
    try:
        idx_cod_ibge = header.index("cod_ibge")
        idx_nome = header.index("nome_municipio")
        idx_pop = header.index("populacao_2022")
        idx_area = header.index("area_km2")
    except ValueError as e:
        logger.error("missing_column", error=str(e))
        raise ValueError(f"Coluna não encontrada: {e}")
    
    print(f"\n📊 Encontrados {len(rows)} municípios em dim_geo")
    
    # Estatísticas
    stats = {
        "total": len(rows),
        "atualizados": 0,
        "com_populacao": 0,
        "erros": 0,
    }
    
    # Preparar atualizações em batch
    updates = []
    
    print("\n🔄 Buscando dados na API IBGE...")
    
    for i, row in enumerate(tqdm(rows, desc="   Processando")):
        cod_ibge = row[idx_cod_ibge]
        nome = row[idx_nome]
        pop_atual = row[idx_pop]
        area_atual = row[idx_area]
        
        # Só atualizar se estiver vazio ou zerado
        precisa_atualizar = (
            not pop_atual or 
            pop_atual == "0" or 
            not area_atual or 
            area_atual == "0" or
            area_atual == "0.0"
        )
        
        if not precisa_atualizar:
            if verbose:
                print(f"   ✓ {nome}: já tem dados")
            continue
        
        # Buscar dados na API
        data = get_municipio_data_ibge(cod_ibge)
        
        if data["populacao_2022"] > 0:
            stats["com_populacao"] += 1
        
        if data["populacao_2022"] == 0 and data["area_km2"] == 0:
            stats["erros"] += 1
            if verbose:
                print(f"   ✗ {nome}: sem dados na API")
            continue
        
        # Preparar atualização (índice + 2 porque: +1 para header, +1 para 1-indexed)
        row_num = i + 2
        
        # Atualizar células
        updates.append({
            "range": f"E{row_num}",  # populacao_2022 (coluna F)
            "values": [[data["populacao_2022"]]],
        })
        
        updates.append({
            "range": f"G{row_num}",  # area_km2 (coluna G)
            "values": [[data["area_km2"]]],
        })
        
        stats["atualizados"] += 1
        
        if verbose:
            print(f"   ✓ {nome}: pop={data['populacao_2022']:,}, área={data['area_km2']} km²")
        
        # Rate limit: 60 requests/minute
        time.sleep(0.1)
    
    # Aplicar atualizações em batch
    if updates and not dry_run:
        logger.info("applying_updates", count=len(updates))
        
        print(f"\n💾 Aplicando {len(updates)} atualizações...")
        
        # Batch update (100 por vez para não estourar limites)
        batch_size = 100
        
        for i in range(0, len(updates), batch_size):
            batch = updates[i:i + batch_size]
            worksheet.batch_update(batch)
            
            if verbose:
                print(f"   ✓ Batch {i // batch_size + 1}/{(len(updates) + batch_size - 1) // batch_size}")
        
        logger.info("updates_applied", count=len(updates))
    
    elif dry_run:
        print(f"\n🔍 DRY RUN: {len(updates)} atualizações seriam aplicadas")
    
    return stats


def main():
    """Função principal."""
    parser = argparse.ArgumentParser(
        description="Atualizar população 2022 e área em dim_geo"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simular atualização sem modificar dados",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Mostrar detalhes de cada município",
    )

    args = parser.parse_args()

    print("═" * 63)
    print("📊 UPDATE POPULAÇÃO/ÁREA - dim_geo")
    print("═" * 63)

    try:
        # Conectar ao Google Sheets
        print("\n🔍 Conectando ao Google Sheets...")
        spreadsheet = connect_sheets()
        print("   ✅ Conectado à planilha")

        # Atualizar dados
        stats = update_populacao_dim_geo(
            spreadsheet,
            dry_run=args.dry_run,
            verbose=args.verbose,
        )

        # Resumo
        print("\n" + "━" * 63)
        print("\n✅ RESUMO:")
        print(f"   • Total municípios: {stats['total']}")
        print(f"   • Atualizados: {stats['atualizados']}")
        print(f"   • Com população: {stats['com_populacao']}")
        print(f"   • Erros API: {stats['erros']}")

        if args.dry_run:
            print("\n⚠️  DRY RUN: Nenhuma alteração foi feita")

        print("\n" + "═" * 63)

        logger.info(
            "update_completed",
            total=stats["total"],
            atualizados=stats["atualizados"],
            com_populacao=stats["com_populacao"],
            erros=stats["erros"],
        )

    except Exception as e:
        logger.error("update_failed", error=str(e))
        print(f"\n❌ ERRO: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
