"""
Script para atualizar população e área dos municípios SC em dim_geo.

Usa dados do Censo 2022 via API IBGE Sidra (mais estável).

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

# Configurar encoding para Windows
if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

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

# API SIDRA (mais estável que agregados)
SIDRA_POPULACAO_URL = "https://apisidra.ibge.gov.br/values/t/4709/n6/all/v/93/p/2022"
SIDRA_AREA_URL = "https://apisidra.ibge.gov.br/values/t/1301/n6/all/v/615/p/last%201"


def get_municipios_area_sidra() -> Dict[str, float]:
    """
    Buscar área territorial de todos os municípios via API SIDRA.
    
    Tabela 1301: Área territorial oficial (km²).
    Variável 615: Área total das unidades territoriais.
    
    Returns:
        Dict[cod_ibge, area_km2]
    """
    logger.info("fetching_area_from_sidra", url=SIDRA_AREA_URL)
    
    try:
        response = requests.get(SIDRA_AREA_URL, timeout=60)
        response.raise_for_status()
        
        data = response.json()
        
        area_map = {}
        
        # Parse da resposta SIDRA
        # Estrutura: [{"D1C": "4200051", "D1N": "Abdon Batista (SC)", "V": "235.6", "D2C": "615", ...}]
        
        for i, row in enumerate(data):
            # Skip primeira linha (header)
            if i == 0:
                continue
            
            # Dados estão em D1C (código município) e V (valor)
            cod_ibge = str(row.get("D1C", ""))
            valor_str = str(row.get("V", "0"))
            variavel_cod = str(row.get("D2C", ""))
            
            # Filtrar apenas variável 615 (área territorial)
            if variavel_cod != "615":
                continue
            
            # Filtrar apenas SC (código começa com 42)
            if not cod_ibge or not cod_ibge.startswith("42"):
                continue
            
            # Validar código IBGE (7 dígitos)
            if len(cod_ibge) != 7:
                continue
            
            # Converter área (usar vírgula como separador decimal)
            try:
                area = float(valor_str.replace(",", "."))
                area_map[cod_ibge] = round(area, 3)
            except (ValueError, TypeError):
                area_map[cod_ibge] = 0.0
        
        logger.info(
            "area_fetched",
            total_municipios=len(area_map),
        )
        
        return area_map
        
    except Exception as e:
        logger.error(
            "failed_to_fetch_area_sidra",
            error=str(e),
        )
        raise


def get_municipios_data_sidra() -> Dict[str, Dict[str, any]]:
    """
    Buscar população 2022 de todos os municípios via API SIDRA.
    
    Tabela 4709: População residente (Censo 2022).
    
    Returns:
        Dict[cod_ibge, {"populacao": int, "nome": str}]
    """
    logger.info("fetching_from_sidra", url=SIDRA_POPULACAO_URL)
    
    try:
        response = requests.get(SIDRA_POPULACAO_URL, timeout=60)
        response.raise_for_status()
        
        data = response.json()
        
        municipios_data = {}
        
        # Parse da resposta SIDRA
        # Estrutura: [{"NC": "6", "NN": "Município", "MC": "45", "MN": "Pessoas", "V": "500000", "D1C": "4200051", "D1N": "Florianópolis (SC)", ...}]
        # Primeira linha é header
        
        for i, row in enumerate(data):
            # Skip primeira linha (header)
            if i == 0:
                continue
            
            # Dados estão em D1C (código município) e V (valor)
            cod_ibge = str(row.get("D1C", ""))
            valor_str = str(row.get("V", "0"))
            nome_completo = str(row.get("D1N", ""))
            
            # Filtrar apenas SC (código começa com 42)
            if not cod_ibge or not cod_ibge.startswith("42"):
                continue
            
            # Validar código IBGE (7 dígitos)
            if len(cod_ibge) != 7:
                continue
            
            # Remover " (SC)" do nome
            nome = nome_completo.replace(" (SC)", "").strip()
            
            # Converter população
            try:
                populacao = int(valor_str)
            except (ValueError, TypeError):
                populacao = 0
            
            municipios_data[cod_ibge] = {
                "populacao": populacao,
                "nome": nome,
            }
        
        logger.info(
            "sidra_data_fetched",
            total_municipios=len(municipios_data),
        )
        
        return municipios_data
        
    except Exception as e:
        logger.error(
            "failed_to_fetch_sidra",
            error=str(e),
        )
        raise


def get_municipio_area_ibge(cod_ibge: str) -> float:
    """
    Buscar área territorial de um município via API Localidades.
    
    Args:
        cod_ibge: Código IBGE do município
        
    Returns:
        Área em km² (3 casas decimais)
    """
    url = f"https://servicodados.ibge.gov.br/api/v1/localidades/municipios/{cod_ibge}"
    
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        
        area_obj = data.get("area", {})
        if isinstance(area_obj, dict):
            area = float(area_obj.get("territorial", 0))
        else:
            area = 0.0
        
        return round(area, 3)
        
    except Exception as e:
        logger.warning(
            "failed_to_fetch_area",
            cod_ibge=cod_ibge,
            error=str(e),
        )
        return 0.0


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


def update_municipios_data(
    spreadsheet: gspread.Spreadsheet,
    dry_run: bool = False,
    verbose: bool = False,
) -> Dict[str, int]:
    """
    Atualizar população e área em dim_geo.
    
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
    
    # Buscar dados do SIDRA
    print("\n🔄 Buscando dados de população (SIDRA/Censo 2022)...")
    municipios_sidra = get_municipios_data_sidra()
    
    print(f"   ✓ {len(municipios_sidra)} municípios SC com dados de população")
    
    print("\n🔄 Buscando dados de área territorial (SIDRA/Tabela 1301)...")
    area_sidra = get_municipios_area_sidra()
    
    print(f"   ✓ {len(area_sidra)} municípios SC com dados de área")
    
    # Estatísticas
    stats = {
        "total": len(rows),
        "atualizados_pop": 0,
        "atualizados_area": 0,
        "sem_dados": 0,
    }
    
    # Preparar atualizações em batch
    updates = []
    
    print("\n🔄 Processando atualizações...")
    
    for i, row in enumerate(tqdm(rows, desc="   Verificando")):
        cod_ibge = row[idx_cod_ibge]
        nome = row[idx_nome]
        pop_atual = row[idx_pop]
        area_atual = row[idx_area]
        
        # Dados do SIDRA
        dados_sidra = municipios_sidra.get(cod_ibge)
        
        if not dados_sidra:
            stats["sem_dados"] += 1
            if verbose:
                print(f"   ⚠ {nome}: sem dados no SIDRA")
            continue
        
        # Preparar atualização (índice + 2 porque: +1 para header, +1 para 1-indexed)
        row_num = i + 2
        
        # Verificar se precisa atualizar população
        populacao = dados_sidra["populacao"]
        precisa_atualizar_pop = (
            not pop_atual or 
            pop_atual == "0" or 
            str(pop_atual) != str(populacao)
        )
        
        if precisa_atualizar_pop:
            updates.append({
                "range": f"F{row_num}",  # populacao_2022 (coluna F)
                "values": [[populacao]],
            })
            stats["atualizados_pop"] += 1
        
        # Verificar se precisa atualizar área
        try:
            # Normalizar área (pode ser "0", "0.0", "0,0", etc.)
            area_str = area_atual.strip().replace(",", ".") if area_atual else "0"
            area_valor = float(area_str) if area_str else 0.0
        except (ValueError, AttributeError):
            area_valor = 0.0
        
        precisa_atualizar_area = (area_valor == 0.0)
        
        if precisa_atualizar_area:
            # Buscar área no mapa do SIDRA
            area = area_sidra.get(cod_ibge, 0.0)
            
            if area > 0:
                updates.append({
                    "range": f"G{row_num}",  # area_km2 (coluna G)
                    "values": [[area]],
                })
                stats["atualizados_area"] += 1
        
        if verbose and (precisa_atualizar_pop or precisa_atualizar_area):
            status = []
            if precisa_atualizar_pop:
                status.append(f"pop={populacao:,}")
            if precisa_atualizar_area:
                area = area_sidra.get(cod_ibge, 0.0)
                if area > 0:
                    status.append(f"área={area} km²")
            
            if status:
                print(f"   ✓ {nome}: {', '.join(status)}")
    
    # Aplicar atualizações em batch
    if updates and not dry_run:
        logger.info("applying_updates", count=len(updates))
        
        print(f"\n💾 Aplicando {len(updates)} atualizações no Google Sheets...")
        
        # Batch update (100 por vez)
        batch_size = 100
        
        for i in range(0, len(updates), batch_size):
            batch = updates[i:i + batch_size]
            worksheet.batch_update(batch)
            
            if verbose:
                print(f"   ✓ Batch {i // batch_size + 1}/{(len(updates) + batch_size - 1) // batch_size}")
            
            time.sleep(1)  # Rate limit
        
        logger.info("updates_applied", count=len(updates))
        print("   ✅ Atualizações concluídas!")
    
    elif dry_run:
        print(f"\n🔍 DRY RUN: {len(updates)} atualizações seriam aplicadas")
    
    else:
        print("\n✅ Nenhuma atualização necessária")
    
    return stats


def main():
    """Função principal."""
    parser = argparse.ArgumentParser(
        description="Atualizar população e área dos municípios SC em dim_geo"
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
    print("📊 UPDATE MUNICÍPIOS SC - População & Área")
    print("═" * 63)

    try:
        # Conectar ao Google Sheets
        print("\n🔍 Conectando ao Google Sheets...")
        spreadsheet = connect_sheets()
        print("   ✅ Conectado à planilha")

        # Atualizar dados
        stats = update_municipios_data(
            spreadsheet,
            dry_run=args.dry_run,
            verbose=args.verbose,
        )

        # Resumo
        print("\n" + "━" * 63)
        print("\n✅ RESUMO:")
        print(f"   • Total municípios: {stats['total']}")
        print(f"   • População atualizada: {stats['atualizados_pop']}")
        print(f"   • Área atualizada: {stats['atualizados_area']}")
        print(f"   • Sem dados: {stats['sem_dados']}")

        if args.dry_run:
            print("\n⚠️  DRY RUN: Nenhuma alteração foi feita")

        print("\n" + "═" * 63)

        logger.info(
            "update_completed",
            **stats,
        )

    except Exception as e:
        logger.error("update_failed", error=str(e))
        print(f"\n❌ ERRO: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
