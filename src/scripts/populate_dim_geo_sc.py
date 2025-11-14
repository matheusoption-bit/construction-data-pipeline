"""
Script para popular aba dim_geo com TODOS os municípios de Santa Catarina.

Busca dados atualizados via API IBGE (295 municípios) e adiciona ao Google Sheets.

Autor: Sistema de ETL - Construction Data Pipeline
Data: 2025-11-13
"""

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Set

import gspread
import requests
import structlog
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from tenacity import retry, stop_after_attempt, wait_exponential
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

# Constantes API IBGE
IBGE_MUNICIPIOS_SC_URL = "https://servicodados.ibge.gov.br/api/v1/localidades/estados/SC/municipios"

# API alternativa: Cidades IBGE (retorna população estimada mais recente)
IBGE_CIDADES_API_URL = "https://servicodados.ibge.gov.br/api/v1/pesquisas/indicadores/29171/resultados"


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
)
def get_populacao_municipios() -> Dict[str, int]:
    """
    Buscar população estimada dos municípios via API IBGE Cidades.
    
    Indicador 29171: População residente estimada (pessoas) mais recente.
    
    Returns:
        Dict[cod_ibge, populacao]
    """
    logger.info("fetching_populacao_from_ibge_cidades", url=IBGE_CIDADES_API_URL)
    
    try:
        # Buscar dados de população para SC (código 42)
        response = requests.get(
            f"{IBGE_CIDADES_API_URL}/42",
            timeout=30
        )
        response.raise_for_status()
        
        data = response.json()
        
        populacao_map = {}
        
        # Parse da resposta
        # Estrutura: [{"localidade": "4200051", "res": [{"periodo": "2023", "valor": "1234"}]}]
        for item in data:
            if "localidade" in item and "res" in item:
                cod_ibge = str(item["localidade"])
                
                # Pegar o valor mais recente
                if item["res"]:
                    valor_str = item["res"][0].get("valor", "0")
                    populacao = int(valor_str) if valor_str.isdigit() else 0
                    populacao_map[cod_ibge] = populacao
        
        logger.info(
            "populacao_fetched",
            total_municipios=len(populacao_map),
        )
        
        return populacao_map
        
    except Exception as e:
        logger.warning(
            "failed_to_fetch_populacao",
            error=str(e),
        )
        # Retornar dicionário vazio em caso de erro
        return {}


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
)
def get_municipios_sc_ibge() -> List[Dict[str, any]]:
    """
    Buscar todos os municípios de Santa Catarina via API IBGE.

    Returns:
        Lista de dicionários com dados dos municípios

    Raises:
        requests.RequestException: Se falhar ao buscar dados da API
        ValueError: Se dados inválidos forem retornados
    """
    logger.info("fetching_municipios_from_ibge", url=IBGE_MUNICIPIOS_SC_URL)

    # 1. Buscar lista de municípios
    response = requests.get(IBGE_MUNICIPIOS_SC_URL, timeout=30)
    response.raise_for_status()

    municipios_raw = response.json()

    if not municipios_raw or len(municipios_raw) < 290:
        raise ValueError(
            f"Dados incompletos da API IBGE: {len(municipios_raw)} municípios (esperado: 295)"
        )

    logger.info(
        "municipios_fetched",
        count=len(municipios_raw),
    )

    # 2. Buscar população de todos os municípios SC
    print("\n📊 Buscando população estimada...")
    populacao_map = get_populacao_municipios()
    
    if populacao_map:
        logger.info("populacao_data_available", total=len(populacao_map))
    else:
        logger.warning("populacao_data_unavailable")

    # 3. Extrair dados completos
    municipios = []

    for mun in municipios_raw:
        cod_ibge = str(mun["id"])

        # Validar código IBGE
        if not cod_ibge.startswith("42") or len(cod_ibge) != 7:
            logger.warning(
                "invalid_cod_ibge",
                cod_ibge=cod_ibge,
                nome=mun.get("nome"),
            )
            continue

        # Área territorial (já vem no endpoint de localidades)
        area_territorial = mun.get("area", {})
        if isinstance(area_territorial, dict):
            area_km2 = float(area_territorial.get("territorial", 0))
        else:
            area_km2 = 0.0

        # População estimada
        populacao = populacao_map.get(cod_ibge, 0)

        municipio = {
            "cod_ibge": cod_ibge,
            "nome_municipio": mun["nome"],
            "uf": "SC",
            "cod_uf": "42",
            "regiao": "SUL",
            "populacao_2022": populacao,
            "area_km2": round(area_km2, 3),
            "is_capital": "TRUE" if mun["nome"] == "Florianópolis" else "FALSE",
            "created_at": datetime.now().strftime("%Y-%m-%d"),
        }

        municipios.append(municipio)

    # 5. Validações finais
    total_municipios = len(municipios)
    municipios_com_pop = sum(1 for m in municipios if m["populacao_2022"] > 0)
    capitais = sum(1 for m in municipios if m["is_capital"] == "TRUE")

    logger.info(
        "municipios_processed",
        total=total_municipios,
        com_populacao=municipios_com_pop,
        capitais=capitais,
    )

    # Validações
    if total_municipios < 290:
        raise ValueError(f"Total de municípios abaixo do esperado: {total_municipios}")

    if capitais != 1:
        raise ValueError(f"Número incorreto de capitais: {capitais} (esperado: 1)")

    print(f"   ✅ População obtida para {municipios_com_pop} municípios")

    return municipios


def connect_sheets() -> gspread.Spreadsheet:
    """
    Conectar ao Google Sheets e retornar objeto da planilha.

    Returns:
        Objeto gspread.Spreadsheet

    Raises:
        FileNotFoundError: Se arquivo de credenciais não existir
        ValueError: Se variáveis de ambiente não estiverem configuradas
    """
    credentials_path = os.getenv(
        "GOOGLE_SHEETS_CREDENTIALS_PATH", "config/google_credentials.json"
    )
    spreadsheet_url = os.getenv("GOOGLE_SHEETS_URL")

    if not spreadsheet_url:
        raise ValueError("GOOGLE_SHEETS_URL não configurada no .env")

    if not Path(credentials_path).exists():
        raise FileNotFoundError(f"Arquivo de credenciais não encontrado: {credentials_path}")

    logger.info(
        "connecting_to_sheets",
        credentials_path=credentials_path,
    )

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    credentials = Credentials.from_service_account_file(credentials_path, scopes=scopes)
    client = gspread.authorize(credentials)
    spreadsheet = client.open_by_url(spreadsheet_url)

    logger.info(
        "sheets_connected",
        spreadsheet_title=spreadsheet.title,
    )

    return spreadsheet


def get_existing_municipios(sheet: gspread.Worksheet) -> Set[str]:
    """
    Obter códigos IBGE dos municípios já existentes na aba.

    Args:
        sheet: Worksheet do gspread (aba dim_geo)

    Returns:
        Set com códigos IBGE existentes
    """
    logger.info("fetching_existing_municipios", sheet_name=sheet.title)

    # Buscar primeira coluna (cod_ibge)
    all_values = sheet.col_values(1)

    # Pular header (primeira linha)
    cod_ibge_existentes = set(all_values[1:]) if len(all_values) > 1 else set()

    # Remover valores vazios
    cod_ibge_existentes = {cod for cod in cod_ibge_existentes if cod.strip()}

    logger.info(
        "existing_municipios_fetched",
        count=len(cod_ibge_existentes),
    )

    return cod_ibge_existentes


def append_municipios(sheet: gspread.Worksheet, municipios: List[Dict[str, any]]) -> None:
    """
    Adicionar municípios à aba dim_geo.

    Args:
        sheet: Worksheet do gspread (aba dim_geo)
        municipios: Lista de dicionários com dados dos municípios
    """
    if not municipios:
        logger.info("no_municipios_to_append")
        return

    logger.info("appending_municipios", count=len(municipios))

    # Converter dicts em lista de listas
    # Ordem: cod_ibge, nome_municipio, uf, cod_uf, regiao, populacao_2022, area_km2, is_capital, created_at
    rows = []

    for mun in municipios:
        row = [
            mun["cod_ibge"],
            mun["nome_municipio"],
            mun["uf"],
            mun["cod_uf"],
            mun["regiao"],
            mun["populacao_2022"],
            mun["area_km2"],
            mun["is_capital"],
            mun["created_at"],
        ]
        rows.append(row)

    # Adicionar ao Google Sheets
    sheet.append_rows(rows, value_input_option="USER_ENTERED")

    logger.info(
        "municipios_appended",
        count=len(rows),
    )

    print(f"   ✅ {len(rows)} linhas adicionadas com sucesso!")


def main(dry_run: bool = False, verbose: bool = False) -> int:
    """
    Função principal.

    Args:
        dry_run: Se True, não grava no Google Sheets
        verbose: Se True, exibe mais detalhes

    Returns:
        Código de saída (0 = sucesso, 1 = erro)
    """
    print("\n═══════════════════════════════════════════════════════════")
    print("🗺️  POPULATE dim_geo - MUNICÍPIOS SC")
    print("═══════════════════════════════════════════════════════════\n")

    try:
        # 1. Buscar municípios na API IBGE
        print("📡 Buscando municípios SC na API IBGE...")
        municipios = get_municipios_sc_ibge()

        if len(municipios) < 290:
            print(f"❌ ERRO: Apenas {len(municipios)} municípios encontrados (esperado: 295)")
            return 1

        print(f"   ✅ {len(municipios)} municípios encontrados\n")

        # 2. Conectar ao Google Sheets
        print("🔍 Conectando ao Google Sheets...")
        spreadsheet = connect_sheets()
        sheet = spreadsheet.worksheet("dim_geo")
        print("   ✅ Conectado à planilha\n")

        # 3. Verificar municípios existentes
        print("🔍 Verificando municípios existentes em dim_geo...")
        existentes = get_existing_municipios(sheet)
        print(f"   ℹ️  {len(existentes)} municípios já existem")

        # 4. Filtrar novos municípios
        novos = [m for m in municipios if m["cod_ibge"] not in existentes]

        if not novos:
            print("   ✅ Todos municípios já estão na planilha\n")
            print("═══════════════════════════════════════════════════════════\n")
            return 0

        print(f"   ✅ {len(novos)} novos municípios para adicionar\n")

        # 5. Adicionar novos municípios
        if dry_run:
            print("🔍 DRY-RUN MODE: Não gravando no Google Sheets")
            print(f"   Municípios que seriam adicionados: {len(novos)}\n")

            if verbose:
                print("   Amostra (primeiros 10):")
                for i, mun in enumerate(novos[:10], 1):
                    print(f"   {i}. {mun['nome_municipio']} ({mun['cod_ibge']}) - Pop: {mun['populacao_2022']:,}")
                print()
        else:
            print("💾 Adicionando ao Google Sheets...")
            append_municipios(sheet, novos)
            print()

        # 6. Resumo final
        capital = next((m for m in municipios if m["is_capital"] == "TRUE"), None)

        print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
        print("✅ RESUMO:")
        print(f"   • Total municípios SC: {len(municipios)}")
        print(f"   • Já existentes: {len(existentes)}")
        print(f"   • Novos adicionados: {len(novos) if not dry_run else 0}")
        print(f"   • Total final em dim_geo: {len(existentes) + (len(novos) if not dry_run else 0)} linhas")

        if capital:
            print(f"   • Capital: {capital['nome_municipio']} ✓")

        print("\n═══════════════════════════════════════════════════════════\n")

        # Exibir detalhes se verbose
        if verbose:
            print("📊 DETALHES:\n")
            print(f"   População total SC 2022: {sum(m['populacao_2022'] for m in municipios):,} habitantes")
            print(f"   Área total SC: {sum(m['area_km2'] for m in municipios):,.2f} km²")
            print(f"   Maior município (pop): {max(municipios, key=lambda x: x['populacao_2022'])['nome_municipio']}")
            print(f"   Maior município (área): {max(municipios, key=lambda x: x['area_km2'])['nome_municipio']}")
            print()

        logger.info(
            "populate_completed",
            total_municipios=len(municipios),
            existentes=len(existentes),
            novos_adicionados=len(novos) if not dry_run else 0,
        )

        return 0

    except requests.RequestException as e:
        logger.error("api_request_failed", error=str(e))
        print(f"\n❌ ERRO ao buscar dados da API IBGE: {e}")
        return 1

    except gspread.exceptions.WorksheetNotFound:
        logger.error("worksheet_not_found", worksheet="dim_geo")
        print("\n❌ ERRO: Aba 'dim_geo' não encontrada no Google Sheets")
        return 1

    except Exception as e:
        logger.error("populate_failed", error=str(e))
        print(f"\n❌ ERRO: {e}")
        return 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Popular aba dim_geo com municípios de Santa Catarina"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simular execução sem gravar no Google Sheets",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Exibir informações detalhadas",
    )

    args = parser.parse_args()

    sys.exit(main(dry_run=args.dry_run, verbose=args.verbose))
