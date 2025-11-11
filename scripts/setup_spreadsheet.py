"""
Script de setup da planilha-mestre do projeto.

Configura uma planilha Google Sheets existente com todas as abas necessárias
para o pipeline de dados da construção civil, incluindo tabelas dimensionais,
fatos e controle.

Uso:
    python scripts/setup_spreadsheet.py <SPREADSHEET_ID>
    
Exemplo:
    python scripts/setup_spreadsheet.py 1A2B3C4D5E6F7G8H9I0J
"""

import os
import sys
import time
from pathlib import Path
from typing import List, Dict, Any, Optional
from functools import wraps

import gspread
import pandas as pd
import structlog
from dotenv import load_dotenv, set_key
from oauth2client.service_account import ServiceAccountCredentials

# Configurar logger
structlog.configure(
    processors=[
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer()
    ],
    wrapper_class=structlog.stdlib.BoundLogger,
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger(__name__)


def rate_limit(calls_per_minute=50):
    """
    Decorator para limitar número de chamadas por minuto à API do Google Sheets.
    
    Args:
        calls_per_minute: Número máximo de chamadas permitidas por minuto
        
    Returns:
        Decorator que adiciona rate limiting à função
    """
    min_interval = 60.0 / calls_per_minute
    last_called = [0.0]
    
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            elapsed = time.time() - last_called[0]
            left_to_wait = min_interval - elapsed
            if left_to_wait > 0:
                time.sleep(left_to_wait)
            result = func(*args, **kwargs)
            last_called[0] = time.time()
            return result
        return wrapper
    return decorator


def retry_on_rate_limit(max_retries=3, wait_time=60):
    """
    Decorator para retry automático em caso de rate limit (HTTP 429).
    
    Args:
        max_retries: Número máximo de tentativas
        wait_time: Tempo de espera entre tentativas (segundos)
        
    Returns:
        Decorator que adiciona retry automático
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except gspread.exceptions.APIError as e:
                    if hasattr(e, 'response') and e.response.status_code == 429:
                        if attempt < max_retries - 1:
                            logger.warning(
                                "rate_limit_hit",
                                attempt=attempt + 1,
                                max_retries=max_retries,
                                wait_time=wait_time
                            )
                            print(f"  ⚠️  Rate limit atingido (tentativa {attempt + 1}/{max_retries})")
                            print(f"      Aguardando {wait_time} segundos...")
                            time.sleep(wait_time)
                        else:
                            raise
                    else:
                        raise
            return None
        return wrapper
    return decorator


class SpreadsheetSetup:
    """
    Classe para configuração e setup da planilha-mestre.
    
    Gerencia autenticação, criação de abas, formatação e população
    de dados iniciais na planilha Google Sheets.
    """
    
    def __init__(self, credentials_path: str = "credentials.json"):
        """
        Inicializa o setup da planilha.
        
        Args:
            credentials_path: Caminho para arquivo de credenciais
        
        Raises:
            FileNotFoundError: Se credentials.json não existir
            Exception: Erro na autenticação
        """
        logger.info("initializing_spreadsheet_setup", credentials_path=credentials_path)
        
        # Validar que credentials existe
        if not os.path.exists(credentials_path):
            logger.error("credentials_not_found", path=credentials_path)
            raise FileNotFoundError(
                f"Arquivo de credenciais não encontrado: {credentials_path}\n"
                "Certifique-se de ter o arquivo credentials.json na raiz do projeto."
            )
        
        # Autenticar com Google Sheets API
        try:
            scope = [
                "https://spreadsheets.google.com/feeds",
                "https://www.googleapis.com/auth/drive"
            ]
            
            credentials = ServiceAccountCredentials.from_json_keyfile_name(
                credentials_path,
                scope
            )
            
            self.client = gspread.authorize(credentials)
            
            logger.info("google_sheets_authenticated")
        
        except Exception as e:
            logger.error(
                "authentication_failed",
                error=str(e),
                error_type=type(e).__name__
            )
            raise
    
    def open_existing_spreadsheet(self, spreadsheet_id: str) -> gspread.Spreadsheet:
        """
        Abre planilha existente por ID.
        
        Args:
            spreadsheet_id: ID da planilha Google Sheets
        
        Returns:
            Objeto Spreadsheet
        
        Raises:
            gspread.exceptions.SpreadsheetNotFound: Planilha não encontrada
            gspread.exceptions.APIError: Erro de permissão ou API
        """
        logger.info("opening_existing_spreadsheet", spreadsheet_id=spreadsheet_id)
        
        try:
            spreadsheet = self.client.open_by_key(spreadsheet_id)
            
            spreadsheet_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet.id}"
            
            # Validar permissões de escrita (tentar listar worksheets)
            worksheets = spreadsheet.worksheets()
            
            logger.info(
                "spreadsheet_opened",
                title=spreadsheet.title,
                id=spreadsheet.id,
                url=spreadsheet_url,
                existing_sheets=len(worksheets)
            )
            
            print(f"\n✓ Planilha encontrada: {spreadsheet.title}")
            print(f"  URL: {spreadsheet_url}")
            print(f"  ID: {spreadsheet.id}")
            print(f"  Abas existentes: {len(worksheets)}\n")
            
            return spreadsheet
        
        except gspread.exceptions.SpreadsheetNotFound:
            logger.error(
                "spreadsheet_not_found",
                spreadsheet_id=spreadsheet_id
            )
            raise Exception(
                f"Planilha não encontrada: {spreadsheet_id}\n"
                "Verifique se o ID está correto e se você tem permissão de acesso."
            )
        
        except gspread.exceptions.APIError as e:
            logger.error(
                "spreadsheet_api_error",
                spreadsheet_id=spreadsheet_id,
                error=str(e)
            )
            raise Exception(
                f"Erro ao acessar planilha: {str(e)}\n"
                "Verifique se o Service Account tem permissão de editor na planilha."
            )
        
        except Exception as e:
            logger.error(
                "spreadsheet_open_failed",
                spreadsheet_id=spreadsheet_id,
                error=str(e),
                error_type=type(e).__name__
            )
            raise
    
    @retry_on_rate_limit(max_retries=3, wait_time=60)
    @rate_limit(calls_per_minute=30)
    def setup_sheet(
        self,
        worksheet: gspread.Worksheet,
        name: str,
        headers: List[str],
        bg_color: Dict[str, float]
    ) -> None:
        """
        Configura aba com nome, cabeçalhos e formatação.
        
        Args:
            worksheet: Objeto Worksheet a configurar
            name: Nome da aba
            headers: Lista de cabeçalhos
            bg_color: Cor de fundo RGB normalizado (0-1)
        """
        logger.info("setting_up_sheet", name=name, headers_count=len(headers))
        
        try:
            # Renomear aba
            worksheet.update_title(name)
            
            # Adicionar cabeçalhos
            worksheet.append_row(headers)
            
            # Formatar linha de cabeçalho
            header_format = {
                "backgroundColor": bg_color,
                "textFormat": {
                    "bold": True,
                    "foregroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0}
                },
                "horizontalAlignment": "CENTER"
            }
            
            # Aplicar formato ao range de cabeçalhos
            worksheet.format(f"A1:{chr(64 + len(headers))}1", header_format)
            
            # Congelar linha 1
            worksheet.freeze(rows=1)
            
            # Ajustar largura de colunas usando requests batch
            # Note: gspread não tem set_column_width, então vamos usar o método correto
            requests = []
            for i in range(len(headers)):
                width = min(max(len(headers[i]) * 10, 100), 300)
                requests.append({
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": worksheet.id,
                            "dimension": "COLUMNS",
                            "startIndex": i,
                            "endIndex": i + 1
                        },
                        "properties": {
                            "pixelSize": width
                        },
                        "fields": "pixelSize"
                    }
                })
            
            # Aplicar ajustes de largura se houver
            if requests:
                try:
                    worksheet.spreadsheet.batch_update({"requests": requests})
                except Exception as e:
                    logger.warning(
                        "failed_to_set_column_widths",
                        name=name,
                        error=str(e)
                    )
            
            logger.info("sheet_setup_completed", name=name)
        
        except Exception as e:
            logger.error(
                "sheet_setup_failed",
                name=name,
                error=str(e)
            )
            raise
    
    def create_all_sheets(self, spreadsheet: gspread.Spreadsheet) -> None:
        """
        Cria todas as 18 abas necessárias para o projeto.
        
        Args:
            spreadsheet: Objeto Spreadsheet onde criar abas
        """
        logger.info("creating_all_sheets", spreadsheet_id=spreadsheet.id)
        
        # Cores (RGB normalizado 0-1)
        COLOR_BLUE = {"red": 0.26, "green": 0.52, "blue": 0.96}  # #4285F4
        COLOR_GREEN = {"red": 0.20, "green": 0.66, "blue": 0.33}  # #34A853
        COLOR_YELLOW = {"red": 0.98, "green": 0.74, "blue": 0.02}  # #FBBC04
        COLOR_RED = {"red": 0.92, "green": 0.26, "blue": 0.21}  # #EA4335
        
        # Definição de todas as abas
        sheets_config = [
            # DIMENSIONAIS (azul)
            ("dim_geo", [
                "cod_ibge", "nome_municipio", "uf", "cod_uf", "regiao",
                "populacao_2022", "area_km2", "is_capital", "created_at"
            ], COLOR_BLUE),
            
            ("dim_series", [
                "series_id", "nome", "categoria", "fonte", "freq",
                "unidade", "metodo_versao", "status", "created_at"
            ], COLOR_BLUE),
            
            ("dim_topografia", [
                "id_topografia", "desc_topografia", "fator_custo",
                "fator_prazo", "requer_contencao"
            ], COLOR_BLUE),
            
            ("dim_metodo", [
                "id_metodo", "nome_metodo", "fator_custo",
                "fator_prazo", "limitacao_pavimentos"
            ], COLOR_BLUE),
            
            ("dim_projetos", [
                "id_projeto", "nome_projeto", "custo_base_m2", "is_obrigatorio"
            ], COLOR_BLUE),
            
            ("dim_cub", [
                "id_cub", "uf", "tipo", "subtipo", "padrao",
                "custo_m2_base", "validade_inicio", "fonte_url"
            ], COLOR_BLUE),
            
            ("city_params", [
                "cod_ibge", "municipio", "uf", "fator_cidade", "itbi_aliquota",
                "alvara_base", "alvara_adicional_m2", "habite_se_base",
                "iss_obra", "vigencia_inicio", "fonte_url", "observacoes"
            ], COLOR_BLUE),
            
            ("fin_params_caixa", [
                "id_parametro", "tipo_financiamento", "taxa_juros_aa",
                "prazo_max_meses", "ltv_max", "vigencia_inicio"
            ], COLOR_BLUE),
            
            # FATOS (verde)
            ("fact_series", [
                "id_fato", "series_id", "data_referencia", "valor",
                "variacao_mom", "variacao_yoy", "fonte_original", "created_at"
            ], COLOR_GREEN),
            
            ("fact_credito", [
                "id_fato", "tipo_credito", "uf", "data_referencia",
                "valor_contratado_milhoes", "qtd_contratos", "created_at"
            ], COLOR_GREEN),
            
            ("fact_emprego", [
                "id_fato", "fonte", "uf", "data_referencia",
                "saldo_admissoes", "salario_medio", "created_at"
            ], COLOR_GREEN),
            
            ("fact_materiais", [
                "id_fato", "material", "regiao", "data_referencia",
                "preco_medio", "variacao_mom", "created_at"
            ], COLOR_GREEN),
            
            ("fact_clima", [
                "id_fato", "cod_estacao_inmet", "data_referencia",
                "precipitacao_mm", "temp_media_c", "dias_chuva", "created_at"
            ], COLOR_GREEN),
            
            ("fact_taxas_municipais", [
                "id_fato", "cod_ibge", "tipo_taxa", "valor_base",
                "aliquota", "vigencia_inicio", "created_at"
            ], COLOR_GREEN),
            
            # CONTROLE (amarelo e vermelho)
            ("_map_sgs", [
                "series_id_interno", "codigo_sgs", "nome_serie",
                "freq", "unidade", "url_api", "is_ativa"
            ], COLOR_YELLOW),
            
            ("_map_sidra", [
                "tabela_sidra", "variavel", "nome_variavel",
                "nivel_territorial", "url_api_base", "is_ativa"
            ], COLOR_YELLOW),
            
            ("_ingestion_log", [
                "exec_id", "fonte", "job_tipo", "inicio_exec",
                "fim_exec", "status", "linhas_processadas", "erros"
            ], COLOR_RED),
            
            ("_quality_flags", [
                "id_flag", "series_id", "data_referencia", "tipo_flag",
                "severidade", "valor_observado", "created_at"
            ], COLOR_RED),
        ]
        
        # Mapear abas existentes para evitar duplicatas
        existing_sheets_map = {}
        try:
            existing_sheets = spreadsheet.worksheets()
            existing_sheets_map = {sheet.title: sheet for sheet in existing_sheets}
            logger.info(
                "existing_sheets_found",
                count=len(existing_sheets_map),
                names=list(existing_sheets_map.keys())
            )
        except Exception as e:
            logger.warning("failed_to_list_existing_sheets", error=str(e))
        
        # Criar cada aba (ou reutilizar se já existe)
        created_count = 0
        configured_count = 0
        skipped_count = 0
        total_sheets = len(sheets_config)
        start_time = time.time()
        
        print(f"\n{'='*70}")
        print(f"  📊 Processando {total_sheets} abas...")
        print(f"{'='*70}\n")
        
        for idx, (name, headers, color) in enumerate(sheets_config, 1):
            try:
                # Mostrar progresso
                elapsed = time.time() - start_time
                avg_time_per_sheet = elapsed / idx if idx > 1 else 5
                remaining_sheets = total_sheets - idx
                eta_seconds = int(avg_time_per_sheet * remaining_sheets)
                eta_min = eta_seconds // 60
                eta_sec = eta_seconds % 60
                
                print(f"  [{idx}/{total_sheets}] {name}...", end=" ", flush=True)
                
                # Adicionar delay para evitar rate limit (exceto primeira aba)
                if idx > 1:
                    time.sleep(2)  # Pausa entre abas
                
                # Verificar se aba já existe
                if name in existing_sheets_map:
                    worksheet = existing_sheets_map[name]
                    
                    # Verificar se já tem dados (pelo menos cabeçalho)
                    try:
                        first_row = worksheet.row_values(1)
                        if first_row and len(first_row) >= len(headers):
                            # Aba já configurada - pular
                            print(f"✓ OK (ETA: {eta_min}m{eta_sec}s)")
                            skipped_count += 1
                            continue
                    except:
                        pass
                    
                    logger.info("reconfiguring_sheet", name=name)
                    
                    # Limpar e reconfigurar
                    worksheet.clear()
                    time.sleep(1)  # Pausa após clear
                    
                    self.setup_sheet(worksheet, name, headers, color)
                    time.sleep(1)  # Pausa após setup
                    
                    print(f"↻ Reconfigurada (ETA: {eta_min}m{eta_sec}s)")
                    configured_count += 1
                else:
                    # Criar nova aba
                    worksheet = spreadsheet.add_worksheet(
                        title=name,
                        rows=1000,
                        cols=len(headers)
                    )
                    time.sleep(1)  # Pausa após criar
                    
                    self.setup_sheet(worksheet, name, headers, color)
                    time.sleep(1)  # Pausa após setup
                    
                    print(f"✓ Criada ({len(headers)} cols, ETA: {eta_min}m{eta_sec}s)")
                    created_count += 1
                
                # Mostrar progresso a cada 5 abas
                if idx % 5 == 0:
                    elapsed_min = int(elapsed) // 60
                    elapsed_sec = int(elapsed) % 60
                    print(f"\n  ⏱️  Tempo decorrido: {elapsed_min}m{elapsed_sec}s | Processadas: {idx}/{total_sheets}\n")
            
            except Exception as e:
                logger.error(
                    "failed_to_create_sheet",
                    name=name,
                    error=str(e)
                )
                raise
        
        # Resumo final
        total_time = time.time() - start_time
        total_min = int(total_time) // 60
        total_sec = int(total_time) % 60
        
        print(f"\n{'='*70}")
        print(f"  ✓ Resumo: {created_count} criadas | {configured_count} reconfiguradas | {skipped_count} OK")
        print(f"  ⏱️  Tempo total: {total_min}m{total_sec}s")
        print(f"{'='*70}\n")
        
        logger.info(
            "all_sheets_processed",
            created=created_count,
            configured=configured_count,
            skipped=skipped_count,
            total_time_seconds=int(total_time)
        )
    
    @retry_on_rate_limit(max_retries=3, wait_time=60)
    def populate_initial_data(self, spreadsheet: gspread.Spreadsheet) -> int:
        """
        Preenche dados iniciais nas tabelas dimensionais.
        
        Args:
            spreadsheet: Objeto Spreadsheet
        
        Returns:
            Número total de linhas inseridas
        """
        logger.info("populating_initial_data")
        
        print(f"\n{'='*70}")
        print(f"  📝 Preenchendo dados iniciais...")
        print(f"{'='*70}\n")
        
        total_rows = 0
        
        # 1. Preencher dim_geo (5 municípios SC)
        try:
            print(f"  [1/8] dim_geo...", end=" ", flush=True)
            dim_geo = spreadsheet.worksheet("dim_geo")
            geo_data = [
                ['4205407', 'Florianópolis', 'SC', '42', 'SUL', 516524, 675.409, 'TRUE', '2024-11-07'],
                ['4216602', 'São José', 'SC', '42', 'SUL', 246995, 150.453, 'FALSE', '2024-11-07'],
                ['4212809', 'Palhoça', 'SC', '42', 'SUL', 173739, 394.687, 'FALSE', '2024-11-07'],
                ['4202131', 'Biguaçu', 'SC', '42', 'SUL', 70095, 331.942, 'FALSE', '2024-11-07'],
                ['4215752', 'Santo Amaro da Imperatriz', 'SC', '42', 'SUL', 22452, 344.526, 'FALSE', '2024-11-07']
            ]
            
            for row in geo_data:
                dim_geo.append_row(row)
                time.sleep(0.5)
            
            total_rows += len(geo_data)
            logger.info("dim_geo_populated", rows=len(geo_data))
            print(f"✓ {len(geo_data)} municípios")
        
        except Exception as e:
            logger.error("failed_to_populate_dim_geo", error=str(e))
            print(f"❌ Erro")
        
        time.sleep(1)
        
        time.sleep(1)
        
        # 2. Preencher city_params (5 municípios)
        try:
            print(f"  [7/8] city_params...", end=" ", flush=True)
            city_params = spreadsheet.worksheet("city_params")
            params_data = [
                ['4205407', 'Florianópolis', 'SC', 1.180, 3.00, 1800.00, 4.20, 3200.00, 2.50, '2024-01-01', 'https://leismunicipais.com.br/codigo-tributario-florianopolis-sc', 'Capital'],
                ['4216602', 'São José', 'SC', 1.120, 3.00, 1500.00, 3.80, 2800.00, 2.50, '2024-01-01', 'https://leismunicipais.com.br/a1/codigo-tributario-sao-jose-sc', 'Conurbação'],
                ['4212809', 'Palhoça', 'SC', 1.050, 2.50, 1200.00, 3.50, 2000.00, 2.00, '2024-01-01', 'https://leismunicipais.com.br/a1/codigo-tributario-palhoca-sc', 'ISS menor'],
                ['4202131', 'Biguaçu', 'SC', 1.020, 2.00, 800.00, 2.50, 1500.00, 2.00, '2024-01-01', 'https://leismunicipais.com.br/a1/codigo-tributario-biguacu-sc', 'Menor'],
                ['4215752', 'Santo Amaro da Imperatriz', 'SC', 1.000, 2.00, 600.00, 2.00, 1200.00, 2.00, '2024-01-01', 'https://leismunicipais.com.br/codigo-tributario-santo-amaro-da-imperatriz-sc', 'Rural']
            ]
            
            for row in params_data:
                city_params.append_row(row)
                time.sleep(0.5)
            
            total_rows += len(params_data)
            logger.info("city_params_populated", rows=len(params_data))
            print(f"✓ {len(params_data)} municípios")
        
        except Exception as e:
            logger.error("failed_to_populate_city_params", error=str(e))
            print(f"❌ Erro")
        
        time.sleep(1)
        
        # 3. Preencher dim_cub (5 tipos)
        try:
            print(f"  [8/8] dim_cub...", end=" ", flush=True)
            dim_cub = spreadsheet.worksheet("dim_cub")
            cub_data = [
                ['CUB_SC_R1N', 'SC', 'Residencial', 'R1-N', 'Normal', 2150.32, '2024-10-01', 'https://cbic.org.br'],
                ['CUB_SC_R1B', 'SC', 'Residencial', 'R1-B', 'Baixo', 1720.25, '2024-10-01', 'https://cbic.org.br'],
                ['CUB_SC_R1A', 'SC', 'Residencial', 'R1-A', 'Alto', 2580.38, '2024-10-01', 'https://cbic.org.br'],
                ['CUB_SC_R8N', 'SC', 'Residencial', 'R8-N', 'Normal', 2050.15, '2024-10-01', 'https://cbic.org.br'],
                ['CUB_SC_R8A', 'SC', 'Residencial', 'R8-A', 'Alto', 2458.22, '2024-10-01', 'https://cbic.org.br']
            ]
            
            for row in cub_data:
                dim_cub.append_row(row)
                time.sleep(0.5)
            
            total_rows += len(cub_data)
            logger.info("dim_cub_populated", rows=len(cub_data))
            print(f"✓ {len(cub_data)} tipos")
        
        except Exception as e:
            logger.error("failed_to_populate_dim_cub", error=str(e))
        
        # 4. Preencher dim_series (metadados séries BCB)
        try:
            print(f"  [2/8] dim_series...", end=" ", flush=True)
            dim_series_path = "configs/dim_series_initial.csv"
            
            if os.path.exists(dim_series_path):
                df = pd.read_csv(dim_series_path)
                dim_series = spreadsheet.worksheet("dim_series")
                
                for _, row in df.iterrows():
                    dim_series.append_row(row.tolist())
                    time.sleep(0.5)
                
                total_rows += len(df)
                logger.info("dim_series_populated", rows=len(df))
                print(f"✓ {len(df)} séries")
            else:
                logger.warning("dim_series_file_not_found", path=dim_series_path)
                print(f"⚠ arquivo não encontrado")
        
        except Exception as e:
            logger.error("failed_to_populate_dim_series", error=str(e))
            print(f"❌ Erro")
        
        time.sleep(1)
        
        # 5. Preencher dim_topografia (tipos de terreno)
        try:
            print(f"  [3/8] dim_topografia...", end=" ", flush=True)
            dim_topografia_path = "configs/dim_topografia_initial.csv"
            
            if os.path.exists(dim_topografia_path):
                df = pd.read_csv(dim_topografia_path)
                dim_topografia = spreadsheet.worksheet("dim_topografia")
                
                for _, row in df.iterrows():
                    dim_topografia.append_row(row.tolist())
                    time.sleep(0.5)
                
                total_rows += len(df)
                logger.info("dim_topografia_populated", rows=len(df))
                print(f"✓ {len(df)} tipos")
            else:
                logger.warning("dim_topografia_file_not_found", path=dim_topografia_path)
                print(f"⚠ arquivo não encontrado")
        
        except Exception as e:
            logger.error("failed_to_populate_dim_topografia", error=str(e))
            print(f"❌ Erro")
        
        time.sleep(1)
        
        # 6. Preencher dim_metodo (métodos construtivos)
        try:
            print(f"  [4/8] dim_metodo...", end=" ", flush=True)
            dim_metodo_path = "configs/dim_metodo_initial.csv"
            
            if os.path.exists(dim_metodo_path):
                df = pd.read_csv(dim_metodo_path)
                dim_metodo = spreadsheet.worksheet("dim_metodo")
                
                for _, row in df.iterrows():
                    dim_metodo.append_row(row.tolist())
                    time.sleep(0.5)
                
                total_rows += len(df)
                logger.info("dim_metodo_populated", rows=len(df))
                print(f"✓ {len(df)} métodos")
            else:
                logger.warning("dim_metodo_file_not_found", path=dim_metodo_path)
                print(f"⚠ arquivo não encontrado")
        
        except Exception as e:
            logger.error("failed_to_populate_dim_metodo", error=str(e))
            print(f"❌ Erro")
        
        time.sleep(1)
        
        # 7. Preencher dim_projetos (projetos complementares)
        try:
            print(f"  [5/8] dim_projetos...", end=" ", flush=True)
            dim_projetos_path = "configs/dim_projetos_initial.csv"
            
            if os.path.exists(dim_projetos_path):
                df = pd.read_csv(dim_projetos_path)
                dim_projetos = spreadsheet.worksheet("dim_projetos")
                
                for _, row in df.iterrows():
                    dim_projetos.append_row(row.tolist())
                    time.sleep(0.5)
                
                total_rows += len(df)
                logger.info("dim_projetos_populated", rows=len(df))
                print(f"✓ {len(df)} projetos")
            else:
                logger.warning("dim_projetos_file_not_found", path=dim_projetos_path)
                print(f"⚠ arquivo não encontrado")
        
        except Exception as e:
            logger.error("failed_to_populate_dim_projetos", error=str(e))
            print(f"❌ Erro")
        
        time.sleep(1)
        
        # 8. Preencher _map_sgs (se arquivo existir)
        try:
            print(f"  [6/8] _map_sgs...", end=" ", flush=True)
            maps_sgs_path = "configs/maps_sgs.csv"
            
            if os.path.exists(maps_sgs_path):
                df = pd.read_csv(maps_sgs_path)
                map_sgs = spreadsheet.worksheet("_map_sgs")
                
                for _, row in df.iterrows():
                    map_sgs.append_row(row.tolist())
                    time.sleep(0.5)
                
                total_rows += len(df)
                logger.info("map_sgs_populated", rows=len(df))
                print(f"✓ {len(df)} séries")
            else:
                logger.warning("maps_sgs_file_not_found", path=maps_sgs_path)
                print(f"⚠ arquivo não encontrado")
        
        except Exception as e:
            logger.error("failed_to_populate_map_sgs", error=str(e))
            print(f"❌ Erro")
        
        logger.info("initial_data_populated", total_rows=total_rows)
        print(f"\n{'='*70}")
        print(f"  ✓ Total de linhas inseridas: {total_rows}")
        print(f"{'='*70}\n")
        
        return total_rows
    
    def share_with_user(
        self,
        spreadsheet: gspread.Spreadsheet,
        email: str
    ) -> None:
        """
        Compartilha planilha com usuário específico.
        
        Args:
            spreadsheet: Objeto Spreadsheet
            email: Email do usuário
        """
        logger.info("sharing_spreadsheet", email=email)
        
        try:
            spreadsheet.share(
                email,
                perm_type="user",
                role="writer",
                notify=True,
                email_message="Planilha-Mestre do Centro de Inteligência da Construção Civil criada com sucesso!"
            )
            
            logger.info("spreadsheet_shared", email=email)
            print(f"✓ Planilha compartilhada com: {email}")
        
        except Exception as e:
            logger.error(
                "failed_to_share_spreadsheet",
                email=email,
                error=str(e)
            )
            print(f"⚠ Não foi possível compartilhar com {email}: {str(e)}")
    
    def save_to_env(self, spreadsheet_id: str) -> None:
        """
        Salva ID da planilha no arquivo .env.
        
        Args:
            spreadsheet_id: ID da planilha
        """
        logger.info("saving_to_env", spreadsheet_id=spreadsheet_id)
        
        try:
            env_path = Path(".env")
            
            # Criar .env se não existir
            if not env_path.exists():
                env_path.touch()
                logger.info("env_file_created")
            
            # Atualizar ou adicionar GOOGLE_SPREADSHEET_ID
            set_key(str(env_path), "GOOGLE_SPREADSHEET_ID", spreadsheet_id)
            
            logger.info("spreadsheet_id_saved_to_env")
            print(f"✓ GOOGLE_SPREADSHEET_ID salvo no .env")
        
        except Exception as e:
            logger.error(
                "failed_to_save_to_env",
                error=str(e)
            )
            print(f"⚠ Não foi possível salvar no .env: {str(e)}")


def main(spreadsheet_id: str) -> int:
    """
    Função principal do script de setup.
    
    Args:
        spreadsheet_id: ID da planilha Google Sheets existente
    
    Returns:
        0 para sucesso, 1 para erro
    """
    print("\n" + "=" * 70)
    print("SETUP DA PLANILHA-MESTRE")
    print("Centro de Inteligência da Construção Civil")
    print("=" * 70 + "\n")
    
    try:
        # Inicializar setup
        setup = SpreadsheetSetup()
        
        # Abrir planilha existente
        spreadsheet = setup.open_existing_spreadsheet(spreadsheet_id)
        
        # Criar todas as abas
        print("\nCriando abas...")
        setup.create_all_sheets(spreadsheet)
        
        # Preencher dados iniciais
        print("\nPreenchendo dados iniciais...")
        total_rows = setup.populate_initial_data(spreadsheet)
        
        # Salvar ID no .env
        print("\nSalvando configuração...")
        setup.save_to_env(spreadsheet.id)
        
        # Resumo final
        print("\n" + "=" * 70)
        print("SETUP CONCLUÍDO COM SUCESSO!")
        print("=" * 70)
        print(f"URL: https://docs.google.com/spreadsheets/d/{spreadsheet.id}")
        print(f"ID: {spreadsheet.id}")
        print(f"Abas criadas: 18")
        print(f"Linhas inseridas: {total_rows}")
        print(f"ID salvo em: .env")
        print("=" * 70 + "\n")
        
        logger.info(
            "setup_completed_successfully",
            spreadsheet_id=spreadsheet.id,
            sheets_created=18,
            rows_inserted=total_rows
        )
        
        return 0
    
    except Exception as e:
        print("\n" + "=" * 70)
        print("❌ ERRO NO SETUP")
        print("=" * 70)
        print(f"Erro: {str(e)}")
        print(f"Tipo: {type(e).__name__}")
        print("=" * 70 + "\n")
        
        logger.error(
            "setup_failed",
            error=str(e),
            error_type=type(e).__name__,
            exc_info=True
        )
        
        return 1


if __name__ == "__main__":
    # Validar argumentos
    if len(sys.argv) < 2:
        print("\n" + "=" * 70)
        print("❌ ERRO: SPREADSHEET_ID não fornecido")
        print("=" * 70)
        print("\nUso:")
        print("  python scripts/setup_spreadsheet.py <SPREADSHEET_ID>")
        print("\nExemplo:")
        print("  python scripts/setup_spreadsheet.py 1A2B3C4D5E6F7G8H9I0J")
        print("\nOnde encontrar o SPREADSHEET_ID:")
        print("  Na URL da planilha Google Sheets:")
        print("  https://docs.google.com/spreadsheets/d/[SPREADSHEET_ID]/edit")
        print("=" * 70 + "\n")
        sys.exit(1)
    
    # Obter SPREADSHEET_ID do primeiro argumento
    spreadsheet_id = sys.argv[1]
    
    print(f"SPREADSHEET_ID: {spreadsheet_id}\n")
    
    # Executar setup
    exit_code = main(spreadsheet_id)
    
    sys.exit(exit_code)
