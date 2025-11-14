"""
Script de auditoria da planilha Google Sheets do Centro de Inteligência da Construção Civil.

Criticidade: ALTA - Apresentação sexta-feira 15/11/2025.

Tarefas:
1. Análise abas CUB (redundância)
2. Validação geral (41 abas)
3. Validação essenciais (foco apresentação)

Autor: Sistema de ETL - Construction Data Pipeline
Data: 2025-11-13
"""

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import gspread
import pandas as pd
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


class AuditGoogleSheets:
    """Auditor para planilha do Centro de Inteligência da Construção Civil."""

    # Abas CUB para Tarefa 1
    CUB_WORKSHEETS = [
        "fact_cub_por_uf",
        "fact_cub_variacao",
        "fact_cub_detalhado",
        "fact_cub_historico",
        "dim_cub",
        "dim_tipo_cub",
        "dim_composicao_cub_medio",
    ]

    # Dados esperados para validação (Tarefa 3)
    EXPECTED_COUNTS = {
        "fact_cub_por_uf": 4598,
        "fact_cub_variacao": 13461,
        "fact_cub_detalhado": 18059,
        "fact_cub_historico": 118,
        "dim_localidade": 27,
        "dim_tipo_cub": 4,
        "city_params": 5,
        "fin_params_caixa": 1,
    }

    # Abas essenciais para Tarefa 3
    ESSENTIAL_WORKSHEETS = [
        "fact_cub_por_uf",
        "fact_cub_variacao",
        "fact_cub_detalhado",
        "fact_cub_historico",
        "dim_localidade",
        "dim_tipo_cub",
        "city_params",
        "fin_params_caixa",
    ]

    # Categorias para Tarefa 2
    WORKSHEET_CATEGORIES = {
        "DIMENSIONAIS": [
            "dim_geo",
            "dim_series",
            "dim_topografia",
            "dim_metodo",
            "dim_projetos",
            "dim_cub",
            "dim_tipo_cub",
            "dim_localidade",
            "dim_composicao_cub_medio",
            "dim_tempo",
            "dim_moeda",
            "dim_fonte_dados",
            "dim_tipo_edificacao",
            "dim_padrao_acabamento",
        ],
        "FATOS": [
            "fact_cub_por_uf",
            "fact_cub_variacao",
            "fact_cub_detalhado",
            "fact_cub_historico",
            "fact_series",
            "fact_credito",
            "fact_emprego",
            "fact_materiais",
            "fact_clima",
            "fact_taxas_municipais",
        ],
        "BCB_SERIES": [
            "taxa_cambio",
            "igp_m_infla",
            "taxa_ref",
            "credito_habit",
            "inpc_infla",
            "taxa_selic",
            "ipca_infla",
            "credito_pf",
            "rend_poupanca",
        ],
        "CONFIGURACAO": [
            "city_params",
            "fin_params_caixa",
            "_map_sgs",
            "_map_sidra",
            "_data_sources",
            "_update_schedule",
        ],
        "LOGS": [
            "_ingestion_log",
            "_quality_flags",
        ],
    }

    def __init__(self, credentials_path: Optional[str] = None, spreadsheet_url: Optional[str] = None):
        """
        Inicializar auditor.

        Args:
            credentials_path: Caminho para credenciais Google (padrão: config/google_credentials.json)
            spreadsheet_url: URL da planilha Google Sheets
        """
        # Configuração
        self.credentials_path = credentials_path or os.getenv(
            "GOOGLE_SHEETS_CREDENTIALS_PATH", "config/google_credentials.json"
        )
        self.spreadsheet_url = spreadsheet_url or os.getenv("GOOGLE_SHEETS_URL")

        if not self.spreadsheet_url:
            raise ValueError("GOOGLE_SHEETS_URL não configurada no .env")

        # Cliente e planilha
        self.client: Optional[gspread.Client] = None
        self.spreadsheet: Optional[gspread.Spreadsheet] = None

        # Resultados
        self.all_worksheets: List[gspread.Worksheet] = []
        self.audit_results: Dict[str, Any] = {}

        # Criar pasta reports
        self.reports_dir = Path("reports")
        self.reports_dir.mkdir(exist_ok=True)

        logger.info(
            "auditor_initialized",
            credentials_path=self.credentials_path,
            spreadsheet_url=self.spreadsheet_url,
        )

    def connect(self) -> None:
        """Conectar ao Google Sheets."""
        try:
            scopes = [
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive",
            ]

            credentials = Credentials.from_service_account_file(
                self.credentials_path, scopes=scopes
            )

            self.client = gspread.authorize(credentials)
            self.spreadsheet = self.client.open_by_url(self.spreadsheet_url)

            logger.info(
                "connection_successful",
                spreadsheet_title=self.spreadsheet.title,
            )

            print(f"✅ Conectado: {self.spreadsheet.title}")

        except Exception as e:
            logger.error("connection_failed", error=str(e))
            raise

    def get_all_worksheets(self) -> List[gspread.Worksheet]:
        """
        Obter todas as abas da planilha.

        Returns:
            Lista de objetos worksheet
        """
        if not self.spreadsheet:
            raise RuntimeError("Não conectado. Execute connect() primeiro.")

        self.all_worksheets = self.spreadsheet.worksheets()

        logger.info(
            "worksheets_fetched",
            count=len(self.all_worksheets),
        )

        print(f"📊 Total de abas encontradas: {len(self.all_worksheets)}")

        return self.all_worksheets

    def analyze_worksheet(self, ws: gspread.Worksheet) -> Dict[str, Any]:
        """
        Analisar uma aba e retornar metadados.

        Args:
            ws: Objeto worksheet do gspread

        Returns:
            Dicionário com informações da aba
        """
        try:
            # Obter todos os valores
            all_values = ws.get_all_values()

            # Metadados básicos
            name = ws.title
            total_rows = len(all_values)
            has_data = total_rows > 1

            if has_data:
                headers = all_values[0] if all_values else []
                data_rows = total_rows - 1
                num_cols = len(headers)
                status = "COM DADOS"
            else:
                headers = []
                data_rows = 0
                num_cols = 0
                status = "VAZIA"

            result = {
                "name": name,
                "status": status,
                "total_rows": total_rows,
                "data_rows": data_rows,
                "num_cols": num_cols,
                "headers": headers,
            }

            logger.info(
                "worksheet_analyzed",
                name=name,
                status=status,
                data_rows=data_rows,
            )

            return result

        except Exception as e:
            logger.error("worksheet_analysis_failed", worksheet=ws.title, error=str(e))
            return {
                "name": ws.title,
                "status": "ERRO",
                "error": str(e),
            }

    def analyze_cub_tabs(self) -> Dict[str, Any]:
        """
        Analisar abas CUB e identificar redundâncias.

        Returns:
            Dicionário com análise completa das abas CUB
        """
        results = {
            "abas_analisadas": 0,
            "detalhes": {},
            "redundancias": {},
        }

        print("📊 Analisando abas CUB...\n")

        # Propósitos conhecidos de cada aba
        propositos = {
            "fact_cub_por_uf": "Valores CUB-medio (R$/m²) por estado",
            "fact_cub_variacao": "Variações percentuais (mensal, 12m, ano)",
            "fact_cub_detalhado": "Consolidado de valores + variações",
            "fact_cub_historico": "Backfill histórico SC (dez/2015 - set/2025)",
            "dim_cub": "Dimensão tipos de CUB (possível redundância)",
            "dim_tipo_cub": "Dimensão tipos de métricas CUB",
            "dim_composicao_cub_medio": "Composição detalhada do CUB médio",
        }

        decisoes = {
            "fact_cub_por_uf": "MANTER (dados únicos, não redundante)",
            "fact_cub_variacao": "MANTER (dados únicos, não redundante)",
            "fact_cub_detalhado": "AVALIAR (consolidado - pode ser redundante)",
            "fact_cub_historico": "MANTER (dados históricos exclusivos)",
            "dim_cub": "AVALIAR (verificar vs dim_tipo_cub)",
            "dim_tipo_cub": "MANTER (dimensão padrão)",
            "dim_composicao_cub_medio": "MANTER (dados específicos)",
        }

        for ws_name in self.CUB_WORKSHEETS:
            try:
                ws = self.spreadsheet.worksheet(ws_name)
                all_values = ws.get_all_values()

                if not all_values or len(all_values) <= 1:
                    results["detalhes"][ws_name] = {
                        "linhas": 0,
                        "colunas": [],
                        "sample": [],
                        "proposito": propositos.get(ws_name, "Desconhecido"),
                        "decisao": "VAZIA",
                        "status": "VAZIA",
                    }
                    results["abas_analisadas"] += 1
                    continue

                # Separar headers e dados
                headers = all_values[0]
                data = all_values[1:]
                num_rows = len(data)

                # Sample: primeiras 3 linhas
                sample = data[:3] if len(data) >= 3 else data

                results["detalhes"][ws_name] = {
                    "linhas": num_rows,
                    "colunas": headers,
                    "sample": sample,
                    "proposito": propositos.get(ws_name, "Desconhecido"),
                    "decisao": decisoes.get(ws_name, "AVALIAR"),
                    "status": "COM DADOS",
                }

                results["abas_analisadas"] += 1

                logger.info(
                    "cub_tab_analyzed",
                    name=ws_name,
                    rows=num_rows,
                    cols=len(headers),
                )

            except gspread.exceptions.WorksheetNotFound:
                results["detalhes"][ws_name] = {
                    "linhas": 0,
                    "colunas": [],
                    "sample": [],
                    "proposito": propositos.get(ws_name, "Desconhecido"),
                    "decisao": "NÃO ENCONTRADA",
                    "status": "NÃO ENCONTRADA",
                }
                results["abas_analisadas"] += 1

                logger.warning("cub_tab_not_found", name=ws_name)

        # Análise de redundâncias
        detalhes = results["detalhes"]

        # 1. fact_cub_detalhado vs (por_uf + variacao)
        if all(k in detalhes for k in ["fact_cub_por_uf", "fact_cub_variacao", "fact_cub_detalhado"]):
            por_uf_lines = detalhes["fact_cub_por_uf"]["linhas"]
            variacao_lines = detalhes["fact_cub_variacao"]["linhas"]
            detalhado_lines = detalhes["fact_cub_detalhado"]["linhas"]
            expected_sum = por_uf_lines + variacao_lines

            if abs(detalhado_lines - expected_sum) <= 10:
                results["redundancias"]["fact_cub_detalhado"] = (
                    f"REDUNDANTE (soma de por_uf + variacao: {por_uf_lines:,} + {variacao_lines:,} = {expected_sum:,}, "
                    f"detalhado tem {detalhado_lines:,})"
                )
                # Atualizar decisão
                detalhes["fact_cub_detalhado"]["decisao"] = "REDUNDANTE - Pode deletar ou manter como 'aba mestre'"
            else:
                results["redundancias"]["fact_cub_detalhado"] = (
                    f"NÃO REDUNDANTE (diferença de {abs(detalhado_lines - expected_sum)} linhas)"
                )
                detalhes["fact_cub_detalhado"]["decisao"] = "MANTER - Contém dados diferentes"

        # 2. dim_cub vs dim_tipo_cub
        if all(k in detalhes for k in ["dim_cub", "dim_tipo_cub"]):
            dim_cub_lines = detalhes["dim_cub"]["linhas"]
            dim_tipo_cub_lines = detalhes["dim_tipo_cub"]["linhas"]

            if dim_cub_lines > 0 and dim_tipo_cub_lines > 0:
                results["redundancias"]["dim_cub_vs_dim_tipo_cub"] = (
                    f"AVALIAR - Ambas com dados (dim_cub: {dim_cub_lines}, dim_tipo_cub: {dim_tipo_cub_lines}). "
                    "Verificar conteúdo para consolidar"
                )
                detalhes["dim_cub"]["decisao"] = "AVALIAR - Possível redundância com dim_tipo_cub"
            elif dim_tipo_cub_lines > 0:
                results["redundancias"]["dim_cub_vs_dim_tipo_cub"] = (
                    f"dim_cub VAZIA - Manter apenas dim_tipo_cub ({dim_tipo_cub_lines} linhas)"
                )
                detalhes["dim_cub"]["decisao"] = "DELETAR - Redundante e vazia"
            else:
                results["redundancias"]["dim_cub_vs_dim_tipo_cub"] = "Ambas vazias - Aguardar dados"

        logger.info(
            "cub_analysis_completed",
            tabs_analyzed=results["abas_analisadas"],
            redundancies_found=len(results["redundancias"]),
        )

        return results

    def export_task1_report(self, data: Dict[str, Any]) -> None:
        """
        Exportar relatório da Tarefa 1 em formato Markdown.

        Args:
            data: Dicionário com análise das abas CUB
        """
        report_path = self.reports_dir / "audit_task1_cub_analysis.md"

        with open(report_path, "w", encoding="utf-8") as f:
            f.write("# Tarefa 1: Análise de Redundância - Abas CUB\n\n")
            f.write(f"**Data:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"**Apresentação:** Sexta-feira, 15/11/2025\n")
            f.write(f"**Abas Analisadas:** {data['abas_analisadas']}\n\n")

            f.write("---\n\n")
            f.write("## 📊 Resumo Executivo\n\n")

            # Contar por status
            com_dados = sum(1 for d in data["detalhes"].values() if d["status"] == "COM DADOS")
            vazias = sum(1 for d in data["detalhes"].values() if d["status"] == "VAZIA")
            nao_encontradas = sum(1 for d in data["detalhes"].values() if d["status"] == "NÃO ENCONTRADA")

            f.write(f"- **Total de abas CUB:** {data['abas_analisadas']}\n")
            f.write(f"- **Com dados:** {com_dados}\n")
            f.write(f"- **Vazias:** {vazias}\n")
            f.write(f"- **Não encontradas:** {nao_encontradas}\n")
            f.write(f"- **Redundâncias identificadas:** {len(data['redundancias'])}\n\n")

            f.write("---\n\n")
            f.write("## 🗂️ Detalhes por Aba\n\n")

            for ws_name, details in data["detalhes"].items():
                status_icon = {
                    "COM DADOS": "✅",
                    "VAZIA": "⚠️",
                    "NÃO ENCONTRADA": "❌",
                }.get(details["status"], "❓")

                f.write(f"### {status_icon} {ws_name}\n\n")
                f.write(f"**Status:** {details['status']}\n\n")
                f.write(f"**Linhas:** {details['linhas']:,}\n\n")
                f.write(f"**Colunas ({len(details['colunas'])}):**\n")
                if details['colunas']:
                    f.write(f"```\n{', '.join(details['colunas'])}\n```\n\n")
                else:
                    f.write("*(Nenhuma coluna)*\n\n")

                f.write(f"**Propósito:**\n{details['proposito']}\n\n")
                f.write(f"**Decisão:**\n{details['decisao']}\n\n")

                # Sample
                if details['sample']:
                    f.write("**Sample (primeiras 3 linhas):**\n\n")
                    f.write("```\n")
                    for i, row in enumerate(details['sample'], 1):
                        row_preview = " | ".join(str(v)[:40] for v in row[:5])
                        f.write(f"{i}. {row_preview}\n")
                    f.write("```\n\n")

                f.write("---\n\n")

            f.write("## 🔍 Análise de Redundâncias\n\n")

            if data["redundancias"]:
                for key, analysis in data["redundancias"].items():
                    f.write(f"### {key.replace('_', ' ').title()}\n\n")
                    f.write(f"{analysis}\n\n")
            else:
                f.write("*Nenhuma redundância identificada.*\n\n")

            f.write("---\n\n")
            f.write("## 💡 Recomendações\n\n")

            # Gerar recomendações baseadas nas decisões
            manter = [name for name, d in data["detalhes"].items() if "MANTER" in d["decisao"]]
            avaliar = [name for name, d in data["detalhes"].items() if "AVALIAR" in d["decisao"]]
            redundante = [name for name, d in data["detalhes"].items() if "REDUNDANTE" in d["decisao"]]
            deletar = [name for name, d in data["detalhes"].items() if "DELETAR" in d["decisao"]]

            f.write("### ✅ Manter (Essenciais)\n\n")
            if manter:
                for ws_name in manter:
                    f.write(f"- **{ws_name}**: {data['detalhes'][ws_name]['proposito']}\n")
            else:
                f.write("*Nenhuma aba nesta categoria.*\n")
            f.write("\n")

            f.write("### ⚠️ Avaliar (Verificar Necessidade)\n\n")
            if avaliar:
                for ws_name in avaliar:
                    f.write(f"- **{ws_name}**: {data['detalhes'][ws_name]['decisao']}\n")
            else:
                f.write("*Nenhuma aba nesta categoria.*\n")
            f.write("\n")

            f.write("### 🗑️ Possível Remoção\n\n")
            if redundante or deletar:
                for ws_name in redundante + deletar:
                    f.write(f"- **{ws_name}**: {data['detalhes'][ws_name]['decisao']}\n")
            else:
                f.write("*Nenhuma aba nesta categoria.*\n")
            f.write("\n")

            f.write("---\n\n")
            f.write("## 🎯 Impacto na Apresentação\n\n")
            f.write("- **Dados essenciais:** Validar que fact_cub_por_uf, fact_cub_variacao e fact_cub_historico estão populados\n")
            f.write("- **Redundâncias:** Não bloqueiam apresentação, mas podem ser otimizadas posteriormente\n")
            f.write("- **Prioridade:** Garantir que as 3 abas essenciais (por_uf, variacao, historico) estejam 100% corretas\n\n")

        logger.info("task1_report_exported", report_path=str(report_path))
        print(f"📄 Relatório Tarefa 1 salvo: {report_path}\n")

    def task1_analyze_cub_redundancy(self) -> Dict[str, Any]:
        """
        TAREFA 1: Análise de redundância nas abas CUB.

        Returns:
            Dicionário com análise de redundância
        """
        print("\n╔════════════════════════════════════════════════════════╗")
        print("║ TAREFA 1: ANÁLISE ABAS CUB                            ║")
        print("╚════════════════════════════════════════════════════════╝\n")

        # Usar novo método analyze_cub_tabs
        results = self.analyze_cub_tabs()

        # Exibir resultados no terminal
        print("📊 ABAS CUB ENCONTRADAS:\n")

        for ws_name, details in results["detalhes"].items():
            status_icon = {
                "COM DADOS": "✅",
                "VAZIA": "⚠️",
                "NÃO ENCONTRADA": "❌",
            }.get(details["status"], "❓")

            print(f"{status_icon} {ws_name}")
            print(f"   • {details['linhas']:,} linhas")
            print(f"   • {len(details['colunas'])} colunas")
            if details['colunas']:
                print(f"   • Colunas: {', '.join(details['colunas'][:6])}")
            print(f"   • Propósito: {details['proposito']}")
            print(f"   • DECISÃO: {details['decisao']}")
            print()

        # Exibir análise de redundância
        print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
        print("💡 ANÁLISE DE REDUNDÂNCIA:\n")

        if results["redundancias"]:
            for key, analysis in results["redundancias"].items():
                print(f"⚠️  {key.replace('_', ' ').title()}")
                print(f"   {analysis}\n")
        else:
            print("✅ Nenhuma redundância crítica identificada.\n")

        print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")

        # Exportar relatório
        self.export_task1_report(results)

        return results

    def analyze_all_tabs(self) -> Dict[str, Any]:
        """
        Analisar todas as 41 abas e categorizar.

        Returns:
            Dicionário com análise categorizada de todas as abas
        """
        results = {
            "total_abas": 0,
            "abas_com_dados": 0,
            "abas_vazias": 0,
            "categorias": {},
            "nao_mapeadas": [],
        }

        print("📊 Analisando todas as abas...\n")

        # Definir categorias
        categorias_map = {
            "DIMENSIONAIS": [],
            "FATOS": [],
            "BCB_SERIES": [],
            "CONFIGURACAO": [],
            "LOGS": [],
        }

        # Analisar cada aba
        for ws in tqdm(self.all_worksheets, desc="Analisando todas as abas"):
            analysis = self.analyze_worksheet(ws)
            results["total_abas"] += 1

            if analysis["status"] == "COM DADOS":
                results["abas_com_dados"] += 1
            elif analysis["status"] == "VAZIA":
                results["abas_vazias"] += 1

            # Categorizar
            ws_name = ws.title
            categorized = False

            # DIMENSIONAIS
            if ws_name.startswith("dim_"):
                categorias_map["DIMENSIONAIS"].append(analysis)
                categorized = True

            # FATOS
            elif ws_name.startswith("fact_"):
                categorias_map["FATOS"].append(analysis)
                categorized = True

            # BCB SERIES
            elif (
                ws_name.startswith("taxa_")
                or ws_name.endswith("_infla")
                or ws_name.startswith("credito_")
                or ws_name == "rend_poupanca"
                or ws_name == "igp_m_infla"
            ):
                categorias_map["BCB_SERIES"].append(analysis)
                categorized = True

            # CONFIGURACAO
            elif (
                ws_name.endswith("_params")
                or ws_name.startswith("_map_")
                or ws_name == "_data_sources"
                or ws_name == "_update_schedule"
            ):
                categorias_map["CONFIGURACAO"].append(analysis)
                categorized = True

            # LOGS
            elif ws_name in ["_ingestion_log", "_quality_flags"]:
                categorias_map["LOGS"].append(analysis)
                categorized = True

            # Não mapeada
            if not categorized:
                results["nao_mapeadas"].append(analysis)

        results["categorias"] = categorias_map

        logger.info(
            "all_tabs_analyzed",
            total=results["total_abas"],
            with_data=results["abas_com_dados"],
            empty=results["abas_vazias"],
        )

        return results

    def export_task2_report(self, data: Dict[str, Any]) -> None:
        """
        Exportar relatório da Tarefa 2 em formato Markdown.

        Args:
            data: Dicionário com análise de todas as abas
        """
        report_path = self.reports_dir / "audit_task2_all_tabs_status.md"

        with open(report_path, "w", encoding="utf-8") as f:
            f.write("# Tarefa 2: Validação Geral - Todas as Abas\n\n")
            f.write(f"**Data:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"**Apresentação:** Sexta-feira, 15/11/2025\n\n")

            f.write("---\n\n")
            f.write("## 📊 Resumo Executivo\n\n")
            f.write(f"- **Total de abas:** {data['total_abas']}\n")
            f.write(f"- **Abas COM DADOS:** {data['abas_com_dados']}\n")
            f.write(f"- **Abas VAZIAS:** {data['abas_vazias']}\n\n")

            # Detalhes por categoria
            for categoria, abas in data["categorias"].items():
                if not abas:
                    continue

                f.write("---\n\n")

                icon_map = {
                    "DIMENSIONAIS": "🔵",
                    "FATOS": "🟢",
                    "BCB_SERIES": "🟡",
                    "CONFIGURACAO": "⚙️",
                    "LOGS": "📋",
                }
                icon = icon_map.get(categoria, "📦")

                f.write(f"## {icon} {categoria} ({len(abas)} abas)\n\n")

                for ws_info in abas:
                    status_icon = "✅" if ws_info["status"] == "COM DADOS" else "⚠️"

                    f.write(f"### {status_icon} {ws_info['name']}\n\n")
                    f.write(f"- **Status:** {ws_info['status']}\n")
                    f.write(f"- **Linhas:** {ws_info.get('data_rows', 0):,}\n")
                    f.write(f"- **Colunas:** {ws_info.get('num_cols', 0)}\n")

                    if ws_info.get("headers"):
                        headers_preview = ", ".join(ws_info["headers"][:6])
                        f.write(f"- **Headers:** `{headers_preview}`\n")

                    f.write("\n")

            # Abas não mapeadas
            if data["nao_mapeadas"]:
                f.write("---\n\n")
                f.write(f"## 🔍 Abas Não Mapeadas ({len(data['nao_mapeadas'])})\n\n")

                for ws_info in data["nao_mapeadas"]:
                    status_icon = "✅" if ws_info["status"] == "COM DADOS" else "⚠️"
                    f.write(
                        f"- {status_icon} **{ws_info['name']}**: {ws_info.get('data_rows', 0):,} linhas\n"
                    )

                f.write("\n")

            # Estatísticas
            f.write("---\n\n")
            f.write("## 📊 Estatísticas\n\n")

            # Abas mais populosas
            all_abas = []
            for abas in data["categorias"].values():
                all_abas.extend(abas)
            all_abas.extend(data["nao_mapeadas"])

            populated = [a for a in all_abas if a["status"] == "COM DADOS"]
            populated_sorted = sorted(
                populated, key=lambda x: x.get("data_rows", 0), reverse=True
            )

            f.write("### Abas Mais Populosas\n\n")
            for i, aba in enumerate(populated_sorted[:10], 1):
                f.write(f"{i}. **{aba['name']}**: {aba.get('data_rows', 0):,} linhas\n")

            f.write("\n")

            # Abas vazias
            empty_abas = [a for a in all_abas if a["status"] == "VAZIA"]
            f.write(f"### Abas Vazias ({len(empty_abas)})\n\n")

            for aba in empty_abas:
                f.write(f"- {aba['name']}\n")

            f.write("\n")

        logger.info("task2_report_exported", report_path=str(report_path))
        print(f"📄 Relatório Tarefa 2 salvo: {report_path}\n")

    def validate_essentials(self) -> Dict[str, Any]:
        """
        Validar profundamente abas essenciais para apresentação.

        Returns:
            Dicionário com validação detalhada e veredicto GO/NO-GO
        """
        results = {
            "abas_validadas": 0,
            "veredicto": "GO",
            "issues": [],
            "detalhes": {},
        }

        print("🎯 Validando abas essenciais...\n")

        for ws_name in tqdm(self.ESSENTIAL_WORKSHEETS, desc="Validando essenciais"):
            try:
                ws = self.spreadsheet.worksheet(ws_name)
                all_values = ws.get_all_values()

                if not all_values or len(all_values) <= 1:
                    results["detalhes"][ws_name] = {
                        "status": "FAILED",
                        "reason": "Aba vazia",
                    }
                    results["issues"].append(f"{ws_name}: Aba vazia")
                    results["veredicto"] = "NO-GO"
                    results["abas_validadas"] += 1
                    continue

                # Separar headers e dados
                headers = all_values[0]
                data = all_values[1:]
                data_rows = len(data)

                # Converter para DataFrame
                df = pd.DataFrame(data, columns=headers)

                # Validações
                validation = {
                    "linhas": data_rows,
                    "colunas": len(headers),
                    "headers": headers,
                    "duplicatas": int(df.duplicated().sum()),
                    "null_values": {},
                    "sample_first": data[:3],
                    "sample_last": data[-3:] if len(data) > 3 else [],
                    "status": "PASSED",
                    "warnings": [],
                }

                # Verificar contagem esperada
                expected = self.EXPECTED_COUNTS.get(ws_name)
                if expected:
                    if data_rows < expected * 0.95:  # Tolerância 95%
                        validation["warnings"].append(
                            f"Linhas abaixo do esperado: {data_rows} < {expected}"
                        )
                        validation["status"] = "WARNING"

                # Verificar duplicatas
                if validation["duplicatas"] > 0:
                    validation["warnings"].append(
                        f"{validation['duplicatas']} duplicatas encontradas"
                    )
                    validation["status"] = "WARNING"

                # Verificar NULL por coluna
                null_counts = df.isnull().sum()
                for col, count in null_counts.items():
                    if count > 0:
                        validation["null_values"][col] = int(count)

                # Validações específicas por aba
                if ws_name == "fact_cub_por_uf":
                    if "uf" in headers:
                        unique_ufs = df["uf"].nunique()
                        validation["ufs_unicas"] = unique_ufs

                    if "data_referencia" in headers:
                        dates = df["data_referencia"].unique()
                        if len(dates) > 0:
                            validation["periodo"] = {
                                "inicio": min(dates),
                                "fim": max(dates),
                            }

                elif ws_name == "dim_localidade":
                    if data_rows != 27:
                        validation["warnings"].append(
                            f"Esperado 27 UFs, encontrado {data_rows}"
                        )

                elif ws_name == "dim_tipo_cub":
                    if data_rows != 4:
                        validation["warnings"].append(
                            f"Esperado 4 tipos, encontrado {data_rows}"
                        )

                elif ws_name == "city_params":
                    if "uf" in headers:
                        sc_count = (df["uf"] == "SC").sum()
                        validation["municipios_sc"] = int(sc_count)
                        if sc_count < 5:
                            validation["warnings"].append(
                                f"Menos de 5 municípios SC: {sc_count}"
                            )
                            validation["status"] = "WARNING"

                # Adicionar aos resultados
                results["detalhes"][ws_name] = validation
                results["abas_validadas"] += 1

                # Atualizar veredicto geral
                if validation["status"] == "FAILED":
                    results["veredicto"] = "NO-GO"
                    results["issues"].append(f"{ws_name}: Falha crítica")

            except gspread.exceptions.WorksheetNotFound:
                results["detalhes"][ws_name] = {
                    "status": "FAILED",
                    "reason": "Não encontrada",
                }
                results["issues"].append(f"{ws_name}: Não encontrada")
                results["veredicto"] = "NO-GO"
                results["abas_validadas"] += 1

            except Exception as e:
                results["detalhes"][ws_name] = {
                    "status": "FAILED",
                    "reason": str(e),
                }
                results["issues"].append(f"{ws_name}: {str(e)}")
                results["veredicto"] = "NO-GO"
                results["abas_validadas"] += 1

        logger.info(
            "essentials_validated",
            tabs=results["abas_validadas"],
            veredicto=results["veredicto"],
            issues=len(results["issues"]),
        )

        return results

    def export_task3_report(self, data: Dict[str, Any]) -> None:
        """
        Exportar relatórios da Tarefa 3 (JSON detalhado + Markdown resumo).

        Args:
            data: Dicionário com validação das abas essenciais
        """
        # 1. Relatório JSON com dados completos
        json_path = self.reports_dir / "audit_task3_essentials_validation.json"

        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "timestamp": datetime.now().isoformat(),
                    "veredicto": data["veredicto"],
                    "abas_validadas": data["abas_validadas"],
                    "issues": data["issues"],
                    "detalhes": data["detalhes"],
                },
                f,
                indent=2,
                ensure_ascii=False,
            )

        logger.info("task3_json_exported", report_path=str(json_path))
        print(f"📄 Relatório Tarefa 3 (JSON) salvo: {json_path}")

        # 2. Resumo GO/NO-GO para apresentação
        md_path = self.reports_dir / "audit_summary_presentation.md"

        with open(md_path, "w", encoding="utf-8") as f:
            f.write("# Resumo Executivo - Auditoria Google Sheets\n\n")
            f.write(f"**Data:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"**Apresentação:** Sexta-feira, 15/11/2025\n\n")

            f.write("---\n\n")

            # Veredicto principal
            if data["veredicto"] == "GO":
                f.write("## ✅ VEREDICTO: GO PARA APRESENTAÇÃO!\n\n")
                f.write("Todos os dados essenciais estão validados e prontos.\n\n")
            else:
                f.write("## ⚠️ VEREDICTO: NO-GO - REVISAR ANTES DA APRESENTAÇÃO\n\n")
                f.write("Issues críticos foram identificados nas abas essenciais.\n\n")

            f.write("---\n\n")
            f.write("## 📊 Status das Abas Essenciais\n\n")

            passed = sum(
                1 for d in data["detalhes"].values() if d.get("status") == "PASSED"
            )
            warnings = sum(
                1 for d in data["detalhes"].values() if d.get("status") == "WARNING"
            )
            failed = sum(
                1 for d in data["detalhes"].values() if d.get("status") == "FAILED"
            )

            f.write(f"- **Total validadas:** {data['abas_validadas']}\n")
            f.write(f"- **✅ PASSED:** {passed}\n")
            f.write(f"- **⚠️ WARNING:** {warnings}\n")
            f.write(f"- **❌ FAILED:** {failed}\n\n")

            # Detalhes por aba
            f.write("---\n\n")
            f.write("## 🗂️ Detalhes por Aba\n\n")

            for ws_name, details in data["detalhes"].items():
                status_icon = {
                    "PASSED": "✅",
                    "WARNING": "⚠️",
                    "FAILED": "❌",
                }.get(details.get("status"), "❓")

                f.write(f"### {status_icon} {ws_name}\n\n")
                f.write(f"**Status:** {details.get('status', 'UNKNOWN')}\n\n")

                if details.get("status") in ["PASSED", "WARNING"]:
                    f.write(f"- Linhas: {details.get('linhas', 0):,}\n")
                    f.write(f"- Colunas: {details.get('colunas', 0)}\n")
                    f.write(f"- Duplicatas: {details.get('duplicatas', 0)}\n")

                    if details.get("null_values"):
                        f.write(f"- Valores NULL: {sum(details['null_values'].values())}\n")

                    if details.get("warnings"):
                        f.write("\n**Alertas:**\n")
                        for warning in details["warnings"]:
                            f.write(f"- ⚠️ {warning}\n")

                elif details.get("reason"):
                    f.write(f"- **Erro:** {details['reason']}\n")

                f.write("\n")

            # Issues críticos
            if data["issues"]:
                f.write("---\n\n")
                f.write("## ⚠️ Issues Críticos\n\n")
                for issue in data["issues"]:
                    f.write(f"- {issue}\n")
                f.write("\n")

            # Próximos passos
            f.write("---\n\n")
            f.write("## 📋 Próximos Passos\n\n")

            if data["veredicto"] == "GO":
                f.write("1. ✅ Sistema pronto para demonstração\n")
                f.write("2. Preparar visualizações para apresentação\n")
                f.write("3. Testar queries principais\n")
                f.write("4. Revisar warnings (não bloqueantes)\n")
            else:
                f.write("1. ⚠️ **URGENTE:** Corrigir issues críticos listados acima\n")
                f.write("2. Re-executar auditoria após correções\n")
                f.write("3. Validar dados manualmente\n")
                f.write("4. Considerar adiar apresentação se necessário\n")

        logger.info("task3_summary_exported", report_path=str(md_path))
        print(f"📄 Resumo para apresentação salvo: {md_path}\n")

    def task2_validate_all_tabs(self) -> Dict[str, Any]:
        """
        TAREFA 2: Validação geral de todas as 41 abas.

        Returns:
            Dicionário com status de todas as abas
        """
        print("\n╔════════════════════════════════════════════════════════╗")
        print("║ TAREFA 2: VALIDAÇÃO GERAL (41 ABAS)                   ║")
        print("╚════════════════════════════════════════════════════════╝\n")

        # Usar novo método analyze_all_tabs
        results = self.analyze_all_tabs()

        # Exibir estatísticas
        print("\n📊 RESUMO EXECUTIVO:")
        print(f"   • Total de abas: {results['total_abas']}")
        print(f"   • Abas COM DADOS: {results['abas_com_dados']}")
        print(f"   • Abas VAZIAS: {results['abas_vazias']}")
        print()

        # Exibir por categoria
        for categoria, abas in results["categorias"].items():
            if not abas:
                continue

            print(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")

            icon_map = {
                "DIMENSIONAIS": "🔵",
                "FATOS": "🟢",
                "BCB_SERIES": "🟡",
                "CONFIGURACAO": "⚙️",
                "LOGS": "📋",
            }
            icon = icon_map.get(categoria, "📦")

            print(f"{icon} {categoria} ({len(abas)} abas):\n")

            for ws_info in abas:
                status_icon = "✅" if ws_info["status"] == "COM DADOS" else "⚠️"
                name = ws_info["name"]
                rows = ws_info.get("data_rows", 0)
                cols = ws_info.get("num_cols", 0)

                if ws_info["status"] == "COM DADOS":
                    print(f"{status_icon} {name:35s} {rows:>6,} linhas   {cols} colunas")
                    if ws_info.get("headers"):
                        headers_preview = ", ".join(ws_info["headers"][:4])
                        print(f"   Headers: {headers_preview}")
                else:
                    print(f"{status_icon} {name:35s} 0 linhas   (VAZIA)")

                print()

        # Abas não mapeadas
        if results["nao_mapeadas"]:
            print(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
            print(f"🔍 ABAS NÃO MAPEADAS ({len(results['nao_mapeadas'])}):\n")
            for ws_info in results["nao_mapeadas"]:
                status_icon = "✅" if ws_info["status"] == "COM DADOS" else "⚠️"
                print(f"{status_icon} {ws_info['name']} - {ws_info.get('data_rows', 0):,} linhas")
            print()

        print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")

        # Exportar relatório
        self.export_task2_report(results)

        return results

    def task3_validate_essentials(self) -> Dict[str, Any]:
        """
        TAREFA 3: Validação profunda das abas essenciais.

        Returns:
            Dicionário com validação detalhada
        """
        print("\n╔════════════════════════════════════════════════════════╗")
        print("║ TAREFA 3: VALIDAÇÃO ESSENCIAIS                        ║")
        print("╚════════════════════════════════════════════════════════╝\n")

        print("🎯 ABAS CRÍTICAS PARA APRESENTAÇÃO:\n")

        # Usar novo método validate_essentials
        results = self.validate_essentials()

        # Exibir resultados no terminal
        for i, ws_name in enumerate(self.ESSENTIAL_WORKSHEETS, 1):
            print(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
            print(f"{i}️⃣  {ws_name}\n")

            details = results["detalhes"].get(ws_name, {})

            if details.get("status") in ["PASSED", "WARNING"]:
                print("📊 CONTAGEM:")
                expected = self.EXPECTED_COUNTS.get(ws_name)
                if expected:
                    print(f"   ✅ Linhas: {details['linhas']:,} (esperado: {expected:,})")
                else:
                    print(f"   ℹ️  Linhas: {details['linhas']:,}")
                print(f"   ✅ Colunas: {details['colunas']}")
                print()

                print("🔍 QUALIDADE:")
                dup_icon = "✅" if details['duplicatas'] == 0 else "⚠️"
                print(f"   {dup_icon} Duplicatas: {details['duplicatas']}")

                null_total = sum(details.get('null_values', {}).values())
                null_icon = "✅" if null_total == 0 else "⚠️"
                print(f"   {null_icon} Valores NULL: {null_total}")
                print()

                # Validações específicas
                if "ufs_unicas" in details:
                    print("📈 CONSISTÊNCIA:")
                    print(f"   ℹ️  UFs únicas: {details['ufs_unicas']}")
                    if "periodo" in details:
                        print(f"   ℹ️  Período: {details['periodo']['inicio']} até {details['periodo']['fim']}")
                    print()

                if "municipios_sc" in details:
                    print("📈 CONSISTÊNCIA:")
                    sc_icon = "✅" if details['municipios_sc'] >= 5 else "⚠️"
                    print(f"   {sc_icon} Municípios SC: {details['municipios_sc']} (mínimo: 5)")
                    print()

                # Warnings
                if details.get("warnings"):
                    print("⚠️  ALERTAS:")
                    for warning in details["warnings"]:
                        print(f"   • {warning}")
                    print()

                # Veredicto individual
                if details["status"] == "PASSED":
                    print("✅ VEREDICTO: PRONTO PARA APRESENTAÇÃO")
                else:
                    print("⚠️  VEREDICTO: ATENÇÃO - Verificar alertas")

            else:
                print(f"❌ ERRO: {details.get('reason', 'Erro desconhecido')}")

            print()

        # Veredicto final
        print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
        print("🎯 VEREDICTO FINAL:\n")

        passed = sum(1 for d in results["detalhes"].values() if d.get("status") == "PASSED")
        warnings = sum(1 for d in results["detalhes"].values() if d.get("status") == "WARNING")

        print(f"✅ ABAS ESSENCIAIS: {passed}/{results['abas_validadas']} perfeitas")
        if warnings > 0:
            print(f"⚠️  ALERTAS: {warnings} abas com issues menores")

        if results["veredicto"] == "GO":
            print("\n🟢 STATUS GERAL: GO PARA APRESENTAÇÃO!")
        else:
            print("\n🔴 STATUS GERAL: NO-GO - REVISAR ISSUES CRÍTICOS!")

        if results["issues"]:
            print("\n⚠️  Issues críticos:")
            for issue in results["issues"]:
                print(f"   • {issue}")

        print("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")

        # Exportar relatórios
        self.export_task3_report(results)

        return results

    def main(self, task: Optional[int] = None) -> None:
        """
        Executar auditoria completa ou tarefa específica.

        Args:
            task: Número da tarefa (1, 2 ou 3). Se None, executa todas.
        """
        print("\n🚀 Iniciando auditoria Google Sheets...\n")
        print(f"📅 Data: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"🎯 Apresentação: Sexta-feira, 15/11/2025\n")

        # Conectar
        self.connect()

        # Obter todas as abas
        self.get_all_worksheets()

        # Executar tarefas conforme solicitado
        if task == 1:
            print("📋 Executando apenas TAREFA 1: Análise abas CUB\n")
            self.audit_results["task1"] = self.task1_analyze_cub_redundancy()

        elif task == 2:
            print("📋 Executando apenas TAREFA 2: Validação geral\n")
            self.audit_results["task2"] = self.task2_validate_all_tabs()

        elif task == 3:
            print("📋 Executando apenas TAREFA 3: Validação essenciais\n")
            self.audit_results["task3"] = self.task3_validate_essentials()

        else:
            # Executar todas as 3 tarefas
            print("📋 Executando TODAS as 3 tarefas\n")
            self.audit_results["task1"] = self.task1_analyze_cub_redundancy()
            self.audit_results["task2"] = self.task2_validate_all_tabs()
            self.audit_results["task3"] = self.task3_validate_essentials()

        # Finalizar
        print("\n" + "=" * 60)
        print("\n✅ Auditoria concluída com sucesso!")
        print(f"\n📁 Relatórios salvos em: {self.reports_dir.absolute()}")
        print("\nArquivos gerados:")
        for report_file in sorted(self.reports_dir.glob("audit_*")):
            print(f"   • {report_file.name}")
        print()


def main() -> int:
    """
    Função principal para execução via CLI.

    Returns:
        Código de saída (0 = sucesso, 1 = erro)
    """
    parser = argparse.ArgumentParser(
        description="Auditoria Google Sheets - Centro de Inteligência CC"
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Modo verbose (mais detalhes)",
    )
    parser.add_argument(
        "--task",
        type=int,
        choices=[1, 2, 3],
        help="Executar tarefa específica (1, 2 ou 3)",
    )

    args = parser.parse_args()

    try:
        # Verificar variáveis de ambiente
        if not os.getenv("GOOGLE_SHEETS_URL"):
            print("❌ ERRO: GOOGLE_SHEETS_URL não configurada no .env")
            return 1

        # Criar auditor e executar
        auditor = AuditGoogleSheets()
        auditor.main(task=args.task)

        return 0

    except KeyboardInterrupt:
        print("\n\n⚠️ Auditoria interrompida pelo usuário")
        return 1

    except Exception as e:
        logger.error("audit_failed", error=str(e))
        print(f"\n❌ ERRO: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
