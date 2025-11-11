"""
Script para upload automático de dados corrigidos para Google Sheets.

Faz upload do DataFrame fact_cub_detalhado_CORRIGIDO_V3.md para Google Sheets,
substituindo completamente o conteúdo da aba especificada.

Uso:
    python -m src.scripts.upload_to_google_sheets
    python -m src.scripts.upload_to_google_sheets --file custom.md --tab-name nova_aba
    python -m src.scripts.upload_to_google_sheets --dry-run

Requisitos:
    - Arquivo .env com GOOGLE_SHEETS_CREDENTIALS_PATH e GOOGLE_SHEETS_SPREADSHEET_ID
    - Service Account JSON com permissões de edição na planilha
    - Planilha compartilhada com o email do Service Account
"""

import sys
import argparse
import time
from pathlib import Path
from typing import Optional

import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv
import os
import structlog

# Configurar logger
logger = structlog.get_logger(__name__)


def authenticate_gspread(credentials_path: str) -> gspread.Client:
    """
    Autentica no Google Sheets usando Service Account.
    
    Args:
        credentials_path: Caminho para o arquivo JSON de credenciais
        
    Returns:
        Cliente gspread autenticado
        
    Raises:
        FileNotFoundError: Se arquivo de credenciais não existe
        ValueError: Se credenciais são inválidas
    """
    print("🔐 Autenticando Google Sheets...")
    
    if not Path(credentials_path).exists():
        raise FileNotFoundError(
            f"Arquivo de credenciais não encontrado: {credentials_path}"
        )
    
    try:
        # Definir escopos necessários
        scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        
        # Carregar credenciais
        credentials = Credentials.from_service_account_file(
            credentials_path,
            scopes=scopes
        )
        
        # Autenticar
        client = gspread.authorize(credentials)
        
        print("  ✅ Autenticação bem-sucedida!\n")
        logger.info("google_sheets_authenticated", credentials_path=credentials_path)
        
        return client
        
    except Exception as e:
        logger.error("google_sheets_auth_failed", error=str(e))
        raise ValueError(f"Falha na autenticação: {str(e)}")


def upload_dataframe_to_sheet(
    df: pd.DataFrame,
    client: gspread.Client,
    spreadsheet_id: str,
    sheet_name: str,
    dry_run: bool = False
) -> dict:
    """
    Faz upload de DataFrame para Google Sheets.
    
    Args:
        df: DataFrame a ser enviado
        client: Cliente gspread autenticado
        spreadsheet_id: ID da planilha Google Sheets
        sheet_name: Nome da aba (worksheet)
        dry_run: Se True, apenas simula o upload
        
    Returns:
        Dicionário com estatísticas do upload
        
    Raises:
        ValueError: Se planilha ou aba não existem
    """
    print(f"📤 Preparando upload para aba '{sheet_name}'...")
    
    try:
        # Abrir planilha
        spreadsheet = client.open_by_key(spreadsheet_id)
        logger.info("spreadsheet_opened", 
                   spreadsheet_id=spreadsheet_id,
                   title=spreadsheet.title)
        
        # Verificar se aba existe, criar se necessário
        try:
            worksheet = spreadsheet.worksheet(sheet_name)
            print(f"  ✅ Aba '{sheet_name}' encontrada")
        except gspread.exceptions.WorksheetNotFound:
            if dry_run:
                print(f"  ℹ️  [DRY RUN] Aba '{sheet_name}' seria criada")
                worksheet = None
            else:
                print(f"  ⚠️  Aba '{sheet_name}' não existe, criando...")
                worksheet = spreadsheet.add_worksheet(
                    title=sheet_name,
                    rows=len(df) + 1,
                    cols=len(df.columns)
                )
                logger.info("worksheet_created", sheet_name=sheet_name)
        
        if dry_run:
            print("\n🔍 [DRY RUN] Simulando upload...")
            print(f"  📊 Linhas a enviar: {len(df):,}")
            print(f"  📊 Colunas: {len(df.columns)}")
            print(f"  📊 Colunas: {list(df.columns)}")
            print(f"  📊 Tipos de dados:")
            for col in df.columns:
                non_null = df[col].notna().sum()
                print(f"     • {col}: {df[col].dtype} ({non_null:,} não-nulos)")
            
            return {
                "success": True,
                "dry_run": True,
                "rows": len(df),
                "cols": len(df.columns),
                "time_elapsed": 0.0
            }
        
        # Limpar aba completamente
        print(f"  🧹 Limpando aba '{sheet_name}'...")
        worksheet.clear()
        logger.info("worksheet_cleared", sheet_name=sheet_name)
        
        # Fazer upload
        start_time = time.time()
        print(f"  📤 Enviando {len(df):,} linhas...")
        
        # Converter DataFrame para formato adequado
        # Substituir NaN por string vazia para evitar problemas
        df_upload = df.fillna("")
        
        # Fazer upload do DataFrame
        set_with_dataframe(
            worksheet,
            df_upload,
            include_index=False,
            include_column_header=True,
            resize=True
        )
        
        elapsed = time.time() - start_time
        
        print(f"  ✅ Upload concluído em {elapsed:.2f}s\n")
        
        # Formatar coluna "valor" como número com 2 casas decimais
        print("  🎨 Formatando coluna 'valor'...")
        try:
            # Encontrar índice da coluna "valor"
            valor_col_idx = list(df.columns).index("valor") + 1  # +1 porque Sheets é 1-indexed
            
            # Formatar coluna inteira
            worksheet.format(
                f"{chr(64 + valor_col_idx)}2:{chr(64 + valor_col_idx)}{len(df) + 1}",
                {
                    "numberFormat": {
                        "type": "NUMBER",
                        "pattern": "#,##0.00"
                    }
                }
            )
            print(f"  ✅ Coluna 'valor' formatada\n")
            logger.info("column_formatted", column="valor", pattern="#,##0.00")
        except Exception as e:
            print(f"  ⚠️  Não foi possível formatar coluna 'valor': {str(e)}\n")
            logger.warning("column_format_failed", column="valor", error=str(e))
        
        logger.info("upload_completed",
                   sheet_name=sheet_name,
                   rows=len(df),
                   cols=len(df.columns),
                   time_elapsed=elapsed)
        
        return {
            "success": True,
            "dry_run": False,
            "rows": len(df),
            "cols": len(df.columns),
            "time_elapsed": elapsed,
            "spreadsheet_url": spreadsheet.url
        }
        
    except gspread.exceptions.SpreadsheetNotFound:
        error_msg = f"Planilha não encontrada: {spreadsheet_id}"
        logger.error("spreadsheet_not_found", spreadsheet_id=spreadsheet_id)
        raise ValueError(error_msg)
    except Exception as e:
        logger.error("upload_failed", error=str(e), exc_info=True)
        raise


def load_dataframe(file_path: str) -> pd.DataFrame:
    """
    Carrega DataFrame do arquivo TSV.
    
    Args:
        file_path: Caminho para o arquivo
        
    Returns:
        DataFrame carregado
        
    Raises:
        FileNotFoundError: Se arquivo não existe
        ValueError: Se arquivo está vazio ou malformado
    """
    print(f"📊 Carregando dados de {file_path}...")
    
    if not Path(file_path).exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {file_path}")
    
    try:
        df = pd.read_csv(file_path, sep="\t")
        
        if df.empty:
            raise ValueError(f"Arquivo está vazio: {file_path}")
        
        print(f"  ✅ {len(df):,} linhas carregadas")
        print(f"  ✅ {len(df.columns)} colunas: {list(df.columns)}\n")
        
        logger.info("dataframe_loaded",
                   file_path=file_path,
                   rows=len(df),
                   cols=len(df.columns))
        
        return df
        
    except Exception as e:
        logger.error("dataframe_load_failed", file_path=file_path, error=str(e))
        raise ValueError(f"Erro ao carregar arquivo: {str(e)}")


def main():
    """Função principal do script."""
    
    # Parse argumentos CLI
    parser = argparse.ArgumentParser(
        description="Upload de dados corrigidos para Google Sheets",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos de uso:
  python -m src.scripts.upload_to_google_sheets
  python -m src.scripts.upload_to_google_sheets --file custom.md
  python -m src.scripts.upload_to_google_sheets --tab-name nova_aba
  python -m src.scripts.upload_to_google_sheets --dry-run

Configuração:
  Crie arquivo .env na raiz do projeto com:
    GOOGLE_SHEETS_CREDENTIALS_PATH=config/google_credentials.json
    GOOGLE_SHEETS_SPREADSHEET_ID=1QhLqfPB_yJDipDfDg1-2zPj9cEHCcWi9glv-fFOO_B8
        """
    )
    
    parser.add_argument(
        "--file",
        type=str,
        default="docs/fact_cub_detalhado_CORRIGIDO_V3.md",
        help="Caminho do arquivo a ser enviado (default: docs/fact_cub_detalhado_CORRIGIDO_V3.md)"
    )
    
    parser.add_argument(
        "--sheet-id",
        type=str,
        default=None,
        help="ID da planilha Google Sheets (default: variável de ambiente GOOGLE_SHEETS_SPREADSHEET_ID)"
    )
    
    parser.add_argument(
        "--tab-name",
        type=str,
        default="fact_cub_detalhado",
        help="Nome da aba (worksheet) (default: fact_cub_detalhado)"
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simular upload sem modificar a planilha"
    )
    
    args = parser.parse_args()
    
    # Banner
    print("\n" + "="*80)
    print("  📊 UPLOAD PARA GOOGLE SHEETS")
    print("="*80 + "\n")
    
    try:
        # Carregar variáveis de ambiente
        load_dotenv()
        
        # Obter configurações
        credentials_path = os.getenv("GOOGLE_SHEETS_CREDENTIALS_PATH")
        spreadsheet_id = args.sheet_id or os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID")
        
        if not credentials_path:
            raise ValueError(
                "GOOGLE_SHEETS_CREDENTIALS_PATH não definido no .env"
            )
        
        if not spreadsheet_id:
            raise ValueError(
                "GOOGLE_SHEETS_SPREADSHEET_ID não definido no .env ou --sheet-id"
            )
        
        # Modo dry-run
        if args.dry_run:
            print("🔍 MODO DRY RUN - Nenhuma modificação será feita\n")
        
        # 1. Carregar dados
        df = load_dataframe(args.file)
        
        # 2. Autenticar
        client = authenticate_gspread(credentials_path)
        
        # 3. Fazer upload
        result = upload_dataframe_to_sheet(
            df=df,
            client=client,
            spreadsheet_id=spreadsheet_id,
            sheet_name=args.tab_name,
            dry_run=args.dry_run
        )
        
        # 4. Exibir resultados
        print("="*80)
        print("  ✅ UPLOAD CONCLUÍDO COM SUCESSO!")
        print("="*80 + "\n")
        
        print(f"📊 Estatísticas:")
        print(f"  • Linhas enviadas: {result['rows']:,}")
        print(f"  • Colunas: {result['cols']}")
        if not result['dry_run']:
            print(f"  • Tempo decorrido: {result['time_elapsed']:.2f}s")
            print(f"  • Planilha: {result['spreadsheet_url']}")
            print(f"  • Aba: {args.tab_name}")
        else:
            print(f"  • Modo: DRY RUN (simulação)")
        print()
        
        logger.info("script_completed", result=result)
        sys.exit(0)
        
    except FileNotFoundError as e:
        print(f"\n❌ ERRO: {str(e)}\n")
        logger.error("file_not_found", error=str(e))
        sys.exit(1)
        
    except ValueError as e:
        print(f"\n❌ ERRO: {str(e)}\n")
        logger.error("value_error", error=str(e))
        sys.exit(1)
        
    except Exception as e:
        print(f"\n❌ ERRO FATAL: {str(e)}\n")
        logger.error("fatal_error", error=str(e), exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
