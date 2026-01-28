#!/usr/bin/env python3
"""
🚀 IMPLEMENTAÇÃO FASE 1 - FONTES CRÍTICAS CBIC
==============================================

Implementa as fontes mais críticas do CBIC para criar uma base sólida
do sistema de BI antes da expansão completa.

FONTES FASE 1 (CRÍTICAS):
1. CUB Global Oneroso (mensal)
2. CUB Global por UF (mensal) 
3. CUB Global Desonerado (mensal)
4. PIB Brasil (trimestral)
5. PIB Construção Civil (trimestral)
6. Investimento Construção Civil (trimestral)
7. Investimento Infraestrutura (trimestral)
8. Participação Construção no PIB (trimestral)
9. Consumo Cimento (mensal)
10. Produção Cimento (mensal)
11. IPCA (mensal)
12. SELIC (diário)
13. Taxa Desemprego (mensal)

TOTAL: 13 fontes críticas → 13 novas abas Google Sheets

Autor: matheusoption-bit
Data: 2025-11-14
"""

import os
import sys
import time
import requests
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

# Constantes
SPREADSHEET_ID = "11-KC18ShMKXZOSbWvHcLHJwz3oDjexGQLb26xm2Wq4w"
CREDENTIALS_PATH = "config/google_credentials.json"

# Fontes críticas da Fase 1
FONTES_FASE1 = {
    "cub_global_oneroso": {
        "url": "http://www.cbicdados.com.br/media/anexos/tabela_06.A.01_BI_54.xlsx",
        "aba_destino": "cub_on_global",
        "descricao": "CUB Global - Série Histórica",
        "frequencia": "mensal",
        "colunas_esperadas": ["data_referencia", "tipo_cub", "valor_m2"],
        "cor_aba": {"red": 0.20, "green": 0.66, "blue": 0.33}
    },
    "cub_global_uf": {
        "url": "http://www.cbicdados.com.br/media/anexos/tabela_06.A.06_BI_53.xlsx",
        "aba_destino": "cub_on_global_uf",
        "descricao": "CUB por UF - Global",
        "frequencia": "mensal",
        "colunas_esperadas": ["data_referencia", "uf", "tipo_cub", "valor_m2"],
        "cor_aba": {"red": 0.20, "green": 0.66, "blue": 0.33}
    },
    "cub_global_desonerado": {
        "url": "http://www.cbicdados.com.br/media/anexos/tabela_06.B.01_BI_53.xlsx",
        "aba_destino": "cub_des_global",
        "descricao": "CUB Global Desonerado",
        "frequencia": "mensal",
        "colunas_esperadas": ["data_referencia", "tipo_cub", "valor_m2"],
        "cor_aba": {"red": 0.98, "green": 0.74, "blue": 0.02}
    },
    "pib_brasil": {
        "url": "http://www.cbicdados.com.br/media/anexos/tabela_02.D.01_44.xlsx",
        "aba_destino": "pib_brasil_serie",
        "descricao": "PIB Brasil - Série Histórica",
        "frequencia": "trimestral",
        "colunas_esperadas": ["data_referencia", "pib_valor", "variacao_trim", "variacao_anual"],
        "cor_aba": {"red": 0.92, "green": 0.26, "blue": 0.21}
    },
    "pib_construcao": {
        "url": "http://www.cbicdados.com.br/media/anexos/tabela_02.D.02_44.xlsx",
        "aba_destino": "pib_construcao_civil",
        "descricao": "PIB Construção Civil",
        "frequencia": "trimestral",
        "colunas_esperadas": ["data_referencia", "pib_construcao", "variacao_trim", "variacao_anual"],
        "cor_aba": {"red": 0.92, "green": 0.26, "blue": 0.21}
    },
    "investimento_construcao": {
        "url": "http://www.cbicdados.com.br/media/anexos/tabela_02.D.04_44.xlsx",
        "aba_destino": "inv_construcao_civil",
        "descricao": "Investimento em Construção Civil",
        "frequencia": "trimestral",
        "colunas_esperadas": ["data_referencia", "investimento_valor", "variacao_trim", "variacao_anual"],
        "cor_aba": {"red": 0.92, "green": 0.26, "blue": 0.21}
    },
    "investimento_infraestrutura": {
        "url": "http://www.cbicdados.com.br/media/anexos/tabela_02.D.14_19.xlsx",
        "aba_destino": "inv_infraestrutura",
        "descricao": "Investimento em Infraestrutura",
        "frequencia": "trimestral",
        "colunas_esperadas": ["data_referencia", "investimento_infra", "variacao_trim", "variacao_anual"],
        "cor_aba": {"red": 0.92, "green": 0.26, "blue": 0.21}
    },
    "participacao_construcao_pib": {
        "url": "http://www.cbicdados.com.br/media/anexos/tabela_02.D.10_44.xlsx",
        "aba_destino": "pib_part_construcao",
        "descricao": "Participação da Construção no PIB",
        "frequencia": "trimestral",
        "colunas_esperadas": ["data_referencia", "participacao_percentual", "valor_absoluto"],
        "cor_aba": {"red": 0.92, "green": 0.26, "blue": 0.21}
    },
    "consumo_cimento": {
        "url": "http://www.cbicdados.com.br/media/anexos/tabela_07.A.03_Consumo_cimento_54.xlsx",
        "aba_destino": "mat_cimento_consumo",
        "descricao": "Consumo de Cimento",
        "frequencia": "mensal",
        "colunas_esperadas": ["data_referencia", "consumo_toneladas", "variacao_mensal", "variacao_anual"],
        "cor_aba": {"red": 0.61, "green": 0.61, "blue": 0.61}
    },
    "producao_cimento": {
        "url": "http://www.cbicdados.com.br/media/anexos/tabela_07.A.04_Produ%C3%A7ao_cimento_53.xlsx",
        "aba_destino": "mat_cimento_producao",
        "descricao": "Produção de Cimento",
        "frequencia": "mensal",
        "colunas_esperadas": ["data_referencia", "producao_toneladas", "variacao_mensal", "variacao_anual"],
        "cor_aba": {"red": 0.61, "green": 0.61, "blue": 0.61}
    },
    "ipca": {
        "url": "http://www.cbicdados.com.br/media/anexos/tabela_09.B.02_n_70.xlsx",
        "aba_destino": "ind_ipca_consumidor",
        "descricao": "Índice de Preços Consumidor (IPCA)",
        "frequencia": "mensal",
        "colunas_esperadas": ["data_referencia", "ipca_valor", "variacao_mensal", "variacao_anual"],
        "cor_aba": {"red": 0.15, "green": 0.68, "blue": 0.68}
    },
    "selic": {
        "url": "http://www.cbicdados.com.br/media/anexos/tabela_09.B.04_n_67.xlsx",
        "aba_destino": "ind_taxa_selic",
        "descricao": "Taxa de Juros (SELIC)",
        "frequencia": "diario",
        "colunas_esperadas": ["data_referencia", "taxa_selic", "variacao_diaria"],
        "cor_aba": {"red": 0.15, "green": 0.68, "blue": 0.68}
    },
    "desemprego": {
        "url": "http://www.cbicdados.com.br/media/anexos/tabela_09.B.06_n_595.xls",
        "aba_destino": "ind_taxa_desemprego",
        "descricao": "Taxa de Desemprego",
        "frequencia": "mensal",
        "colunas_esperadas": ["data_referencia", "taxa_desemprego", "variacao_mensal"],
        "cor_aba": {"red": 0.15, "green": 0.68, "blue": 0.68}
    }
}

class CBICETLProcessor:
    """Processador ETL para dados CBIC."""
    
    def __init__(self):
        self.setup_sheets_client()
        self.successful_extractions = 0
        self.failed_extractions = 0
        
    def setup_sheets_client(self):
        """Configura cliente Google Sheets."""
        print("🔗 Configurando cliente Google Sheets...")
        
        scope = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        
        creds = Credentials.from_service_account_file(CREDENTIALS_PATH, scopes=scope)
        self.sheets_client = gspread.authorize(creds)
        self.spreadsheet = self.sheets_client.open_by_key(SPREADSHEET_ID)
        
        print("✅ Cliente Google Sheets configurado")
    
    def extract_data_from_url(self, url: str, fonte_key: str) -> Optional[pd.DataFrame]:
        """
        Extrai dados de uma URL CBIC.
        
        Args:
            url: URL do arquivo Excel/CSV
            fonte_key: Chave identificadora da fonte
            
        Returns:
            DataFrame com dados extraídos ou None se falha
        """
        print(f"📥 Extraindo dados: {fonte_key}")
        print(f"   URL: {url}")
        
        try:
            # Determina tipo de arquivo
            if url.endswith('.xlsx'):
                df = pd.read_excel(url)
            elif url.endswith('.xls'):
                df = pd.read_excel(url)
            elif url.endswith('.csv'):
                df = pd.read_csv(url)
            else:
                print(f"   ⚠️ Tipo de arquivo não suportado: {url}")
                return None
            
            print(f"   ✅ Extraídos: {len(df)} registros, {len(df.columns)} colunas")
            return df
            
        except Exception as e:
            print(f"   ❌ Erro na extração: {str(e)}")
            return None
    
    def transform_data(self, df: pd.DataFrame, fonte_key: str, fonte_info: Dict) -> pd.DataFrame:
        """
        Transforma dados para formato padronizado.
        
        Args:
            df: DataFrame original
            fonte_key: Chave da fonte
            fonte_info: Informações da fonte
            
        Returns:
            DataFrame transformado
        """
        print(f"🔄 Transformando dados: {fonte_key}")
        
        if df is None or df.empty:
            return df
        
        # Cria cópia para transformação
        df_transformed = df.copy()
        
        # Adiciona metadados
        df_transformed['fonte_cbic'] = fonte_key
        df_transformed['descricao_fonte'] = fonte_info['descricao']
        df_transformed['frequencia'] = fonte_info['frequencia']
        df_transformed['data_extracao'] = datetime.now()
        df_transformed['versao_pipeline'] = "1.0.0"
        
        # Padroniza nomes de colunas (remove espaços, caracteres especiais)
        df_transformed.columns = [
            col.lower().replace(' ', '_').replace('-', '_').replace('(', '').replace(')', '')
            for col in df_transformed.columns
        ]
        
        # Tenta identificar coluna de data
        date_columns = [col for col in df_transformed.columns if 
                       'data' in col or 'date' in col or 'mes' in col or 'ano' in col]
        
        if date_columns:
            try:
                df_transformed[date_columns[0]] = pd.to_datetime(df_transformed[date_columns[0]], errors='coerce')
                print(f"   📅 Coluna de data processada: {date_columns[0]}")
            except:
                print(f"   ⚠️ Erro ao processar data: {date_columns[0]}")
        
        print(f"   ✅ Dados transformados: {len(df_transformed)} registros")
        return df_transformed
    
    def load_to_sheets(self, df: pd.DataFrame, aba_name: str, cor_aba: Dict) -> bool:
        """
        Carrega dados no Google Sheets.
        
        Args:
            df: DataFrame para carregar
            aba_name: Nome da aba de destino
            cor_aba: Cor da aba (RGB dict)
            
        Returns:
            True se sucesso, False caso contrário
        """
        print(f"📤 Carregando dados na aba: {aba_name}")
        
        if df is None or df.empty:
            print(f"   ⚠️ DataFrame vazio - pulando")
            return False
        
        try:
            # Criar ou limpar aba
            try:
                worksheet = self.spreadsheet.worksheet(aba_name)
                worksheet.clear()
                print(f"   🔄 Aba '{aba_name}' limpa")
            except gspread.WorksheetNotFound:
                worksheet = self.spreadsheet.add_worksheet(
                    title=aba_name, 
                    rows=len(df) + 100, 
                    cols=len(df.columns) + 10
                )
                print(f"   ➕ Nova aba '{aba_name}' criada")
            
            # Preparar dados para upload
            headers = df.columns.tolist()
            values = [headers] + df.fillna('').astype(str).values.tolist()
            
            # Upload em lotes para evitar timeout
            batch_size = 100
            for i in range(0, len(values), batch_size):
                batch = values[i:i+batch_size]
                start_row = i + 1
                end_row = start_row + len(batch) - 1
                
                range_name = f"A{start_row}:Z{end_row}"
                worksheet.update(values=batch, range_name=range_name)
                
                print(f"   📦 Lote {i//batch_size + 1}: linhas {start_row}-{end_row}")
                time.sleep(1)  # Evitar rate limits
            
            # Formatação do header
            worksheet.format("A1:Z1", {
                "backgroundColor": cor_aba,
                "textFormat": {"foregroundColor": {"red": 1, "green": 1, "blue": 1}, "bold": True}
            })
            
            print(f"   ✅ Upload concluído: {len(df)} linhas em '{aba_name}'")
            return True
            
        except Exception as e:
            print(f"   ❌ Erro no upload: {str(e)}")
            return False
    
    def process_fonte(self, fonte_key: str, fonte_info: Dict) -> bool:
        """
        Processa uma fonte CBIC completa (ETL).
        
        Args:
            fonte_key: Chave da fonte
            fonte_info: Dicionário com informações da fonte
            
        Returns:
            True se processamento bem-sucedido
        """
        print(f"\n{'='*60}")
        print(f"🚀 PROCESSANDO FONTE: {fonte_key.upper()}")
        print(f"📋 Descrição: {fonte_info['descricao']}")
        print(f"⏱️ Frequência: {fonte_info['frequencia']}")
        print(f"🎯 Aba destino: {fonte_info['aba_destino']}")
        print(f"{'='*60}")
        
        # Extract
        df = self.extract_data_from_url(fonte_info['url'], fonte_key)
        if df is None:
            self.failed_extractions += 1
            return False
        
        # Transform
        df_transformed = self.transform_data(df, fonte_key, fonte_info)
        if df_transformed is None:
            self.failed_extractions += 1
            return False
        
        # Load
        success = self.load_to_sheets(
            df_transformed, 
            fonte_info['aba_destino'], 
            fonte_info['cor_aba']
        )
        
        if success:
            self.successful_extractions += 1
            print(f"✅ Fonte {fonte_key} processada com sucesso!")
        else:
            self.failed_extractions += 1
            print(f"❌ Fonte {fonte_key} falhou no processamento!")
        
        return success
    
    def run_fase1_complete(self):
        """Executa processamento completo da Fase 1."""
        print("🚀 INICIANDO FASE 1 - FONTES CRÍTICAS CBIC")
        print(f"📅 Data/Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"📊 Total de fontes: {len(FONTES_FASE1)}")
        
        start_time = datetime.now()
        
        # Processa cada fonte
        for fonte_key, fonte_info in FONTES_FASE1.items():
            try:
                self.process_fonte(fonte_key, fonte_info)
                time.sleep(2)  # Pausa entre fontes para evitar sobrecarga
            except KeyboardInterrupt:
                print("\n⏹️ Processamento interrompido pelo usuário")
                break
            except Exception as e:
                print(f"\n❌ Erro inesperado na fonte {fonte_key}: {str(e)}")
                self.failed_extractions += 1
                continue
        
        # Relatório final
        end_time = datetime.now()
        duration = end_time - start_time
        
        print(f"\n{'='*70}")
        print(f"📊 RELATÓRIO FINAL - FASE 1")
        print(f"{'='*70}")
        print(f"⏱️ Duração total: {duration}")
        print(f"✅ Fontes processadas com sucesso: {self.successful_extractions}")
        print(f"❌ Fontes com falha: {self.failed_extractions}")
        print(f"📈 Taxa de sucesso: {self.successful_extractions/(self.successful_extractions + self.failed_extractions)*100:.1f}%")
        print(f"🔗 Planilha: https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}")
        
        if self.successful_extractions > 0:
            print(f"\n🎉 FASE 1 CONCLUÍDA COM SUCESSO!")
            print(f"📊 {self.successful_extractions} novas abas criadas no Google Sheets")
            print(f"🚀 Sistema de BI CBIC expandido significativamente!")
        else:
            print(f"\n❌ FASE 1 FALHOU - Nenhuma fonte processada com sucesso")
        
        print(f"{'='*70}\n")

def main():
    """Função principal da Fase 1."""
    processor = CBICETLProcessor()
    processor.run_fase1_complete()

if __name__ == "__main__":
    main()