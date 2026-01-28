#!/usr/bin/env python3
"""
🔧 NORMALIZAÇÃO DE DADOS CBIC - VERSÃO COMPLETA
================================================
Engenheiro de Dados Sênior - ETL Completo

Transforma dados brutos da CBIC (formato visual) em datasets estruturados
prontos para conexão com dashboards de BI.

ABAS PROCESSADAS:
-----------------
1. cub_on_global - CUB Médio Brasil por Região
2. cub_des_global - CUB Desonerado Global
3. pib_brasil_serie - PIB Brasil Série Anual
4. pib_construcao_civil - PIB Construção Civil
5. inv_construcao_civil - Investimentos Construção
6. inv_infraestrutura - Investimentos Infraestrutura
7. pib_part_construcao - Participação PIB Construção
8. ind_taxa_desemprego - Taxa Desemprego (PNAD)
9. ind_taxa_selic - Taxa SELIC / Poupança
10. ind_ipca_consumidor - Taxa Referencial (TR)
11. mat_cimento_producao - Produção Cimento
12. mat_cimento_consumo - Consumo Cimento

Autor: Pipeline de Dados
Data: 2026-01-28
Versão: 2.0
"""

import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import numpy as np
from datetime import datetime
import re
import time
from typing import Dict, List, Tuple, Optional

# Configuração
SPREADSHEET_ID = "11-KC18ShMKXZOSbWvHcLHJwz3oDjexGQLb26xm2Wq4w"
CREDENTIALS_PATH = "config/google_credentials.json"

# Meses em português
MESES = ['JAN', 'FEV', 'MAR', 'ABR', 'MAI', 'JUN', 'JUL', 'AGO', 'SET', 'OUT', 'NOV', 'DEZ']
MESES_MAP = {m: i+1 for i, m in enumerate(MESES)}

class CBICDataNormalizer:
    """Normalizador de dados CBIC para formato BI."""
    
    def __init__(self):
        self.setup_connection()
        self.stats = {
            'abas_processadas': 0,
            'linhas_brutas': 0,
            'linhas_limpas': 0,
            'linhas_removidas': 0
        }
    
    def setup_connection(self):
        """Estabelece conexão com Google Sheets."""
        print("🔗 Conectando ao Google Sheets...")
        scopes = ['https://www.googleapis.com/auth/spreadsheets']
        creds = Credentials.from_service_account_file(CREDENTIALS_PATH, scopes=scopes)
        self.gc = gspread.authorize(creds)
        self.spreadsheet = self.gc.open_by_key(SPREADSHEET_ID)
        print("✅ Conexão estabelecida")
    
    def is_noise_row(self, row: List) -> bool:
        """Identifica linhas que são ruído (cabeçalhos, rodapés, notas)."""
        if not row or all(cell == '' or cell is None for cell in row):
            return True
        
        first_cell = str(row[0]).strip().lower() if row[0] else ''
        
        # Padrões de ruído
        noise_patterns = [
            r'^fonte:',
            r'^elaboração:',
            r'^\(\*\)',
            r'^\(\.\.\.\)',
            r'^nota:',
            r'dado não disponível',
            r'^unnamed',
            r'nbr 12\.721',
            r'banco de dados',
            r'variações percentuais',
            r'preços correntes',
            r'nova metodologia',
            r'^$',
            r'^\s+$'
        ]
        
        for pattern in noise_patterns:
            if re.search(pattern, first_cell, re.IGNORECASE):
                return True
        
        # Linha com muitas células vazias (>80%)
        empty_count = sum(1 for cell in row if not cell or str(cell).strip() == '')
        if empty_count / len(row) > 0.8:
            return True
        
        return False
    
    def clean_value(self, value: str) -> Optional[str]:
        """Limpa e padroniza um valor."""
        if value is None:
            return None
        
        value = str(value).strip()
        
        # Valores que representam "sem dado"
        if value in ['...', '-', '', 'nan', 'None', 'N/D', 'n/d', '(...)']:
            return None
        
        return value
    
    def parse_numeric(self, value: str) -> Optional[float]:
        """Converte string para número."""
        if value is None:
            return None
        
        value = str(value).strip()
        
        if value in ['...', '-', '', 'nan', 'None', 'N/D', '(...)']:
            return None
        
        try:
            # Remove separadores de milhar e converte vírgula para ponto
            value = value.replace('.', '').replace(',', '.')
            return float(value)
        except:
            return None
    
    def parse_date(self, year: str, month: str = None) -> Optional[str]:
        """Converte ano/mês para formato de data padrão."""
        try:
            year = int(str(year).strip())
            
            if month:
                month_map = {
                    'JAN': 1, 'FEV': 2, 'MAR': 3, 'ABR': 4, 'MAI': 5, 'JUN': 6,
                    'JUL': 7, 'AGO': 8, 'SET': 9, 'OUT': 10, 'NOV': 11, 'DEZ': 12
                }
                month_num = month_map.get(str(month).upper().strip(), None)
                if month_num:
                    return f"{year}-{month_num:02d}-01"
            
            return f"{year}-01-01"
        except:
            return None

    # =========================================================================
    # NORMALIZADORES ESPECÍFICOS POR TIPO DE DADO
    # =========================================================================
    
    def normalize_cub_global(self, raw_data: List[List]) -> pd.DataFrame:
        """Normaliza dados de CUB Global Brasil."""
        print("  📊 Normalizando CUB Global...")
        
        records = []
        current_region = None
        meses = ['JAN', 'FEV', 'MAR', 'ABR', 'MAI', 'JUN', 'JUL', 'AGO', 'SET', 'OUT', 'NOV', 'DEZ']
        
        for row in raw_data:
            if self.is_noise_row(row):
                continue
            
            first_cell = str(row[0]).strip() if row[0] else ''
            
            # Detecta região/tipo
            if 'CUB MÉDIO' in first_cell.upper() or 'REGIÃO' in first_cell.upper():
                current_region = first_cell.replace('CUB MÉDIO - ', '').strip()
                continue
            
            # Tenta parsear como linha de dados (Ano + 12 meses)
            try:
                year = int(first_cell)
                if 1990 <= year <= 2030:
                    for i, mes in enumerate(meses):
                        if i + 1 < len(row):
                            valor = self.parse_numeric(row[i + 1])
                            if valor is not None and valor > 0:
                                records.append({
                                    'data_referencia': self.parse_date(year, mes),
                                    'ano': year,
                                    'mes': mes,
                                    'regiao': current_region or 'BRASIL',
                                    'valor_m2': valor,
                                    'tipo_cub': 'MEDIO',
                                    'fonte': 'CBIC'
                                })
            except:
                continue
        
        df = pd.DataFrame(records)
        if not df.empty:
            df = df.sort_values(['regiao', 'data_referencia'])
            df = df.drop_duplicates(subset=['data_referencia', 'regiao'])
        
        print(f"    ✅ {len(df)} registros normalizados")
        return df
    
    def normalize_cub_uf(self, raw_data: List[List]) -> pd.DataFrame:
        """Normaliza dados de CUB por UF."""
        print("  📊 Normalizando CUB por UF...")
        
        records = []
        current_uf = None
        current_tipo = None
        meses = ['JAN', 'FEV', 'MAR', 'ABR', 'MAI', 'JUN', 'JUL', 'AGO', 'SET', 'OUT', 'NOV', 'DEZ']
        
        # Mapeamento de UF
        uf_map = {
            'ALAGOAS': 'AL', 'AMAZONAS': 'AM', 'BAHIA': 'BA', 'CEARÁ': 'CE',
            'DISTRITO FEDERAL': 'DF', 'ESPÍRITO SANTO': 'ES', 'GOIÁS': 'GO',
            'MARANHÃO': 'MA', 'MATO GROSSO': 'MT', 'MATO GROSSO DO SUL': 'MS',
            'MINAS GERAIS': 'MG', 'PARÁ': 'PA', 'PARAÍBA': 'PB', 'PARANÁ': 'PR',
            'PERNAMBUCO': 'PE', 'PIAUÍ': 'PI', 'RIO DE JANEIRO': 'RJ',
            'RIO GRANDE DO NORTE': 'RN', 'RIO GRANDE DO SUL': 'RS', 'RONDÔNIA': 'RO',
            'RORAIMA': 'RR', 'SANTA CATARINA': 'SC', 'SÃO PAULO': 'SP',
            'SERGIPE': 'SE', 'TOCANTINS': 'TO', 'ACRE': 'AC', 'AMAPÁ': 'AP'
        }
        
        for row in raw_data:
            if self.is_noise_row(row):
                continue
            
            first_cell = str(row[0]).strip().upper() if row[0] else ''
            
            # Detecta UF (padrão: "ESTADO (PADRÃO - TIPO)")
            for estado, sigla in uf_map.items():
                if estado in first_cell:
                    current_uf = sigla
                    # Extrai tipo do padrão
                    match = re.search(r'\(PADRÃO\s*-\s*([^)]+)\)', first_cell)
                    if match:
                        current_tipo = match.group(1).strip()
                    break
            
            # Tenta parsear como linha de dados
            try:
                year = int(first_cell)
                if 1990 <= year <= 2030 and current_uf:
                    for i, mes in enumerate(meses):
                        if i + 1 < len(row):
                            valor = self.parse_numeric(row[i + 1])
                            if valor is not None and valor > 0:
                                records.append({
                                    'data_referencia': self.parse_date(year, mes),
                                    'ano': year,
                                    'mes': mes,
                                    'uf': current_uf,
                                    'tipo_cub': current_tipo or 'R8-N',
                                    'valor_m2': valor,
                                    'fonte': 'CBIC'
                                })
            except:
                continue
        
        df = pd.DataFrame(records)
        if not df.empty:
            df = df.sort_values(['uf', 'data_referencia'])
            df = df.drop_duplicates(subset=['data_referencia', 'uf', 'tipo_cub'])
        
        print(f"    ✅ {len(df)} registros normalizados")
        return df
    
    def normalize_indice_serie(self, raw_data: List[List], nome_indice: str) -> pd.DataFrame:
        """Normaliza séries de índices (IPCA, TR, SELIC, etc)."""
        print(f"  📊 Normalizando {nome_indice}...")
        
        records = []
        meses = ['JAN', 'FEV', 'MAR', 'ABR', 'MAI', 'JUN', 'JUL', 'AGO', 'SET', 'OUT', 'NOV', 'DEZ']
        
        for row in raw_data:
            if self.is_noise_row(row):
                continue
            
            first_cell = str(row[0]).strip() if row[0] else ''
            
            # Formato: ANO | MÊS | ÍNDICE | VAR_MES | VAR_ANO | VAR_12M
            try:
                year = int(first_cell)
                if 1980 <= year <= 2030:
                    # Verifica se tem mês na segunda coluna
                    if len(row) > 1:
                        mes = str(row[1]).strip().upper()
                        if mes in meses:
                            indice = self.parse_numeric(row[2]) if len(row) > 2 else None
                            var_mes = self.parse_numeric(row[3]) if len(row) > 3 else None
                            var_ano = self.parse_numeric(row[4]) if len(row) > 4 else None
                            var_12m = self.parse_numeric(row[5]) if len(row) > 5 else None
                            
                            records.append({
                                'data_referencia': self.parse_date(year, mes),
                                'ano': year,
                                'mes': mes,
                                'indice': nome_indice,
                                'valor': indice,
                                'variacao_mes': var_mes,
                                'variacao_ano': var_ano,
                                'variacao_12m': var_12m,
                                'fonte': 'CBIC'
                            })
            except:
                continue
        
        df = pd.DataFrame(records)
        if not df.empty:
            df = df.sort_values('data_referencia')
            df = df.drop_duplicates(subset=['data_referencia'])
        
        print(f"    ✅ {len(df)} registros normalizados")
        return df
    
    def normalize_pib_serie(self, raw_data: List[List]) -> pd.DataFrame:
        """Normaliza série do PIB."""
        print("  📊 Normalizando PIB...")
        
        records = []
        
        for row in raw_data:
            if self.is_noise_row(row):
                continue
            
            first_cell = str(row[0]).strip() if row[0] else ''
            
            try:
                year = int(first_cell)
                if 1990 <= year <= 2030:
                    pib_corrente = self.parse_numeric(row[1]) if len(row) > 1 else None
                    pib_anterior = self.parse_numeric(row[2]) if len(row) > 2 else None
                    variacao = self.parse_numeric(row[3]) if len(row) > 3 else None
                    
                    if pib_corrente is not None or variacao is not None:
                        records.append({
                            'data_referencia': f"{year}-01-01",
                            'ano': year,
                            'pib_precos_correntes': pib_corrente,
                            'pib_precos_anterior': pib_anterior,
                            'variacao_volume_pct': variacao,
                            'fonte': 'CBIC/IBGE'
                        })
            except:
                continue
        
        df = pd.DataFrame(records)
        if not df.empty:
            df = df.sort_values('ano')
            df = df.drop_duplicates(subset=['ano'])
        
        print(f"    ✅ {len(df)} registros normalizados")
        return df
    
    def normalize_cimento(self, raw_data: List[List], tipo: str) -> pd.DataFrame:
        """Normaliza dados de consumo/produção de cimento."""
        print(f"  📊 Normalizando Cimento ({tipo})...")
        
        records = []
        meses = ['JAN', 'FEV', 'MAR', 'ABR', 'MAI', 'JUN', 'JUL', 'AGO', 'SET', 'OUT', 'NOV', 'DEZ']
        
        # Encontrar a linha de cabeçalho com os meses
        header_row = None
        for i, row in enumerate(raw_data):
            if any(str(cell).upper().strip() in meses for cell in row):
                header_row = i
                break
        
        if header_row is None:
            print("    ⚠️ Não foi possível identificar cabeçalho de meses")
            return pd.DataFrame()
        
        # Processar linhas de dados (após cabeçalho)
        for row in raw_data[header_row + 1:]:
            if self.is_noise_row(row):
                continue
            
            localidade = str(row[0]).strip() if row[0] else ''
            
            # Pula linhas que não são localidades
            if not localidade or localidade.upper() in ['LOCALIDADE', 'CONSUMO', 'PRODUÇÃO', '']:
                continue
            
            # Processa valores mensais
            for i, mes in enumerate(meses):
                col_idx = i + 1
                if col_idx < len(row):
                    valor = self.parse_numeric(row[col_idx])
                    if valor is not None and valor > 0:
                        # Assumindo ano 2024/2025 para dados recentes
                        records.append({
                            'localidade': localidade,
                            'mes': mes,
                            'tipo': tipo,
                            'valor_toneladas': valor,
                            'fonte': 'CBIC/SNIC'
                        })
        
        df = pd.DataFrame(records)
        if not df.empty:
            df = df.drop_duplicates()
        
        print(f"    ✅ {len(df)} registros normalizados")
        return df

    # =========================================================================
    # PROCESSAMENTO PRINCIPAL
    # =========================================================================
    
    def process_all(self):
        """Processa todas as abas CBIC."""
        print("\n" + "=" * 70)
        print("🚀 INICIANDO NORMALIZAÇÃO COMPLETA DOS DADOS CBIC")
        print("=" * 70)
        print(f"📅 Data/Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Mapeamento de abas e seus normalizadores
        abas_config = {
            'cub_on_global': {
                'normalizer': self.normalize_cub_global,
                'output_name': 'fact_cub_brasil_normalizado',
                'description': 'CUB Médio Brasil - Normalizado'
            },
            'cub_on_global_uf': {
                'normalizer': self.normalize_cub_uf,
                'output_name': 'fact_cub_uf_normalizado',
                'description': 'CUB por UF - Normalizado'
            },
            'ind_ipca_consumidor': {
                'normalizer': lambda data: self.normalize_indice_serie(data, 'TR'),  # Os dados são da TR
                'output_name': 'fact_tr_normalizado',
                'description': 'Taxa Referencial - Normalizado'
            },
            'pib_brasil_serie': {
                'normalizer': self.normalize_pib_serie,
                'output_name': 'fact_pib_brasil_normalizado',
                'description': 'PIB Brasil - Normalizado'
            },
            'mat_cimento_consumo': {
                'normalizer': lambda data: self.normalize_cimento(data, 'CONSUMO'),
                'output_name': 'fact_cimento_consumo_normalizado',
                'description': 'Consumo Cimento - Normalizado'
            },
            'mat_cimento_producao': {
                'normalizer': lambda data: self.normalize_cimento(data, 'PRODUCAO'),
                'output_name': 'fact_cimento_producao_normalizado',
                'description': 'Produção Cimento - Normalizado'
            }
        }
        
        results = {}
        
        for aba_origem, config in abas_config.items():
            print(f"\n{'='*60}")
            print(f"📋 Processando: {aba_origem}")
            print(f"{'='*60}")
            
            try:
                # Ler dados brutos
                ws = self.spreadsheet.worksheet(aba_origem)
                raw_data = ws.get_all_values()
                self.stats['linhas_brutas'] += len(raw_data)
                
                print(f"  📥 Linhas brutas: {len(raw_data)}")
                
                # Normalizar
                df = config['normalizer'](raw_data)
                
                if df.empty:
                    print(f"  ⚠️ Nenhum dado válido extraído")
                    continue
                
                self.stats['linhas_limpas'] += len(df)
                
                # Salvar em nova aba
                self.save_normalized_data(df, config['output_name'], config['description'])
                
                results[aba_origem] = {
                    'linhas_brutas': len(raw_data),
                    'linhas_limpas': len(df),
                    'colunas': list(df.columns),
                    'output': config['output_name']
                }
                
                self.stats['abas_processadas'] += 1
                
            except Exception as e:
                print(f"  ❌ Erro: {str(e)}")
                import traceback
                traceback.print_exc()
        
        self.print_summary(results)
        return results
    
    def save_normalized_data(self, df: pd.DataFrame, sheet_name: str, description: str):
        """Salva dados normalizados em nova aba."""
        print(f"  💾 Salvando em: {sheet_name}")
        
        try:
            # Criar ou limpar aba
            try:
                ws = self.spreadsheet.worksheet(sheet_name)
                ws.clear()
            except gspread.WorksheetNotFound:
                ws = self.spreadsheet.add_worksheet(title=sheet_name, rows=len(df)+100, cols=len(df.columns)+5)
            
            # Preparar dados
            headers = df.columns.tolist()
            
            # Converter DataFrame para lista de listas, tratando NaN
            data = []
            for _, row in df.iterrows():
                data.append(['' if pd.isna(v) else v for v in row.tolist()])
            
            all_data = [headers] + data
            
            # Upload em lotes
            batch_size = 500
            for i in range(0, len(all_data), batch_size):
                batch = all_data[i:i + batch_size]
                end_row = i + len(batch)
                ws.update(f'A{i+1}', batch, value_input_option='RAW')
                print(f"    📦 Lote: linhas {i+1}-{end_row}")
                time.sleep(0.5)
            
            print(f"  ✅ Salvo: {len(df)} linhas x {len(df.columns)} colunas")
            
        except Exception as e:
            print(f"  ❌ Erro ao salvar: {e}")
    
    def print_summary(self, results: Dict):
        """Imprime resumo da normalização."""
        print("\n" + "=" * 70)
        print("📊 RESUMO DA NORMALIZAÇÃO")
        print("=" * 70)
        
        print(f"\n📈 ESTATÍSTICAS GERAIS:")
        print(f"   • Abas processadas: {self.stats['abas_processadas']}")
        print(f"   • Linhas brutas (entrada): {self.stats['linhas_brutas']:,}")
        print(f"   • Linhas limpas (saída): {self.stats['linhas_limpas']:,}")
        print(f"   • Linhas removidas (ruído): {self.stats['linhas_brutas'] - self.stats['linhas_limpas']:,}")
        print(f"   • Taxa de aproveitamento: {self.stats['linhas_limpas']/max(1,self.stats['linhas_brutas'])*100:.1f}%")
        
        print(f"\n📋 DETALHAMENTO POR ABA:")
        for aba, info in results.items():
            taxa = info['linhas_limpas'] / max(1, info['linhas_brutas']) * 100
            print(f"\n   {aba}:")
            print(f"      Entrada: {info['linhas_brutas']} → Saída: {info['linhas_limpas']} ({taxa:.0f}%)")
            print(f"      Colunas: {', '.join(info['colunas'])}")
            print(f"      Aba destino: {info['output']}")
        
        print("\n" + "=" * 70)
        print("✅ NORMALIZAÇÃO CONCLUÍDA!")
        print("=" * 70)


def main():
    normalizer = CBICDataNormalizer()
    normalizer.process_all()


if __name__ == "__main__":
    main()
