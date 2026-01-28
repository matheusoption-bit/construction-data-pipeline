#!/usr/bin/env python3
"""
🔧 NORMALIZAÇÃO DE DADOS CBIC - VERSÃO 2.0 COMPLETA
====================================================
Engenheiro de Dados Sênior - ETL Completo e Otimizado

Transforma dados brutos da CBIC (formato visual) em datasets estruturados
prontos para conexão com dashboards de BI.

CARACTERÍSTICAS:
- Detecta automaticamente o formato dos dados
- Remove cabeçalhos, rodapés e notas de fonte
- Converte dados pivotados (meses em colunas) para formato tabular
- Preenche anos em linhas onde está vazio (carry forward)
- Padroniza tipos de dados e valores nulos

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
from typing import Dict, List, Optional

# Configuração
SPREADSHEET_ID = "11-KC18ShMKXZOSbWvHcLHJwz3oDjexGQLb26xm2Wq4w"
CREDENTIALS_PATH = "config/google_credentials.json"

# Meses em português
MESES = ['JAN', 'FEV', 'MAR', 'ABR', 'MAI', 'JUN', 'JUL', 'AGO', 'SET', 'OUT', 'NOV', 'DEZ']
MESES_MAP = {m: i+1 for i, m in enumerate(MESES)}


class CBICNormalizerV2:
    """Normalizador de dados CBIC versão 2.0."""
    
    def __init__(self):
        self.setup_connection()
        self.stats = {
            'abas_processadas': 0,
            'linhas_brutas': 0,
            'linhas_limpas': 0
        }
    
    def setup_connection(self):
        """Estabelece conexão com Google Sheets."""
        print("🔗 Conectando ao Google Sheets...")
        scopes = ['https://www.googleapis.com/auth/spreadsheets']
        creds = Credentials.from_service_account_file(CREDENTIALS_PATH, scopes=scopes)
        self.gc = gspread.authorize(creds)
        self.spreadsheet = self.gc.open_by_key(SPREADSHEET_ID)
        print("✅ Conexão estabelecida\n")
    
    def is_noise_row(self, row: List, strict: bool = True) -> bool:
        """Identifica linhas que são ruído."""
        if not row or all(not str(cell).strip() for cell in row):
            return True
        
        first_cell = str(row[0]).strip().lower() if row[0] else ''
        
        # Palavras que indicam linha de metadados/ruído
        noise_indicators = [
            'fonte:', 'elaboração:', 'nota:', 'observação:', 'obs:',
            'unnamed', 'nbr 12', 'banco de dados', 'variações percentuais',
            'nova metodologia', 'preços correntes', 'dado não disponível',
            'nan', 'nat', 'caderneta_de_poupança', 'taxa_referencial',
            'indicadores_do_pib', 'valor_adicionado', 'custo_unitário'
        ]
        
        for indicator in noise_indicators:
            if indicator in first_cell:
                return True
        
        return False
    
    def clean_value(self, value) -> Optional[str]:
        """Limpa valor string."""
        if value is None:
            return None
        value = str(value).strip()
        if value in ['...', '-', '', 'nan', 'None', 'N/D', '(...)']:
            return None
        return value
    
    def parse_numeric(self, value) -> Optional[float]:
        """Converte para número."""
        if value is None:
            return None
        value = str(value).strip()
        if value in ['...', '-', '', 'nan', 'None', 'N/D', '(...)']:
            return None
        try:
            # Formatos brasileiros
            value = value.replace('.', '').replace(',', '.')
            return float(value)
        except:
            return None
    
    def make_date(self, year: int, month: int = 1) -> str:
        """Cria data no formato ISO."""
        return f"{year}-{month:02d}-01"
    
    # =========================================================================
    # NORMALIZADORES ESPECÍFICOS
    # =========================================================================
    
    def normalize_indice_mensal(self, raw_data: List[List], nome_indice: str) -> pd.DataFrame:
        """
        Normaliza séries de índices com formato:
        ANO | MÊS | ÍNDICE | VAR_MES | VAR_ANO | VAR_12M
        
        O ano aparece apenas na primeira linha do ano (Jan), depois fica vazio.
        """
        print(f"  📊 Normalizando índice mensal: {nome_indice}...")
        
        records = []
        current_year = None
        
        for row in raw_data:
            if self.is_noise_row(row):
                continue
            
            first_cell = str(row[0]).strip() if row else ''
            second_cell = str(row[1]).strip().upper() if len(row) > 1 else ''
            
            # Detecta ano (pode estar na primeira ou segunda coluna)
            try:
                year_candidate = int(first_cell)
                if 1980 <= year_candidate <= 2030:
                    current_year = year_candidate
            except:
                pass
            
            # Se não temos ano, ignora
            if current_year is None:
                continue
            
            # Verifica se segunda coluna é mês válido
            if second_cell not in MESES:
                continue
            
            # Extrai dados
            indice = self.parse_numeric(row[2]) if len(row) > 2 else None
            var_mes = self.parse_numeric(row[3]) if len(row) > 3 else None
            var_ano = self.parse_numeric(row[4]) if len(row) > 4 else None
            var_12m = self.parse_numeric(row[5]) if len(row) > 5 else None
            
            # Só inclui se tiver pelo menos um valor
            if any(v is not None for v in [indice, var_mes, var_ano, var_12m]):
                records.append({
                    'data_referencia': self.make_date(current_year, MESES_MAP[second_cell]),
                    'ano': current_year,
                    'mes': second_cell,
                    'mes_num': MESES_MAP[second_cell],
                    'indice': nome_indice,
                    'valor': indice,
                    'variacao_mes': var_mes,
                    'variacao_ano': var_ano,
                    'variacao_12m': var_12m,
                    'fonte': 'CBIC'
                })
        
        df = pd.DataFrame(records)
        if not df.empty:
            df = df.sort_values('data_referencia')
            df = df.drop_duplicates(subset=['data_referencia'])
        
        print(f"    ✅ {len(df)} registros normalizados")
        return df
    
    def normalize_serie_anual(self, raw_data: List[List], nome_serie: str, 
                               colunas_mapa: Dict[int, str]) -> pd.DataFrame:
        """
        Normaliza séries anuais com formato:
        ANO | VALOR1 | VALOR2 | ...
        """
        print(f"  📊 Normalizando série anual: {nome_serie}...")
        
        records = []
        
        for row in raw_data:
            if self.is_noise_row(row):
                continue
            
            first_cell = str(row[0]).strip() if row else ''
            
            # Tenta parsear ano
            try:
                year = int(float(first_cell))
                if not (1950 <= year <= 2030):
                    continue
            except:
                continue
            
            record = {
                'ano': year,
                'data_referencia': self.make_date(year),
                'serie': nome_serie,
                'fonte': 'CBIC'
            }
            
            # Mapeia colunas para campos
            for col_idx, field_name in colunas_mapa.items():
                if col_idx < len(row):
                    record[field_name] = self.parse_numeric(row[col_idx])
            
            # Só inclui se tiver algum valor
            if any(v is not None for k, v in record.items() if k not in ['ano', 'data_referencia', 'serie', 'fonte']):
                records.append(record)
        
        df = pd.DataFrame(records)
        if not df.empty:
            df = df.sort_values('ano')
            df = df.drop_duplicates(subset=['ano'])
        
        print(f"    ✅ {len(df)} registros normalizados")
        return df
    
    def normalize_cimento(self, raw_data: List[List], tipo: str) -> pd.DataFrame:
        """
        Normaliza dados de cimento com formato pivotado:
        LOCALIDADE | JAN | FEV | ... | DEZ | TOTAL
        """
        print(f"  📊 Normalizando cimento ({tipo})...")
        
        records = []
        header_found = False
        col_indices = {}
        ano_ref = 2024  # Ano de referência (ajustar conforme dados)
        
        for row in raw_data:
            row_upper = [str(c).upper().strip() for c in row]
            
            # Encontra header com meses
            if not header_found:
                for i, cell in enumerate(row_upper):
                    if cell in MESES:
                        col_indices[cell] = i
                if col_indices:
                    header_found = True
                continue
            
            if self.is_noise_row(row):
                continue
            
            localidade = str(row[0]).strip() if row else ''
            
            # Ignora linhas sem localidade válida ou que são totais/subtotais
            if not localidade or localidade.upper() in ['TOTAL', 'BRASIL', '', 'LOCALIDADE']:
                continue
            
            # Processa cada mês
            for mes, col_idx in col_indices.items():
                if col_idx < len(row):
                    valor = self.parse_numeric(row[col_idx])
                    if valor is not None and valor > 0:
                        records.append({
                            'data_referencia': self.make_date(ano_ref, MESES_MAP[mes]),
                            'ano': ano_ref,
                            'mes': mes,
                            'mes_num': MESES_MAP[mes],
                            'localidade': localidade.title(),
                            'tipo': tipo,
                            'valor_toneladas': valor,
                            'fonte': 'CBIC/SNIC'
                        })
        
        df = pd.DataFrame(records)
        if not df.empty:
            df = df.sort_values(['localidade', 'mes_num'])
            df = df.drop_duplicates()
        
        print(f"    ✅ {len(df)} registros normalizados")
        return df
    
    def normalize_cub_global(self, raw_data: List[List]) -> pd.DataFrame:
        """
        Normaliza CUB Global com formato:
        Header: [ANO, JAN, FEV, ..., DEZ]
        Data: [2020, 1234.56, 1245.67, ...]
        Com seções por região
        """
        print("  📊 Normalizando CUB Global...")
        
        records = []
        current_region = 'BRASIL'
        
        for row in raw_data:
            if self.is_noise_row(row):
                continue
            
            first_cell = str(row[0]).strip().upper() if row else ''
            
            # Detecta mudança de região
            if 'CUB MÉDIO' in first_cell or 'REGIÃO' in first_cell:
                # Extrai nome da região
                match = re.search(r'REGIÃO\s+(\w+)', first_cell)
                if match:
                    current_region = match.group(1)
                elif 'BRASIL' in first_cell:
                    current_region = 'BRASIL'
                continue
            
            # Tenta parsear como ano
            try:
                year = int(first_cell)
                if not (1990 <= year <= 2030):
                    continue
            except:
                continue
            
            # Processa valores mensais
            for i, mes in enumerate(MESES):
                col_idx = i + 1
                if col_idx < len(row):
                    valor = self.parse_numeric(row[col_idx])
                    if valor is not None and valor > 0:
                        records.append({
                            'data_referencia': self.make_date(year, i + 1),
                            'ano': year,
                            'mes': mes,
                            'mes_num': i + 1,
                            'regiao': current_region,
                            'valor_m2': valor,
                            'tipo_cub': 'MEDIO',
                            'fonte': 'CBIC'
                        })
        
        df = pd.DataFrame(records)
        if not df.empty:
            df = df.sort_values(['regiao', 'data_referencia'])
            df = df.drop_duplicates(subset=['data_referencia', 'regiao'])
        
        print(f"    ✅ {len(df)} registros normalizados")
        return df
    
    def normalize_desemprego(self, raw_data: List[List]) -> pd.DataFrame:
        """Normaliza taxa de desemprego PNAD."""
        print("  📊 Normalizando Desemprego PNAD...")
        
        records = []
        current_year = None
        
        for row in raw_data:
            if self.is_noise_row(row):
                continue
            
            first_cell = str(row[0]).strip() if row else ''
            second_cell = str(row[1]).strip().upper() if len(row) > 1 else ''
            
            # Detecta ano
            try:
                year_candidate = int(float(first_cell))
                if 2000 <= year_candidate <= 2030:
                    current_year = year_candidate
            except:
                pass
            
            if current_year is None:
                continue
            
            # Formato: ANO | TRIMESTRE | TAXA
            # ou: ANO | MÊS | TAXA
            if second_cell in MESES:
                taxa = self.parse_numeric(row[2]) if len(row) > 2 else None
                if taxa is not None:
                    records.append({
                        'data_referencia': self.make_date(current_year, MESES_MAP[second_cell]),
                        'ano': current_year,
                        'mes': second_cell,
                        'mes_num': MESES_MAP[second_cell],
                        'taxa_desemprego': taxa,
                        'fonte': 'CBIC/IBGE'
                    })
            # Verifica trimestre
            elif any(t in second_cell for t in ['TRI', '1T', '2T', '3T', '4T', 'Q1', 'Q2', 'Q3', 'Q4']):
                taxa = self.parse_numeric(row[2]) if len(row) > 2 else None
                if taxa is not None:
                    # Mapeia trimestre para mês central
                    trim_map = {'1': 2, '2': 5, '3': 8, '4': 11}
                    for t, m in trim_map.items():
                        if t in second_cell:
                            records.append({
                                'data_referencia': self.make_date(current_year, m),
                                'ano': current_year,
                                'trimestre': f'T{t}',
                                'mes_num': m,
                                'taxa_desemprego': taxa,
                                'fonte': 'CBIC/IBGE'
                            })
                            break
        
        df = pd.DataFrame(records)
        if not df.empty:
            df = df.sort_values('data_referencia')
            df = df.drop_duplicates(subset=['data_referencia'])
        
        print(f"    ✅ {len(df)} registros normalizados")
        return df

    # =========================================================================
    # PROCESSAMENTO PRINCIPAL
    # =========================================================================
    
    def save_to_sheet(self, df: pd.DataFrame, sheet_name: str):
        """Salva DataFrame em nova aba."""
        if df.empty:
            print(f"  ⚠️ DataFrame vazio, pulando {sheet_name}")
            return
        
        print(f"  💾 Salvando em: {sheet_name}")
        
        try:
            # Criar ou limpar aba
            try:
                ws = self.spreadsheet.worksheet(sheet_name)
                ws.clear()
            except gspread.WorksheetNotFound:
                ws = self.spreadsheet.add_worksheet(title=sheet_name, rows=len(df)+100, cols=len(df.columns)+5)
            
            # Converter NaN para string vazia
            df_clean = df.fillna('')
            
            # Preparar dados
            data = [df_clean.columns.tolist()] + df_clean.values.tolist()
            
            # Upload em lotes
            batch_size = 500
            for i in range(0, len(data), batch_size):
                batch = data[i:i + batch_size]
                ws.update(values=batch, range_name=f'A{i+1}', value_input_option='RAW')
                time.sleep(0.3)
            
            print(f"  ✅ {len(df)} linhas salvas")
            
        except Exception as e:
            print(f"  ❌ Erro ao salvar: {e}")
    
    def process_all(self):
        """Processa todas as abas CBIC."""
        print("=" * 70)
        print("🚀 NORMALIZAÇÃO COMPLETA DOS DADOS CBIC v2.0")
        print("=" * 70)
        print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        
        results = []
        
        # =====================================================================
        # 1. ÍNDICES MENSAIS
        # =====================================================================
        
        # Taxa Referencial (TR) - aba ind_ipca_consumidor tem TR, não IPCA!
        print(f"\n{'='*60}")
        print("📋 [1/10] ind_ipca_consumidor → fact_tr_normalizado")
        print(f"{'='*60}")
        try:
            ws = self.spreadsheet.worksheet('ind_ipca_consumidor')
            raw = ws.get_all_values()
            self.stats['linhas_brutas'] += len(raw)
            df = self.normalize_indice_mensal(raw, 'TAXA_REFERENCIAL')
            if not df.empty:
                self.save_to_sheet(df, 'fact_tr_normalizado')
                results.append(('TR', len(raw), len(df)))
                self.stats['linhas_limpas'] += len(df)
        except Exception as e:
            print(f"  ❌ Erro: {e}")
        
        time.sleep(2)
        
        # Taxa SELIC / Poupança
        print(f"\n{'='*60}")
        print("📋 [2/10] ind_taxa_selic → fact_poupanca_normalizado")
        print(f"{'='*60}")
        try:
            ws = self.spreadsheet.worksheet('ind_taxa_selic')
            raw = ws.get_all_values()
            self.stats['linhas_brutas'] += len(raw)
            df = self.normalize_indice_mensal(raw, 'POUPANCA')
            if not df.empty:
                self.save_to_sheet(df, 'fact_poupanca_normalizado')
                results.append(('Poupança', len(raw), len(df)))
                self.stats['linhas_limpas'] += len(df)
        except Exception as e:
            print(f"  ❌ Erro: {e}")
        
        time.sleep(2)
        
        # Desemprego PNAD
        print(f"\n{'='*60}")
        print("📋 [3/10] ind_taxa_desemprego → fact_desemprego_normalizado")
        print(f"{'='*60}")
        try:
            ws = self.spreadsheet.worksheet('ind_taxa_desemprego')
            raw = ws.get_all_values()
            self.stats['linhas_brutas'] += len(raw)
            df = self.normalize_desemprego(raw)
            if not df.empty:
                self.save_to_sheet(df, 'fact_desemprego_normalizado')
                results.append(('Desemprego', len(raw), len(df)))
                self.stats['linhas_limpas'] += len(df)
        except Exception as e:
            print(f"  ❌ Erro: {e}")
        
        time.sleep(2)
        
        # =====================================================================
        # 2. SÉRIES ANUAIS - PIB
        # =====================================================================
        
        # PIB Brasil
        print(f"\n{'='*60}")
        print("📋 [4/10] pib_brasil_serie → fact_pib_brasil_normalizado")
        print(f"{'='*60}")
        try:
            ws = self.spreadsheet.worksheet('pib_brasil_serie')
            raw = ws.get_all_values()
            self.stats['linhas_brutas'] += len(raw)
            colunas = {1: 'pib_corrente', 2: 'pib_anterior', 3: 'variacao_pct'}
            df = self.normalize_serie_anual(raw, 'PIB_BRASIL', colunas)
            if not df.empty:
                self.save_to_sheet(df, 'fact_pib_brasil_normalizado')
                results.append(('PIB Brasil', len(raw), len(df)))
                self.stats['linhas_limpas'] += len(df)
        except Exception as e:
            print(f"  ❌ Erro: {e}")
        
        time.sleep(2)
        
        # PIB Construção Civil
        print(f"\n{'='*60}")
        print("📋 [5/10] pib_construcao_civil → fact_pib_construcao_normalizado")
        print(f"{'='*60}")
        try:
            ws = self.spreadsheet.worksheet('pib_construcao_civil')
            raw = ws.get_all_values()
            self.stats['linhas_brutas'] += len(raw)
            colunas = {1: 'va_corrente', 2: 'va_anterior', 3: 'variacao_volume_pct'}
            df = self.normalize_serie_anual(raw, 'PIB_CONSTRUCAO', colunas)
            if not df.empty:
                self.save_to_sheet(df, 'fact_pib_construcao_normalizado')
                results.append(('PIB Construção', len(raw), len(df)))
                self.stats['linhas_limpas'] += len(df)
        except Exception as e:
            print(f"  ❌ Erro: {e}")
        
        time.sleep(2)
        
        # Investimentos Construção
        print(f"\n{'='*60}")
        print("📋 [6/10] inv_construcao_civil → fact_inv_construcao_normalizado")
        print(f"{'='*60}")
        try:
            ws = self.spreadsheet.worksheet('inv_construcao_civil')
            raw = ws.get_all_values()
            self.stats['linhas_brutas'] += len(raw)
            colunas = {1: 'fbcf_total', 2: 'fbcf_construcao', 3: 'participacao_pct'}
            df = self.normalize_serie_anual(raw, 'INV_CONSTRUCAO', colunas)
            if not df.empty:
                self.save_to_sheet(df, 'fact_inv_construcao_normalizado')
                results.append(('Invest. Construção', len(raw), len(df)))
                self.stats['linhas_limpas'] += len(df)
        except Exception as e:
            print(f"  ❌ Erro: {e}")
        
        time.sleep(2)
        
        # =====================================================================
        # 3. MATERIAIS - CIMENTO
        # =====================================================================
        
        # Consumo Cimento
        print(f"\n{'='*60}")
        print("📋 [7/10] mat_cimento_consumo → fact_cimento_consumo_normalizado")
        print(f"{'='*60}")
        try:
            ws = self.spreadsheet.worksheet('mat_cimento_consumo')
            raw = ws.get_all_values()
            self.stats['linhas_brutas'] += len(raw)
            df = self.normalize_cimento(raw, 'CONSUMO')
            if not df.empty:
                self.save_to_sheet(df, 'fact_cimento_consumo_normalizado')
                results.append(('Cimento Consumo', len(raw), len(df)))
                self.stats['linhas_limpas'] += len(df)
        except Exception as e:
            print(f"  ❌ Erro: {e}")
        
        time.sleep(2)
        
        # Produção Cimento
        print(f"\n{'='*60}")
        print("📋 [8/10] mat_cimento_producao → fact_cimento_producao_normalizado")
        print(f"{'='*60}")
        try:
            ws = self.spreadsheet.worksheet('mat_cimento_producao')
            raw = ws.get_all_values()
            self.stats['linhas_brutas'] += len(raw)
            df = self.normalize_cimento(raw, 'PRODUCAO')
            if not df.empty:
                self.save_to_sheet(df, 'fact_cimento_producao_normalizado')
                results.append(('Cimento Produção', len(raw), len(df)))
                self.stats['linhas_limpas'] += len(df)
        except Exception as e:
            print(f"  ❌ Erro: {e}")
        
        time.sleep(2)
        
        # =====================================================================
        # 4. CUB
        # =====================================================================
        
        # CUB Global Brasil
        print(f"\n{'='*60}")
        print("📋 [9/10] cub_on_global → fact_cub_brasil_normalizado")
        print(f"{'='*60}")
        try:
            ws = self.spreadsheet.worksheet('cub_on_global')
            raw = ws.get_all_values()
            self.stats['linhas_brutas'] += len(raw)
            df = self.normalize_cub_global(raw)
            if not df.empty:
                self.save_to_sheet(df, 'fact_cub_brasil_normalizado')
                results.append(('CUB Brasil', len(raw), len(df)))
                self.stats['linhas_limpas'] += len(df)
        except Exception as e:
            print(f"  ❌ Erro: {e}")
        
        time.sleep(2)
        
        # CUB Desonerado
        print(f"\n{'='*60}")
        print("📋 [10/10] cub_des_global → fact_cub_desonerado_normalizado")
        print(f"{'='*60}")
        try:
            ws = self.spreadsheet.worksheet('cub_des_global')
            raw = ws.get_all_values()
            self.stats['linhas_brutas'] += len(raw)
            df = self.normalize_cub_global(raw)
            # Ajusta o tipo
            if not df.empty:
                df['tipo_cub'] = 'DESONERADO'
                self.save_to_sheet(df, 'fact_cub_desonerado_normalizado')
                results.append(('CUB Desonerado', len(raw), len(df)))
                self.stats['linhas_limpas'] += len(df)
        except Exception as e:
            print(f"  ❌ Erro: {e}")
        
        # =====================================================================
        # RESUMO FINAL
        # =====================================================================
        self.print_summary(results)
    
    def print_summary(self, results):
        """Imprime resumo."""
        print("\n" + "=" * 70)
        print("📊 RESUMO DA NORMALIZAÇÃO v2.0")
        print("=" * 70)
        
        print(f"\n📈 ESTATÍSTICAS:")
        print(f"   • Total linhas brutas: {self.stats['linhas_brutas']:,}")
        print(f"   • Total linhas limpas: {self.stats['linhas_limpas']:,}")
        print(f"   • Taxa aproveitamento: {self.stats['linhas_limpas']/max(1,self.stats['linhas_brutas'])*100:.1f}%")
        
        print(f"\n📋 RESULTADOS POR FONTE:")
        print(f"   {'Fonte':<25} {'Bruto':>10} {'Limpo':>10} {'Taxa':>10}")
        print(f"   {'-'*55}")
        
        for nome, bruto, limpo in results:
            taxa = limpo / max(1, bruto) * 100
            print(f"   {nome:<25} {bruto:>10} {limpo:>10} {taxa:>9.1f}%")
        
        print("\n" + "=" * 70)
        print("✅ NORMALIZAÇÃO CONCLUÍDA!")
        print("=" * 70)
        print("\n🎯 Abas criadas com dados prontos para BI:")
        print("   • fact_tr_normalizado")
        print("   • fact_poupanca_normalizado")
        print("   • fact_desemprego_normalizado")
        print("   • fact_pib_brasil_normalizado")
        print("   • fact_pib_construcao_normalizado")
        print("   • fact_inv_construcao_normalizado")
        print("   • fact_cimento_consumo_normalizado")
        print("   • fact_cimento_producao_normalizado")
        print("   • fact_cub_brasil_normalizado")
        print("   • fact_cub_desonerado_normalizado")


def main():
    normalizer = CBICNormalizerV2()
    normalizer.process_all()


if __name__ == "__main__":
    main()
