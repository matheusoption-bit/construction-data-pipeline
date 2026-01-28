#!/usr/bin/env python3
"""
🔧 PREPARAÇÃO DE DADOS PARA SUPABASE (PostgreSQL)
==================================================

Script de Consolidação e Limpeza de Dados para Migração

Este script realiza o ETL final para preparar os dados do pipeline
construction-data-pipeline para upload no Supabase (PostgreSQL).

TRANSFORMAÇÕES PRINCIPAIS:
1. CONSOLIDAÇÃO: Múltiplas fontes de CUB → fact_cub unificada
2. VERTICALIZAÇÃO: Indicadores separados → fact_macroeconomia (narrow table)
3. LIMPEZA: Remoção de redundâncias, tratamento de nulos e tipos
4. PADRONIZAÇÃO: Datas ISO, números float, snake_case

OUTPUT:
- data/production_ready/fact_cub.csv
- data/production_ready/fact_macroeconomia.csv
- data/production_ready/dim_metodos_construtivos.csv
- data/production_ready/dim_taxas_municipais.csv
- data/production_ready/dim_localidade.csv

Autor: Pipeline de Dados - construction-data-pipeline
Data: 2026-01-28
Versão: 1.0
"""

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict
import re
import warnings

warnings.filterwarnings('ignore')

# Configuração de diretórios
PROJECT_ROOT = Path(__file__).parent.parent.parent
DATA_CACHE_DIR = PROJECT_ROOT / "data" / "cache"
CONFIG_DIR = PROJECT_ROOT / "configs"
OUTPUT_DIR = PROJECT_ROOT / "data" / "production_ready"

# Configuração Google Sheets (fonte dos dados atuais)
SPREADSHEET_ID = "11-KC18ShMKXZOSbWvHcLHJwz3oDjexGQLb26xm2Wq4w"
CREDENTIALS_PATH = PROJECT_ROOT / "config" / "google_credentials.json"


class DataConsolidator:
    """Classe principal para consolidação de dados."""
    
    def __init__(self, use_sheets: bool = True):
        """
        Inicializa o consolidador.
        
        Args:
            use_sheets: Se True, busca dados do Google Sheets.
                       Se False, usa apenas arquivos locais.
        """
        self.use_sheets = use_sheets
        self.spreadsheet = None
        self.stats = {
            'arquivos_gerados': 0,
            'registros_processados': 0,
            'registros_output': 0
        }
        
        # Criar diretório de output
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        
        if use_sheets:
            self._setup_sheets_connection()
    
    def _setup_sheets_connection(self):
        """Estabelece conexão com Google Sheets."""
        try:
            import gspread
            from google.oauth2.service_account import Credentials
            
            scopes = ['https://www.googleapis.com/auth/spreadsheets.readonly']
            creds = Credentials.from_service_account_file(str(CREDENTIALS_PATH), scopes=scopes)
            gc = gspread.authorize(creds)
            self.spreadsheet = gc.open_by_key(SPREADSHEET_ID)
            print("✅ Conexão com Google Sheets estabelecida")
        except Exception as e:
            print(f"⚠️ Falha na conexão com Sheets: {e}")
            print("   Usando apenas arquivos locais.")
            self.use_sheets = False
    
    def _read_sheet(self, sheet_name: str) -> pd.DataFrame:
        """Lê uma aba do Google Sheets."""
        if not self.spreadsheet:
            return pd.DataFrame()
        
        try:
            ws = self.spreadsheet.worksheet(sheet_name)
            data = ws.get_all_values()
            if data:
                df = pd.DataFrame(data[1:], columns=data[0])
                return df
        except Exception as e:
            print(f"    ⚠️ Erro ao ler {sheet_name}: {e}")
        
        return pd.DataFrame()
    
    # =========================================================================
    # FUNÇÕES DE LIMPEZA E TRANSFORMAÇÃO
    # =========================================================================
    
    def _parse_brazilian_number(self, value) -> Optional[float]:
        """
        Converte número em formato brasileiro para float.
        
        Exemplos:
        - "1.234,56" → 1234.56
        - "1234,56" → 1234.56
        - "1234.56" → 1234.56 (já no formato correto)
        """
        if pd.isna(value) or value == '' or value is None:
            return None
        
        value = str(value).strip()
        
        if value in ['...', '-', 'N/D', 'nan', 'None']:
            return None
        
        try:
            # Detecta formato brasileiro (vírgula como decimal)
            if ',' in value and '.' in value:
                # Formato: 1.234,56 (milhar com ponto, decimal com vírgula)
                value = value.replace('.', '').replace(',', '.')
            elif ',' in value:
                # Formato: 1234,56 (apenas vírgula decimal)
                value = value.replace(',', '.')
            # else: já está no formato correto (1234.56)
            
            return float(value)
        except:
            return None
    
    def _parse_date(self, value) -> Optional[str]:
        """
        Converte data para formato ISO (YYYY-MM-DD).
        
        Aceita formatos:
        - YYYY-MM-DD
        - DD/MM/YYYY
        - YYYY-MM
        """
        if pd.isna(value) or value == '' or value is None:
            return None
        
        value = str(value).strip()
        
        if value in ['NaT', 'nan', 'None', '']:
            return None
        
        try:
            # Tenta parsear diferentes formatos
            for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%Y-%m', '%Y/%m/%d']:
                try:
                    dt = datetime.strptime(value[:10], fmt)
                    return dt.strftime('%Y-%m-%d')
                except:
                    continue
            
            # Se contém apenas ano e mês (ex: "2024-01")
            if re.match(r'^\d{4}-\d{2}$', value):
                return f"{value}-01"
            
            return value  # Retorna original se não conseguir parsear
            
        except:
            return None
    
    def _clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Limpeza básica de DataFrame."""
        # Remove linhas completamente vazias
        df = df.dropna(how='all')
        
        # Remove colunas completamente vazias
        df = df.dropna(axis=1, how='all')
        
        # Padroniza nomes de colunas (snake_case)
        df.columns = [
            re.sub(r'[^a-z0-9]', '_', col.lower().strip())
            .replace('__', '_')
            .strip('_')
            for col in df.columns
        ]
        
        return df
    
    # =========================================================================
    # CONSOLIDAÇÃO DE CUB
    # =========================================================================
    
    def consolidate_cub(self) -> pd.DataFrame:
        """
        Consolida todos os dados de CUB em uma única tabela.
        
        Output: fact_cub com colunas:
        - data_referencia (DATE)
        - uf (VARCHAR - 'BR' para nacional)
        - tipo_cub (VARCHAR - 'R8-N', 'MEDIO', etc.)
        - regime_tributario (VARCHAR - 'ONEROSO', 'DESONERADO')
        - valor_m2 (DECIMAL)
        - variacao_mes (DECIMAL, opcional)
        """
        print("\n📊 Consolidando dados de CUB...")
        
        all_cub_data = []
        
        # 1. CUB Brasil Normalizado (Oneroso)
        df = self._read_sheet('fact_cub_brasil_normalizado')
        if not df.empty:
            df = self._clean_dataframe(df)
            records = []
            for _, row in df.iterrows():
                records.append({
                    'data_referencia': self._parse_date(row.get('data_referencia')),
                    'uf': row.get('regiao', 'BR'),
                    'tipo_cub': row.get('tipo_cub', 'MEDIO'),
                    'regime_tributario': 'ONEROSO',
                    'valor_m2': self._parse_brazilian_number(row.get('valor_m2')),
                    'fonte': 'CBIC'
                })
            all_cub_data.extend(records)
            print(f"    ✓ CUB Brasil Normalizado: {len(records)} registros")
        
        # 2. CUB Desonerado Normalizado
        df = self._read_sheet('fact_cub_desonerado_normalizado')
        if not df.empty:
            df = self._clean_dataframe(df)
            records = []
            for _, row in df.iterrows():
                records.append({
                    'data_referencia': self._parse_date(row.get('data_referencia')),
                    'uf': row.get('regiao', 'BR'),
                    'tipo_cub': row.get('tipo_cub', 'MEDIO'),
                    'regime_tributario': 'DESONERADO',
                    'valor_m2': self._parse_brazilian_number(row.get('valor_m2')),
                    'fonte': 'CBIC'
                })
            all_cub_data.extend(records)
            print(f"    ✓ CUB Desonerado: {len(records)} registros")
        
        # 3. CUB por UF (fact_cub_por_uf)
        df = self._read_sheet('fact_cub_por_uf')
        if not df.empty:
            df = self._clean_dataframe(df)
            records = []
            for _, row in df.iterrows():
                uf = row.get('uf', 'BR')
                # Determina regime baseado no tipo
                tipo = str(row.get('tipo_cub', '')).upper()
                regime = 'DESONERADO' if 'DES' in tipo else 'ONEROSO'
                
                records.append({
                    'data_referencia': self._parse_date(row.get('data_referencia')),
                    'uf': uf if uf and uf != '' else 'BR',
                    'tipo_cub': tipo if tipo else 'R8-N',
                    'regime_tributario': regime,
                    'valor_m2': self._parse_brazilian_number(row.get('valor')),
                    'fonte': 'CBIC'
                })
            all_cub_data.extend(records)
            print(f"    ✓ CUB por UF: {len(records)} registros")
        
        # 4. CUB Histórico
        df = self._read_sheet('fact_cub_historico')
        if not df.empty:
            df = self._clean_dataframe(df)
            records = []
            for _, row in df.iterrows():
                records.append({
                    'data_referencia': self._parse_date(row.get('data_referencia')),
                    'uf': row.get('uf', 'BR'),
                    'tipo_cub': row.get('tipo_cub', 'R8-N'),
                    'regime_tributario': 'ONEROSO',
                    'valor_m2': self._parse_brazilian_number(row.get('custo_m2')),
                    'fonte': 'CBIC_HISTORICO'
                })
            all_cub_data.extend(records)
            print(f"    ✓ CUB Histórico: {len(records)} registros")
        
        # Criar DataFrame consolidado
        df_cub = pd.DataFrame(all_cub_data)
        
        if df_cub.empty:
            print("    ⚠️ Nenhum dado de CUB encontrado")
            return pd.DataFrame()
        
        # Remover duplicatas (priorizar dados mais recentes)
        df_cub = df_cub.drop_duplicates(
            subset=['data_referencia', 'uf', 'tipo_cub', 'regime_tributario'],
            keep='last'
        )
        
        # Remover linhas sem valor
        df_cub = df_cub[df_cub['valor_m2'].notna() & (df_cub['valor_m2'] > 0)]
        
        # Ordenar
        df_cub = df_cub.sort_values(['data_referencia', 'uf', 'tipo_cub'])
        
        # Adicionar ID
        df_cub.insert(0, 'id', range(1, len(df_cub) + 1))
        
        print(f"    ✅ Total consolidado: {len(df_cub)} registros únicos")
        
        self.stats['registros_processados'] += len(all_cub_data)
        self.stats['registros_output'] += len(df_cub)
        
        return df_cub
    
    # =========================================================================
    # CONSOLIDAÇÃO DE INDICADORES MACROECONÔMICOS
    # =========================================================================
    
    def consolidate_macroeconomia(self) -> pd.DataFrame:
        """
        Consolida indicadores macroeconômicos em tabela vertical (narrow table).
        
        Output: fact_macroeconomia com colunas:
        - data_referencia (DATE)
        - indicador (VARCHAR - 'SELIC', 'IPCA', 'INCC', 'TR', etc.)
        - valor (DECIMAL)
        - unidade (VARCHAR - '% a.a.', '% a.m.', 'R$')
        - variacao_mes (DECIMAL, opcional)
        """
        print("\n📊 Consolidando indicadores macroeconômicos...")
        
        all_macro_data = []
        
        # Mapeamento de abas e seus indicadores
        fontes_macro = [
            {
                'sheet': 'taxa_selic',
                'indicador': 'SELIC',
                'unidade': '% a.a.',
                'col_valor': 'valor',
                'col_variacao': 'variacao_mensal'
            },
            {
                'sheet': 'ipca_infla',
                'indicador': 'IPCA',
                'unidade': '% a.m.',
                'col_valor': 'valor',
                'col_variacao': 'variacao_mensal'
            },
            {
                'sheet': 'igp_m_infla',
                'indicador': 'IGP-M',
                'unidade': '% a.m.',
                'col_valor': 'valor',
                'col_variacao': 'variacao_mensal'
            },
            {
                'sheet': 'inpc_infla',
                'indicador': 'INPC',
                'unidade': '% a.m.',
                'col_valor': 'valor',
                'col_variacao': 'variacao_mensal'
            },
            {
                'sheet': 'fact_tr_normalizado',
                'indicador': 'TR',
                'unidade': '% a.m.',
                'col_valor': 'valor',
                'col_variacao': 'variacao_mes'
            },
            {
                'sheet': 'fact_poupanca_normalizado',
                'indicador': 'POUPANCA',
                'unidade': '% a.m.',
                'col_valor': 'valor',
                'col_variacao': 'variacao_mes'
            },
            {
                'sheet': 'taxa_cambio',
                'indicador': 'USD_BRL',
                'unidade': 'R$',
                'col_valor': 'valor',
                'col_variacao': 'variacao_mensal'
            },
            {
                'sheet': 'credito_habit',
                'indicador': 'CREDITO_HABITACIONAL',
                'unidade': 'R$ milhões',
                'col_valor': 'valor',
                'col_variacao': 'variacao_mensal'
            },
            {
                'sheet': 'rend_poupanca',
                'indicador': 'RENDIMENTO_POUPANCA',
                'unidade': '% a.m.',
                'col_valor': 'valor',
                'col_variacao': 'variacao_mensal'
            }
        ]
        
        for fonte in fontes_macro:
            df = self._read_sheet(fonte['sheet'])
            if df.empty:
                continue
            
            df = self._clean_dataframe(df)
            records = []
            
            for _, row in df.iterrows():
                valor = self._parse_brazilian_number(row.get(fonte['col_valor']))
                variacao = self._parse_brazilian_number(row.get(fonte.get('col_variacao', ''), None))
                
                if valor is not None:
                    records.append({
                        'data_referencia': self._parse_date(row.get('data_referencia')),
                        'indicador': fonte['indicador'],
                        'valor': valor,
                        'unidade': fonte['unidade'],
                        'variacao_mes': variacao,
                        'fonte': 'BCB'
                    })
            
            if records:
                all_macro_data.extend(records)
                print(f"    ✓ {fonte['indicador']}: {len(records)} registros")
        
        # Criar DataFrame consolidado
        df_macro = pd.DataFrame(all_macro_data)
        
        if df_macro.empty:
            print("    ⚠️ Nenhum dado macro encontrado")
            return pd.DataFrame()
        
        # Remover duplicatas
        df_macro = df_macro.drop_duplicates(
            subset=['data_referencia', 'indicador'],
            keep='last'
        )
        
        # Ordenar
        df_macro = df_macro.sort_values(['indicador', 'data_referencia'])
        
        # Adicionar ID
        df_macro.insert(0, 'id', range(1, len(df_macro) + 1))
        
        print(f"    ✅ Total consolidado: {len(df_macro)} registros")
        
        self.stats['registros_processados'] += len(all_macro_data)
        self.stats['registros_output'] += len(df_macro)
        
        return df_macro
    
    # =========================================================================
    # DIMENSÕES - MÉTODOS CONSTRUTIVOS
    # =========================================================================
    
    def consolidate_dim_metodos(self) -> pd.DataFrame:
        """
        Consolida dados de métodos construtivos com fatores regionais.
        
        DADOS PROPRIETÁRIOS - Preservar inteligência de negócio.
        """
        print("\n📊 Preparando dim_metodos_construtivos...")
        
        # Prioridade: arquivo local mais recente
        local_files = [
            CONFIG_DIR / 'dim_metodo_regional_completo_LATEST.csv',
            CONFIG_DIR / 'dim_metodo_regional_FASE2_20251114_183325.csv',
            CONFIG_DIR / 'dim_metodo_v2.csv'
        ]
        
        df = None
        for filepath in local_files:
            if filepath.exists():
                try:
                    df = pd.read_csv(filepath)
                    print(f"    ✓ Lido de: {filepath.name}")
                    break
                except Exception as e:
                    print(f"    ⚠️ Erro ao ler {filepath.name}: {e}")
        
        # Fallback: Google Sheets
        if df is None or df.empty:
            df = self._read_sheet('dim_metodo_fase2')
            if not df.empty:
                print("    ✓ Lido de Google Sheets: dim_metodo_fase2")
        
        if df is None or df.empty:
            print("    ⚠️ Nenhum dado de métodos encontrado")
            return pd.DataFrame()
        
        df = self._clean_dataframe(df)
        
        # Selecionar e renomear colunas relevantes
        colunas_output = {
            'id_metodo_uf': 'id',
            'id_metodo': 'codigo_metodo',
            'nome_metodo': 'nome',
            'uf': 'uf',
            'nome_uf': 'nome_uf',
            'regiao': 'regiao',
            'fator_regional_custo': 'fator_custo',
            'fator_regional_prazo': 'fator_prazo',
            'percentual_material': 'pct_material',
            'percentual_mao_obra': 'pct_mao_obra',
            'status_validacao': 'status',
            'data_atualizacao_cub': 'data_atualizacao',
            'origem_fator': 'origem_fator'
        }
        
        # Mapear colunas existentes
        cols_existentes = {k: v for k, v in colunas_output.items() if k in df.columns}
        df_output = df[list(cols_existentes.keys())].rename(columns=cols_existentes)
        
        # Converter tipos
        for col in ['fator_custo', 'fator_prazo', 'pct_material', 'pct_mao_obra']:
            if col in df_output.columns:
                df_output[col] = df_output[col].apply(self._parse_brazilian_number)
        
        print(f"    ✅ Total: {len(df_output)} métodos/UF")
        
        self.stats['registros_output'] += len(df_output)
        
        return df_output
    
    # =========================================================================
    # DIMENSÕES - TAXAS MUNICIPAIS
    # =========================================================================
    
    def consolidate_dim_taxas_municipais(self) -> pd.DataFrame:
        """
        Consolida taxas municipais (ISS, ITBI, Alvarás).
        
        DADOS PROPRIETÁRIOS - Resultado de pesquisa em legislações.
        """
        print("\n📊 Preparando dim_taxas_municipais...")
        
        # Arquivo local
        filepath = CONFIG_DIR / 'taxas_municipais_sc.csv'
        
        if filepath.exists():
            try:
                df = pd.read_csv(filepath)
                print(f"    ✓ Lido de: {filepath.name}")
            except Exception as e:
                print(f"    ⚠️ Erro: {e}")
                return pd.DataFrame()
        else:
            # Fallback: Google Sheets
            df = self._read_sheet('fact_taxas_municipais')
            if df.empty:
                print("    ⚠️ Nenhum dado de taxas municipais")
                return pd.DataFrame()
        
        df = self._clean_dataframe(df)
        
        # Adicionar ID se não existir
        if 'id' not in df.columns:
            df.insert(0, 'id', range(1, len(df) + 1))
        
        # Converter colunas numéricas
        cols_numericas = ['itbi_aliquota', 'iss_construcao_aliquota', 
                         'iss_deducao_materiais', 'taxa_alvara_valor_m2']
        
        for col in cols_numericas:
            if col in df.columns:
                df[col] = df[col].apply(self._parse_brazilian_number)
        
        print(f"    ✅ Total: {len(df)} municípios")
        
        self.stats['registros_output'] += len(df)
        
        return df
    
    # =========================================================================
    # DIMENSÕES - LOCALIDADE (UF + CIDADE)
    # =========================================================================
    
    def consolidate_dim_localidade(self) -> pd.DataFrame:
        """
        Consolida dados de localidade (UF + Cidade em tabela única).
        """
        print("\n📊 Preparando dim_localidade...")
        
        # Ler UFs
        df_uf = self._read_sheet('dim_uf')
        if df_uf.empty:
            print("    ⚠️ dim_uf não encontrada")
            return pd.DataFrame()
        
        df_uf = self._clean_dataframe(df_uf)
        
        # Ler cidades
        df_cidade = self._read_sheet('dim_cidade')
        if not df_cidade.empty:
            df_cidade = self._clean_dataframe(df_cidade)
        
        # Criar tabela unificada
        records = []
        
        # Adicionar UFs (nível estadual)
        for _, row in df_uf.iterrows():
            records.append({
                'id': f"UF_{row.get('sigla_uf', row.get('id_uf', ''))}",
                'tipo': 'UF',
                'codigo_ibge': row.get('codigo_ibge_uf'),
                'sigla': row.get('sigla_uf'),
                'nome': row.get('nome_uf'),
                'regiao': row.get('regiao'),
                'uf_pai': None,
                'populacao': None,
                'latitude': None,
                'longitude': None
            })
        
        # Adicionar cidades
        if not df_cidade.empty:
            for _, row in df_cidade.iterrows():
                records.append({
                    'id': f"CID_{row.get('codigo_ibge', row.get('id_cidade', ''))}",
                    'tipo': 'CIDADE',
                    'codigo_ibge': row.get('codigo_ibge'),
                    'sigla': None,
                    'nome': row.get('nome_cidade'),
                    'regiao': None,
                    'uf_pai': row.get('id_uf'),
                    'populacao': self._parse_brazilian_number(row.get('populacao_2022')),
                    'latitude': self._parse_brazilian_number(row.get('latitude')),
                    'longitude': self._parse_brazilian_number(row.get('longitude'))
                })
        
        df_localidade = pd.DataFrame(records)
        
        print(f"    ✅ Total: {len(df_localidade)} localidades ({len(df_uf)} UFs + cidades)")
        
        self.stats['registros_output'] += len(df_localidade)
        
        return df_localidade
    
    # =========================================================================
    # TABELAS DE CONFIGURAÇÃO (Calculadora)
    # =========================================================================
    
    def _process_config_tables(self) -> List[tuple]:
        """
        Processa tabelas de configuração da calculadora.
        
        Lê CSVs da pasta configs/ e copia para production_ready/.
        """
        arquivos_processados = []
        
        config_files = [
            ('dim_cenarios_construcao.csv', 'dim_cenarios_construcao'),
            ('dim_tipos_contencao.csv', 'dim_tipos_contencao'),
            ('dim_profundidade_subsolo.csv', 'dim_profundidade_subsolo'),
            ('dim_metodos_construtivos_base.csv', 'dim_metodos_construtivos_base'),
            ('dim_topografia.csv', 'dim_topografia'),
            ('dim_fatores_pavimento.csv', 'dim_fatores_pavimento'),
            ('dim_padrao_acabamento.csv', 'dim_padrao_acabamento'),
            ('fatores_regionais_uf.csv', 'dim_fatores_regionais_uf'),
            ('taxas_cartoriais_estaduais.csv', 'dim_taxas_cartoriais'),
        ]
        
        for source_file, table_name in config_files:
            source_path = CONFIG_DIR / source_file
            if source_path.exists():
                try:
                    df = pd.read_csv(source_path)
                    output_path = OUTPUT_DIR / f"{table_name}.csv"
                    df.to_csv(output_path, index=False)
                    arquivos_processados.append((f"{table_name}.csv", len(df)))
                    print(f"    ✓ {table_name}: {len(df)} registros")
                except Exception as e:
                    print(f"    ⚠️ Erro em {source_file}: {e}")
            else:
                print(f"    ⚠️ Não encontrado: {source_file}")
        
        # Processar series_mapping.json para dim_series_bcb
        df_series = self._process_series_mapping()
        if not df_series.empty:
            output_path = OUTPUT_DIR / 'dim_series_bcb.csv'
            df_series.to_csv(output_path, index=False)
            arquivos_processados.append(('dim_series_bcb.csv', len(df_series)))
            print(f"    ✓ dim_series_bcb: {len(df_series)} registros")
        
        return arquivos_processados
    
    def _process_series_mapping(self) -> pd.DataFrame:
        """Processa series_mapping.json para tabela de séries BCB."""
        import json
        
        filepath = CONFIG_DIR / 'series_mapping.json'
        if not filepath.exists():
            return pd.DataFrame()
        
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except Exception:
            return pd.DataFrame()
        
        series_list = []
        
        # Processar séries SGS
        if 'series' in data:
            for serie_id, info in data['series'].items():
                series_list.append({
                    'serie_id': serie_id,
                    'bcb_code': info.get('bcb_code'),
                    'nome_completo': info.get('nome_completo'),
                    'categoria': info.get('categoria'),
                    'unidade': info.get('unidade'),
                    'frequencia': info.get('frequencia'),
                    'relevancia_bi': info.get('relevancia_bi'),
                    'uso': info.get('uso'),
                    'tipo_fonte': 'SGS',
                    'endpoint': None
                })
        
        # Processar expectativas Focus
        if 'expectativas_focus' in data:
            for exp_id, info in data['expectativas_focus'].get('endpoints', {}).items():
                series_list.append({
                    'serie_id': f"focus_{exp_id}",
                    'bcb_code': None,
                    'nome_completo': info.get('nome'),
                    'categoria': 'expectativas',
                    'unidade': None,
                    'frequencia': info.get('frequencia'),
                    'relevancia_bi': info.get('relevancia_bi'),
                    'uso': info.get('uso'),
                    'tipo_fonte': 'FOCUS_ODATA',
                    'endpoint': info.get('endpoint')
                })
        
        df = pd.DataFrame(series_list)
        if not df.empty:
            df.insert(0, 'id', range(1, len(df) + 1))
        
        return df
    
    # =========================================================================
    # EXECUÇÃO PRINCIPAL
    # =========================================================================
    
    def run(self):
        """Executa consolidação completa."""
        print("=" * 70)
        print("🚀 PREPARAÇÃO DE DADOS PARA SUPABASE")
        print("=" * 70)
        print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"📁 Output: {OUTPUT_DIR}")
        print()
        
        arquivos_gerados = []
        
        # 1. fact_cub
        df_cub = self.consolidate_cub()
        if not df_cub.empty:
            output_path = OUTPUT_DIR / 'fact_cub.csv'
            df_cub.to_csv(output_path, index=False)
            arquivos_gerados.append(('fact_cub.csv', len(df_cub)))
            print(f"    💾 Salvo: {output_path}")
        
        # 2. fact_macroeconomia
        df_macro = self.consolidate_macroeconomia()
        if not df_macro.empty:
            output_path = OUTPUT_DIR / 'fact_macroeconomia.csv'
            df_macro.to_csv(output_path, index=False)
            arquivos_gerados.append(('fact_macroeconomia.csv', len(df_macro)))
            print(f"    💾 Salvo: {output_path}")
        
        # 3. dim_metodos_construtivos
        df_metodos = self.consolidate_dim_metodos()
        if not df_metodos.empty:
            output_path = OUTPUT_DIR / 'dim_metodos_construtivos.csv'
            df_metodos.to_csv(output_path, index=False)
            arquivos_gerados.append(('dim_metodos_construtivos.csv', len(df_metodos)))
            print(f"    💾 Salvo: {output_path}")
        
        # 4. dim_taxas_municipais
        df_taxas = self.consolidate_dim_taxas_municipais()
        if not df_taxas.empty:
            output_path = OUTPUT_DIR / 'dim_taxas_municipais.csv'
            df_taxas.to_csv(output_path, index=False)
            arquivos_gerados.append(('dim_taxas_municipais.csv', len(df_taxas)))
            print(f"    💾 Salvo: {output_path}")
        
        # 5. dim_localidade
        df_localidade = self.consolidate_dim_localidade()
        if not df_localidade.empty:
            output_path = OUTPUT_DIR / 'dim_localidade.csv'
            df_localidade.to_csv(output_path, index=False)
            arquivos_gerados.append(('dim_localidade.csv', len(df_localidade)))
            print(f"    💾 Salvo: {output_path}")
        
        # 6-11. Tabelas de Configuração da Calculadora (via update_config_tables)
        print("\n📊 Processando tabelas de configuração...")
        arquivos_gerados.extend(self._process_config_tables())
        
        # Resumo
        self._print_summary(arquivos_gerados)
        
        return arquivos_gerados
    
    def _print_summary(self, arquivos: List[tuple]):
        """Imprime resumo da execução."""
        print("\n" + "=" * 70)
        print("📊 RESUMO DA CONSOLIDAÇÃO")
        print("=" * 70)
        
        print("\n📁 ARQUIVOS GERADOS:")
        total_registros = 0
        for nome, qtd in arquivos:
            print(f"   ✅ {nome}: {qtd:,} registros")
            total_registros += qtd
        
        print(f"\n📈 ESTATÍSTICAS:")
        print(f"   • Arquivos gerados: {len(arquivos)}")
        print(f"   • Total de registros: {total_registros:,}")
        print(f"   • Diretório: {OUTPUT_DIR}")
        
        print("\n" + "=" * 70)
        print("✅ DADOS PRONTOS PARA UPLOAD NO SUPABASE!")
        print("=" * 70)
        
        # Gerar SQL de criação de tabelas
        print("\n📝 SQL SUGERIDO PARA CRIAR TABELAS NO SUPABASE:")
        print("-" * 70)
        
        sql_scripts = """
-- ============================================
-- FACT_CUB - Custo Unitário Básico
-- ============================================
CREATE TABLE IF NOT EXISTS fact_cub (
    id SERIAL PRIMARY KEY,
    data_referencia DATE NOT NULL,
    uf VARCHAR(2) NOT NULL,
    tipo_cub VARCHAR(20) NOT NULL,
    regime_tributario VARCHAR(20) NOT NULL,
    valor_m2 DECIMAL(10,2) NOT NULL,
    fonte VARCHAR(50),
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(data_referencia, uf, tipo_cub, regime_tributario)
);

CREATE INDEX idx_fact_cub_data ON fact_cub(data_referencia);
CREATE INDEX idx_fact_cub_uf ON fact_cub(uf);

-- ============================================
-- FACT_MACROECONOMIA - Indicadores Econômicos
-- ============================================
CREATE TABLE IF NOT EXISTS fact_macroeconomia (
    id SERIAL PRIMARY KEY,
    data_referencia DATE NOT NULL,
    indicador VARCHAR(50) NOT NULL,
    valor DECIMAL(15,6) NOT NULL,
    unidade VARCHAR(20),
    variacao_mes DECIMAL(10,6),
    fonte VARCHAR(50),
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(data_referencia, indicador)
);

CREATE INDEX idx_fact_macro_data ON fact_macroeconomia(data_referencia);
CREATE INDEX idx_fact_macro_indicador ON fact_macroeconomia(indicador);

-- ============================================
-- DIM_METODOS_CONSTRUTIVOS - Fatores Regionais
-- ============================================
CREATE TABLE IF NOT EXISTS dim_metodos_construtivos (
    id VARCHAR(50) PRIMARY KEY,
    codigo_metodo VARCHAR(20),
    nome VARCHAR(100),
    uf VARCHAR(2),
    nome_uf VARCHAR(50),
    regiao VARCHAR(20),
    fator_custo DECIMAL(6,4),
    fator_prazo DECIMAL(6,4),
    pct_material DECIMAL(5,4),
    pct_mao_obra DECIMAL(5,4),
    status VARCHAR(30),
    data_atualizacao DATE,
    origem_fator VARCHAR(30)
);

CREATE INDEX idx_dim_metodos_uf ON dim_metodos_construtivos(uf);

-- ============================================
-- DIM_TAXAS_MUNICIPAIS - ISS, ITBI, Alvarás
-- ============================================
CREATE TABLE IF NOT EXISTS dim_taxas_municipais (
    id SERIAL PRIMARY KEY,
    cidade VARCHAR(100),
    uf VARCHAR(2),
    itbi_aliquota DECIMAL(5,2),
    iss_construcao_aliquota DECIMAL(5,2),
    iss_deducao_materiais DECIMAL(5,2),
    taxa_alvara_valor_m2 DECIMAL(10,2),
    codigo_tributario_lei VARCHAR(100),
    codigo_obras_lei VARCHAR(100),
    fonte_url TEXT,
    data_atualizacao DATE,
    observacoes TEXT
);

CREATE INDEX idx_dim_taxas_cidade ON dim_taxas_municipais(cidade, uf);

-- ============================================
-- DIM_LOCALIDADE - UFs e Cidades
-- ============================================
CREATE TABLE IF NOT EXISTS dim_localidade (
    id VARCHAR(20) PRIMARY KEY,
    tipo VARCHAR(10) NOT NULL,
    codigo_ibge VARCHAR(10),
    sigla VARCHAR(2),
    nome VARCHAR(100) NOT NULL,
    regiao VARCHAR(20),
    uf_pai VARCHAR(20),
    populacao INTEGER,
    latitude DECIMAL(10,6),
    longitude DECIMAL(10,6)
);

CREATE INDEX idx_dim_localidade_tipo ON dim_localidade(tipo);
CREATE INDEX idx_dim_localidade_uf ON dim_localidade(uf_pai);
"""
        print(sql_scripts)


def main():
    """Função principal."""
    consolidator = DataConsolidator(use_sheets=True)
    consolidator.run()


if __name__ == "__main__":
    main()
