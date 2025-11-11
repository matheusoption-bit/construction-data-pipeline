"""
Testes unitários para funcionalidades de UPSERT do SheetsLoader.

Testa:
- Método read_fact_series()
- Método deduplicate_fact_series()
- Método write_fact_series() com lógica UPSERT
"""

import os
import unittest
from unittest.mock import MagicMock, Mock, patch
from datetime import datetime
import pandas as pd
import gspread

from src.etl.sheets import SheetsLoader


class TestReadFactSeries(unittest.TestCase):
    """Testes para o método read_fact_series()."""
    
    @patch.dict('os.environ', {
        'GOOGLE_CREDENTIALS_PATH': 'credentials.json',
        'GOOGLE_SPREADSHEET_ID': 'test_spreadsheet_id'
    })
    def setUp(self):
        """Configuração antes de cada teste."""
        self.loader = SheetsLoader()
        # Mock da conexão do Google Sheets
        self.loader._get_spreadsheet = MagicMock()
        self.loader._get_client = MagicMock()
    
    @patch.object(SheetsLoader, 'read_sheet')
    def test_read_fact_series_with_data(self, mock_read_sheet):
        """Deve ler dados existentes e retornar DataFrame."""
        # Simular dados retornados do Sheets
        mock_data = [
            ["id_fato", "series_id", "data_referencia", "valor", "variacao_mom", "variacao_yoy", "fonte_original", "created_at"],
            ["ipca_2023-01-01", "ipca", "2023-01-01", "100.5", "0.5", "5.2", "bcb_sgs", "2023-01-01 10:00:00"],
            ["ipca_2023-02-01", "ipca", "2023-02-01", "101.2", "0.7", "5.5", "bcb_sgs", "2023-02-01 10:00:00"],
        ]
        mock_read_sheet.return_value = mock_data
        
        # Executar
        result = self.loader.read_fact_series()
        
        # Verificar
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 2)
        self.assertIn('id_fato', result.columns)
        self.assertIn('valor', result.columns)
        self.assertEqual(result.iloc[0]['id_fato'], 'ipca_2023-01-01')
        # Verificar conversão numérica
        self.assertEqual(result.iloc[0]['valor'], 100.5)
    
    @patch.object(SheetsLoader, 'read_sheet')
    def test_read_fact_series_empty_sheet(self, mock_read_sheet):
        """Deve retornar DataFrame vazio quando aba estiver vazia."""
        # Simular aba com apenas header
        mock_data = [
            ["id_fato", "series_id", "data_referencia", "valor", "variacao_mom", "variacao_yoy", "fonte_original", "created_at"]
        ]
        mock_read_sheet.return_value = mock_data
        
        # Executar
        result = self.loader.read_fact_series()
        
        # Verificar
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 0)
    
    @patch.object(SheetsLoader, 'read_sheet')
    def test_read_fact_series_not_found(self, mock_read_sheet):
        """Deve retornar DataFrame vazio quando aba não existir."""
        # Simular aba não encontrada
        mock_read_sheet.side_effect = gspread.exceptions.WorksheetNotFound("fact_series")
        
        # Executar
        result = self.loader.read_fact_series()
        
        # Verificar
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 0)


class TestDeduplicateFactSeries(unittest.TestCase):
    """Testes para o método deduplicate_fact_series()."""
    
    @patch.dict('os.environ', {
        'GOOGLE_CREDENTIALS_PATH': 'credentials.json',
        'GOOGLE_SPREADSHEET_ID': 'test_spreadsheet_id'
    })
    def setUp(self):
        """Configuração antes de cada teste."""
        self.loader = SheetsLoader()
    
    def test_deduplicate_with_duplicates(self):
        """Deve remover duplicatas mantendo o mais recente."""
        df = pd.DataFrame({
            'id_fato': ['ipca_2023-01-01', 'ipca_2023-01-01', 'selic_2023-01-01'],
            'valor': [100.0, 101.0, 13.75],
            'created_at': ['2023-01-01 10:00:00', '2023-01-01 11:00:00', '2023-01-01 10:00:00']
        })
        
        # Executar
        df_clean, removed = self.loader.deduplicate_fact_series(df)
        
        # Verificar
        self.assertEqual(len(df_clean), 2)
        self.assertEqual(removed, 1)
        # Verificar que manteve o mais recente (valor 101.0)
        ipca_row = df_clean[df_clean['id_fato'] == 'ipca_2023-01-01'].iloc[0]
        self.assertEqual(ipca_row['valor'], 101.0)
    
    def test_deduplicate_no_duplicates(self):
        """Deve retornar DataFrame inalterado quando não há duplicatas."""
        df = pd.DataFrame({
            'id_fato': ['ipca_2023-01-01', 'ipca_2023-02-01', 'selic_2023-01-01'],
            'valor': [100.0, 101.0, 13.75],
            'created_at': ['2023-01-01 10:00:00', '2023-02-01 10:00:00', '2023-01-01 10:00:00']
        })
        
        # Executar
        df_clean, removed = self.loader.deduplicate_fact_series(df)
        
        # Verificar
        self.assertEqual(len(df_clean), 3)
        self.assertEqual(removed, 0)
    
    def test_deduplicate_empty_dataframe(self):
        """Deve retornar DataFrame vazio sem erro."""
        df = pd.DataFrame()
        
        # Executar
        df_clean, removed = self.loader.deduplicate_fact_series(df)
        
        # Verificar
        self.assertTrue(df_clean.empty)
        self.assertEqual(removed, 0)
    
    def test_deduplicate_no_id_fato_column(self):
        """Deve retornar DataFrame inalterado se não houver coluna id_fato."""
        df = pd.DataFrame({
            'valor': [100.0, 101.0],
            'created_at': ['2023-01-01 10:00:00', '2023-02-01 10:00:00']
        })
        
        # Executar
        df_clean, removed = self.loader.deduplicate_fact_series(df)
        
        # Verificar
        self.assertEqual(len(df_clean), 2)
        self.assertEqual(removed, 0)


class TestWriteFactSeriesUpsert(unittest.TestCase):
    """Testes para o método write_fact_series() com lógica UPSERT."""
    
    @patch.dict('os.environ', {
        'GOOGLE_CREDENTIALS_PATH': 'credentials.json',
        'GOOGLE_SPREADSHEET_ID': 'test_spreadsheet_id'
    })
    def setUp(self):
        """Configuração antes de cada teste."""
        self.loader = SheetsLoader()
        # Mock da conexão do Google Sheets
        self.mock_worksheet = MagicMock()
        self.mock_spreadsheet = MagicMock()
        self.mock_spreadsheet.worksheet.return_value = self.mock_worksheet
        self.loader._get_spreadsheet = MagicMock(return_value=self.mock_spreadsheet)
        self.loader._get_client = MagicMock()
    
    @patch.object(SheetsLoader, 'read_fact_series')
    @patch.object(SheetsLoader, 'create_sheet_if_not_exists')
    def test_write_fact_series_no_existing_data(self, mock_create_sheet, mock_read_fact):
        """Deve inserir todos os dados quando não há dados existentes."""
        # Simular aba vazia
        mock_read_fact.return_value = pd.DataFrame()
        
        # Dados para inserir
        df = pd.DataFrame({
            'data_referencia': ['2023-01-01', '2023-02-01'],
            'valor': [100.5, 102.3]
        })
        
        # Executar
        self.loader.write_fact_series('ipca', df, 'exec_001')
        
        # Verificar que worksheet.update foi chamado
        self.mock_worksheet.clear.assert_called_once()
        self.mock_worksheet.update.assert_called_once()
        
        # Verificar que os dados foram escritos corretamente
        call_args = self.mock_worksheet.update.call_args
        written_data = call_args[0][1]  # Segundo argumento de update()
        
        # Primeira linha deve ser header
        self.assertEqual(written_data[0], ['id_fato', 'series_id', 'data_referencia', 'valor', 'variacao_mom', 'variacao_yoy', 'fonte_original', 'created_at'])
        # Deve ter 2 linhas de dados + 1 header = 3 linhas
        self.assertEqual(len(written_data), 3)
    
    @patch.object(SheetsLoader, 'read_fact_series')
    @patch.object(SheetsLoader, 'create_sheet_if_not_exists')
    def test_write_fact_series_with_new_data(self, mock_create_sheet, mock_read_fact):
        """Deve adicionar apenas dados novos quando há dados existentes."""
        # Simular dados existentes
        existing_data = pd.DataFrame({
            'id_fato': ['ipca_2023-01-01'],
            'series_id': ['ipca'],
            'data_referencia': ['2023-01-01'],
            'valor': [100.5],
            'variacao_mom': [0.5],
            'variacao_yoy': [5.2],
            'fonte_original': ['bcb_sgs'],
            'created_at': ['2023-01-01 10:00:00']
        })
        mock_read_fact.return_value = existing_data
        
        # Novos dados (sem duplicatas)
        df = pd.DataFrame({
            'data_referencia': ['2023-02-01', '2023-03-01'],
            'valor': [102.3, 103.1]
        })
        
        # Executar
        self.loader.write_fact_series('ipca', df, 'exec_002')
        
        # Verificar que worksheet.update foi chamado
        self.mock_worksheet.update.assert_called_once()
        call_args = self.mock_worksheet.update.call_args
        written_data = call_args[0][1]
        
        # Deve ter 3 linhas de dados (1 existente + 2 novos) + 1 header = 4 linhas
        self.assertEqual(len(written_data), 4)
    
    @patch.object(SheetsLoader, 'read_fact_series')
    @patch.object(SheetsLoader, 'create_sheet_if_not_exists')
    def test_write_fact_series_with_duplicates(self, mock_create_sheet, mock_read_fact):
        """Deve atualizar dados duplicados (mesmos id_fato)."""
        # Simular dados existentes
        existing_data = pd.DataFrame({
            'id_fato': ['ipca_2023-01-01', 'ipca_2023-02-01'],
            'series_id': ['ipca', 'ipca'],
            'data_referencia': ['2023-01-01', '2023-02-01'],
            'valor': [100.5, 102.0],
            'variacao_mom': [0.5, 1.5],
            'variacao_yoy': [5.2, 5.0],
            'fonte_original': ['bcb_sgs', 'bcb_sgs'],
            'created_at': ['2023-01-01 10:00:00', '2023-02-01 10:00:00']
        })
        mock_read_fact.return_value = existing_data
        
        # Dados com duplicata (mesma data_referencia)
        df = pd.DataFrame({
            'data_referencia': ['2023-02-01', '2023-03-01'],
            'valor': [102.5, 103.1]  # Valor atualizado para 2023-02-01
        })
        
        # Executar
        self.loader.write_fact_series('ipca', df, 'exec_003')
        
        # Verificar que worksheet.update foi chamado
        self.mock_worksheet.update.assert_called_once()
        call_args = self.mock_worksheet.update.call_args
        written_data = call_args[0][1]
        
        # Deve ter 3 linhas (1 antigo + 1 atualizado + 1 novo) + 1 header = 4 linhas
        self.assertEqual(len(written_data), 4)
    
    def test_write_fact_series_missing_columns(self):
        """Deve levantar ValueError se colunas obrigatórias faltarem."""
        df = pd.DataFrame({
            'data_referencia': ['2023-01-01'],
            # Faltando coluna 'valor'
        })
        
        # Verificar que levanta erro
        with self.assertRaises(ValueError) as context:
            self.loader.write_fact_series('ipca', df, 'exec_004')
        
        self.assertIn('Faltando', str(context.exception))
        self.assertIn('valor', str(context.exception))


class TestUpsertIntegration(unittest.TestCase):
    """Testes de integração para fluxo completo de UPSERT."""
    
    @patch.dict('os.environ', {
        'GOOGLE_CREDENTIALS_PATH': 'credentials.json',
        'GOOGLE_SPREADSHEET_ID': 'test_spreadsheet_id'
    })
    def setUp(self):
        """Configuração antes de cada teste."""
        self.loader = SheetsLoader()
        # Mock completo do Google Sheets
        self.mock_worksheet = MagicMock()
        self.mock_spreadsheet = MagicMock()
        self.mock_spreadsheet.worksheet.return_value = self.mock_worksheet
        self.loader._get_spreadsheet = MagicMock(return_value=self.mock_spreadsheet)
        self.loader._get_client = MagicMock()
    
    @patch.object(SheetsLoader, 'read_fact_series')
    @patch.object(SheetsLoader, 'create_sheet_if_not_exists')
    def test_multiple_series_upsert(self, mock_create_sheet, mock_read_fact):
        """Deve gerenciar múltiplas séries corretamente."""
        # Simular dados existentes de múltiplas séries
        existing_data = pd.DataFrame({
            'id_fato': ['ipca_2023-01-01', 'selic_2023-01-01'],
            'series_id': ['ipca', 'selic'],
            'data_referencia': ['2023-01-01', '2023-01-01'],
            'valor': [100.5, 13.75],
            'variacao_mom': [0.5, 0.0],
            'variacao_yoy': [5.2, 0.0],
            'fonte_original': ['bcb_sgs', 'bcb_sgs'],
            'created_at': ['2023-01-01 10:00:00', '2023-01-01 10:00:00']
        })
        mock_read_fact.return_value = existing_data
        
        # Inserir dados de nova série (IPCA apenas)
        df = pd.DataFrame({
            'data_referencia': ['2023-02-01'],
            'valor': [102.3]
        })
        
        # Executar
        self.loader.write_fact_series('ipca', df, 'exec_005')
        
        # Verificar que worksheet.update foi chamado
        self.mock_worksheet.update.assert_called_once()
        call_args = self.mock_worksheet.update.call_args
        written_data = call_args[0][1]
        
        # Deve ter 3 linhas (1 IPCA antigo + 1 IPCA novo + 1 SELIC antigo) + 1 header
        self.assertEqual(len(written_data), 4)


if __name__ == '__main__':
    unittest.main()
