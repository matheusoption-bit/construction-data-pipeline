"""
Testes para módulo de clientes de APIs externas.

Testa BCBClient (cliente do Banco Central do Brasil).
"""

import pytest
from unittest.mock import Mock, patch
from datetime import datetime

from src.clients.bcb import BCBClient


@pytest.fixture
def bcb_client():
    """
    Fixture que retorna uma instância de BCBClient configurada para testes.
    
    Returns:
        BCBClient configurado com parâmetros de teste
    """
    return BCBClient(
        timeout=30,
        max_retries=2,
        retry_delay=0.1  # Delay curto para testes rápidos
    )


@pytest.fixture
def mock_bcb_response():
    """
    Fixture com resposta mock da API BCB.
    
    Returns:
        Lista de dicionários no formato da API BCB
    """
    return [
        {"data": "01/01/2023", "valor": "13,75"},
        {"data": "01/02/2023", "valor": "13,75"},
        {"data": "01/03/2023", "valor": "13,75"},
        {"data": "01/04/2023", "valor": "13,25"},
        {"data": "01/05/2023", "valor": "13,25"},
    ]


@pytest.fixture
def mock_bcb_response_with_comma():
    """
    Fixture com resposta incluindo valores com vírgula decimal brasileira.
    
    Returns:
        Lista de dicionários com valores em formato brasileiro
    """
    return [
        {"data": "01/01/2023", "valor": "100,50"},
        {"data": "01/02/2023", "valor": "101,25"},
        {"data": "01/03/2023", "valor": "99,75"},
    ]


class TestBCBClient:
    """Testes para a classe BCBClient."""
    
    def test_bcb_fetch_series_success(self, bcb_client, mock_bcb_response):
        """
        Testa busca bem-sucedida de série temporal.
        
        Verifica:
        - Chamada correta à API
        - Processamento de dados
        - Conversão de formato de data
        - Conversão de valores decimais
        """
        with patch('src.clients.bcb.requests.get') as mock_get:
            # Configurar mock
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = mock_bcb_response
            mock_get.return_value = mock_response
            
            # Executar
            result = bcb_client.fetch_series(432)
            
            # Verificar chamada à API
            mock_get.assert_called_once()
            call_args = mock_get.call_args
            assert "432" in call_args[0][0]  # URL contém código da série
            assert call_args[1]["timeout"] == 30
            
            # Verificar resultado
            assert len(result) == 5
            assert result[0]["date"] == "2023-01-01"
            assert result[0]["value"] == 13.75
            assert result[3]["date"] == "2023-04-01"
            assert result[3]["value"] == 13.25
    
    def test_bcb_fetch_series_with_dates(self, bcb_client, mock_bcb_response):
        """
        Testa busca de série com datas de início e fim especificadas.
        
        Verifica:
        - Parâmetros dataInicial e dataFinal são passados corretamente
        - Formato de data brasileiro (DD/MM/YYYY) é usado
        """
        with patch('src.clients.bcb.requests.get') as mock_get:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = mock_bcb_response
            mock_get.return_value = mock_response
            
            # Executar com datas
            result = bcb_client.fetch_series(
                432,
                start_date="01/01/2023",
                end_date="31/05/2023"
            )
            
            # Verificar parâmetros
            call_args = mock_get.call_args
            assert call_args[1]["params"]["dataInicial"] == "01/01/2023"
            assert call_args[1]["params"]["dataFinal"] == "31/05/2023"
            
            # Verificar resultado
            assert len(result) == 5
    
    def test_bcb_fetch_series_handles_comma_decimal(
        self,
        bcb_client,
        mock_bcb_response_with_comma
    ):
        """
        Testa conversão correta de vírgula decimal brasileira para ponto.
        
        Verifica:
        - Valores com vírgula são convertidos para float corretamente
        - Precisão decimal é mantida
        """
        with patch('src.clients.bcb.requests.get') as mock_get:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = mock_bcb_response_with_comma
            mock_get.return_value = mock_response
            
            # Executar
            result = bcb_client.fetch_series(433)
            
            # Verificar conversão de vírgula para ponto
            assert result[0]["value"] == 100.50
            assert result[1]["value"] == 101.25
            assert result[2]["value"] == 99.75
            
            # Verificar tipo
            assert isinstance(result[0]["value"], float)
    
    def test_bcb_fetch_series_timeout(self, bcb_client):
        """
        Testa comportamento quando ocorre timeout na requisição.
        
        Verifica:
        - Exceção Timeout é levantada após tentativas de retry
        - Retry é executado o número correto de vezes
        """
        with patch('src.clients.bcb.requests.get') as mock_get:
            # Configurar mock para lançar Timeout
            import requests
            mock_get.side_effect = requests.exceptions.Timeout("Connection timeout")
            
            # Executar e verificar exceção
            with pytest.raises(requests.exceptions.Timeout):
                bcb_client.fetch_series(432)
            
            # Verificar que retry foi executado (max_retries=2)
            assert mock_get.call_count == 2
    
    def test_bcb_fetch_series_http_error_4xx(self, bcb_client):
        """
        Testa tratamento de erro HTTP 4xx (erro do cliente).
        
        Verifica:
        - Erro 4xx não faz retry
        - HTTPError é levantado imediatamente
        """
        with patch('src.clients.bcb.requests.get') as mock_get:
            import requests
            
            # Configurar mock para erro 404
            mock_response = Mock()
            mock_response.status_code = 404
            mock_response.text = "Not Found"
            mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
                "404 Client Error"
            )
            mock_get.return_value = mock_response
            
            # Executar e verificar exceção
            with pytest.raises(requests.exceptions.HTTPError):
                bcb_client.fetch_series(999999)
            
            # Verificar que não houve retry para erro 4xx
            assert mock_get.call_count == 1
    
    def test_bcb_fetch_series_http_error_5xx(self, bcb_client):
        """
        Testa tratamento de erro HTTP 5xx (erro do servidor).
        
        Verifica:
        - Erro 5xx faz retry
        - HTTPError é levantado após tentativas esgotadas
        """
        with patch('src.clients.bcb.requests.get') as mock_get:
            import requests
            
            # Configurar mock para erro 503
            mock_response = Mock()
            mock_response.status_code = 503
            mock_response.text = "Service Unavailable"
            mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
                "503 Server Error"
            )
            mock_get.return_value = mock_response
            
            # Executar e verificar exceção
            with pytest.raises(requests.exceptions.HTTPError):
                bcb_client.fetch_series(432)
            
            # Verificar que houve retry (max_retries=2)
            assert mock_get.call_count == 2
    
    def test_bcb_fetch_series_empty_response(self, bcb_client):
        """
        Testa comportamento quando API retorna lista vazia.
        
        Verifica:
        - Lista vazia é retornada
        - Não levanta exceção
        """
        with patch('src.clients.bcb.requests.get') as mock_get:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = []
            mock_get.return_value = mock_response
            
            # Executar
            result = bcb_client.fetch_series(432)
            
            # Verificar
            assert result == []
    
    def test_bcb_fetch_multiple_series(self, bcb_client, mock_bcb_response):
        """
        Testa busca de múltiplas séries temporais.
        
        Verifica:
        - Todas as séries são buscadas
        - Resultados são retornados em dicionário correto
        - Pausa entre requisições é respeitada (implícito no mock)
        """
        with patch('src.clients.bcb.requests.get') as mock_get, \
             patch('src.clients.bcb.time.sleep') as mock_sleep:
            
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = mock_bcb_response
            mock_get.return_value = mock_response
            
            # Executar com múltiplas séries
            series_map = {
                "selic": 432,
                "ipca": 433,
                "igpm": 189
            }
            
            result = bcb_client.fetch_multiple_series(series_map)
            
            # Verificar que todas as séries foram buscadas
            assert len(result) == 3
            assert "selic" in result
            assert "ipca" in result
            assert "igpm" in result
            
            # Verificar que cada série tem dados
            assert len(result["selic"]) == 5
            assert len(result["ipca"]) == 5
            assert len(result["igpm"]) == 5
            
            # Verificar que API foi chamada 3 vezes
            assert mock_get.call_count == 3
            
            # Verificar que sleep foi chamado 2 vezes (n-1 pausas)
            assert mock_sleep.call_count == 2
            mock_sleep.assert_called_with(1)  # Pausa de 1 segundo
    
    def test_bcb_fetch_multiple_series_partial_failure(self, bcb_client):
        """
        Testa busca de múltiplas séries com falha parcial.
        
        Verifica:
        - Séries bem-sucedidas são retornadas
        - Falha em uma série não impede processamento das demais
        - Erros são logados mas não impedem execução
        """
        with patch('src.clients.bcb.requests.get') as mock_get, \
             patch('src.clients.bcb.time.sleep'):
            
            # Configurar mock para falhar na segunda série
            def side_effect(*args, **kwargs):
                url = args[0]
                if "433" in url:  # IPCA - falha
                    raise Exception("API Error")
                else:
                    mock_response = Mock()
                    mock_response.status_code = 200
                    mock_response.json.return_value = [
                        {"data": "01/01/2023", "valor": "10,00"}
                    ]
                    return mock_response
            
            mock_get.side_effect = side_effect
            
            # Executar
            series_map = {
                "selic": 432,
                "ipca": 433,
                "igpm": 189
            }
            
            result = bcb_client.fetch_multiple_series(series_map)
            
            # Verificar que séries bem-sucedidas foram retornadas
            assert "selic" in result
            assert "igpm" in result
            
            # Verificar que série com falha não está nos resultados
            assert "ipca" not in result
            
            # Verificar dados das séries bem-sucedidas
            assert len(result["selic"]) == 1
            assert len(result["igpm"]) == 1
    
    def test_bcb_client_initialization(self):
        """
        Testa inicialização do cliente BCB com parâmetros customizados.
        
        Verifica:
        - Parâmetros são armazenados corretamente
        - Valores padrão são aplicados quando não especificados
        """
        # Com parâmetros customizados
        client = BCBClient(
            base_url="https://custom.api.com",
            timeout=120,
            max_retries=5,
            retry_delay=2
        )
        
        assert client.base_url == "https://custom.api.com"
        assert client.timeout == 120
        assert client.max_retries == 5
        assert client.retry_delay == 2
        
        # Com valores padrão
        default_client = BCBClient()
        
        assert default_client.base_url == "https://api.bcb.gov.br/dados/serie/bcdata.sgs"
        assert default_client.timeout == 60
        assert default_client.max_retries == 3
        assert default_client.retry_delay == 1
    
    def test_bcb_process_series_data_invalid_date(self, bcb_client):
        """
        Testa processamento de dados com data inválida.
        
        Verifica:
        - Dados com data inválida são ignorados
        - Processamento continua para dados válidos
        - Não levanta exceção
        """
        invalid_data = [
            {"data": "01/01/2023", "valor": "10,00"},
            {"data": "invalid-date", "valor": "20,00"},  # Data inválida
            {"data": "01/03/2023", "valor": "30,00"}
        ]
        
        # Chamar método privado diretamente
        result = bcb_client._process_series_data(invalid_data)
        
        # Verificar que apenas dados válidos foram processados
        assert len(result) == 2
        assert result[0]["date"] == "2023-01-01"
        assert result[1]["date"] == "2023-03-01"
    
    def test_bcb_process_series_data_invalid_value(self, bcb_client):
        """
        Testa processamento de dados com valor inválido.
        
        Verifica:
        - Dados com valor inválido são ignorados
        - Processamento continua para dados válidos
        """
        invalid_data = [
            {"data": "01/01/2023", "valor": "10,00"},
            {"data": "01/02/2023", "valor": "not-a-number"},  # Valor inválido
            {"data": "01/03/2023", "valor": "30,00"}
        ]
        
        result = bcb_client._process_series_data(invalid_data)
        
        # Verificar que apenas dados válidos foram processados
        assert len(result) == 2
        assert result[0]["value"] == 10.0
        assert result[1]["value"] == 30.0
