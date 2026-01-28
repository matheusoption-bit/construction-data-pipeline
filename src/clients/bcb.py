"""
Cliente para API do Banco Central do Brasil (BCB).

Este módulo fornece uma interface para buscar séries temporais da API de dados
abertos do Banco Central do Brasil.

Exemplo de uso:
    >>> from src.clients.bcb import BCBClient
    >>> 
    >>> client = BCBClient()
    >>> 
    >>> # Buscar uma série (IPCA - código 433)
    >>> ipca_data = client.fetch_series(433, start_date="01/01/2023", end_date="31/12/2023")
    >>> 
    >>> # Buscar múltiplas séries
    >>> series_map = {
    ...     "ipca": 433,
    ...     "selic": 432,
    ...     "igpm": 189
    ... }
    >>> data = client.fetch_multiple_series(series_map)
"""

import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import requests
import structlog

logger = structlog.get_logger(__name__)


class BCBClient:
    """
    Cliente para interagir com a API do Banco Central do Brasil.
    
    A API fornece acesso a séries temporais econômicas e financeiras.
    Documentação: https://dadosabertos.bcb.gov.br/
    
    Attributes:
        base_url: URL base da API do BCB
        timeout: Timeout em segundos para requisições HTTP
        max_retries: Número máximo de tentativas em caso de falha
        retry_delay: Delay inicial em segundos para retry (com backoff exponencial)
    """
    
    # Séries diárias (dados disponíveis D+1)
    DAILY_SERIES = {
        1,      # USD/BRL (Câmbio)
        11,     # USD/BRL (Ptax venda)
        10813,  # EUR/BRL
        10814,  # GBP/BRL
    }
    
    # Séries mensais (dados disponíveis após fim do mês)
    MONTHLY_SERIES = {
        432,    # Selic
        226,    # TR
        433,    # IPCA
        189,    # IGP-M
        7478,   # Poupança
        4189,   # INPC
        4390,   # Crédito PF
        21864,  # PAIC - Produção Construção Civil (Receita real)
        28561,  # Crédito - Construção Civil (Saldo)
        24364,  # Estoque Crédito Habitacional
    }
    
    def __init__(
        self,
        base_url: str = "https://api.bcb.gov.br/dados/serie/bcdata.sgs",
        timeout: int = 60,
        max_retries: int = 3,
        retry_delay: int = 1
    ):
        """
        Inicializa o cliente BCB.
        
        Args:
            base_url: URL base da API do BCB
            timeout: Timeout em segundos para requisições
            max_retries: Número máximo de tentativas em caso de falha
            retry_delay: Delay inicial para retry em segundos
        """
        self.base_url = base_url
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        
        logger.info(
            "bcb_client_initialized",
            base_url=base_url,
            timeout=timeout,
            max_retries=max_retries
        )
    
    def _is_daily_series(self, series_id: int) -> bool:
        """
        Verifica se série é diária.
        
        Args:
            series_id: Código da série
        
        Returns:
            True se série é diária, False se mensal
        """
        return series_id in self.DAILY_SERIES
    
    def _validate_and_adjust_dates(
        self,
        series_id: int,
        start_date: Optional[str],
        end_date: Optional[str]
    ) -> tuple[str, str]:
        """
        Valida e ajusta datas para evitar buscar dados futuros/não disponíveis.
        
        Args:
            series_id: Código da série
            start_date: Data inicial (DD/MM/YYYY) ou None
            end_date: Data final (DD/MM/YYYY) ou None
        
        Returns:
            Tupla (start_date_adjusted, end_date_adjusted)
        """
        hoje = datetime.now()
        
        # Determinar data final máxima segura
        if self._is_daily_series(series_id):
            # Séries diárias: até ontem (dados vêm D+1)
            max_safe_date = hoje - timedelta(days=1)
        else:
            # Séries mensais: até último dia do mês anterior
            primeiro_dia_mes_atual = hoje.replace(day=1)
            max_safe_date = primeiro_dia_mes_atual - timedelta(days=1)
        
        # Ajustar end_date se não fornecido ou se futuro
        if end_date:
            try:
                end_dt = datetime.strptime(end_date, "%d/%m/%Y")
                if end_dt > max_safe_date:
                    logger.warning(
                        "future_date_adjusted",
                        series_id=series_id,
                        requested_date=end_date,
                        adjusted_to=max_safe_date.strftime("%d/%m/%Y"),
                        reason="Requested date is in the future or data not yet available"
                    )
                    end_date = max_safe_date.strftime("%d/%m/%Y")
            except ValueError:
                logger.warning(
                    "invalid_date_format",
                    series_id=series_id,
                    end_date=end_date
                )
                end_date = max_safe_date.strftime("%d/%m/%Y")
        else:
            end_date = max_safe_date.strftime("%d/%m/%Y")
            logger.info(
                "end_date_auto_set",
                series_id=series_id,
                end_date=end_date,
                series_type="daily" if self._is_daily_series(series_id) else "monthly"
            )
        
        # Garantir start_date
        if not start_date:
            # Padrão: últimos 12 meses
            start_dt = max_safe_date - timedelta(days=365)
            start_date = start_dt.strftime("%d/%m/%Y")
        
        return start_date, end_date
    
    def fetch_series(
        self,
        series_id: int,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Busca uma série temporal da API do BCB.
        
        IMPORTANTE: Valida e ajusta datas automaticamente para evitar
        buscar dados futuros ou não disponíveis.
        
        Args:
            series_id: Código da série no SGS (Sistema Gerenciador de Séries Temporais)
            start_date: Data inicial no formato DD/MM/YYYY (opcional)
            end_date: Data final no formato DD/MM/YYYY (opcional)
        
        Returns:
            Lista de dicionários com 'date' (YYYY-MM-DD) e 'value' (float)
        
        Raises:
            requests.exceptions.HTTPError: Erro HTTP (4xx, 5xx)
            requests.exceptions.Timeout: Timeout na requisição
            requests.exceptions.RequestException: Erro genérico de requisição
            ValueError: Erro ao processar dados da resposta
        
        Example:
            >>> client = BCBClient()
            >>> data = client.fetch_series(433, "01/01/2023", "31/12/2023")
            >>> print(data[0])
            {'date': '2023-01-01', 'value': 5.79}
        """
        # VALIDAÇÃO: Ajustar datas para evitar dados futuros/não disponíveis
        start_date, end_date = self._validate_and_adjust_dates(
            series_id, start_date, end_date
        )
        
        url = f"{self.base_url}.{series_id}/dados"
        params = {}
        
        if start_date:
            params["dataInicial"] = start_date
        if end_date:
            params["dataFinal"] = end_date
        
        logger.info(
            "fetching_bcb_series",
            series_id=series_id,
            start_date=start_date,
            end_date=end_date,
            url=url
        )
        
        # Retry com backoff exponencial
        last_exception = None
        for attempt in range(1, self.max_retries + 1):
            try:
                response = requests.get(
                    url,
                    params=params,
                    timeout=self.timeout,
                    headers={"Accept": "application/json"}
                )
                
                # Verificar status HTTP
                if response.status_code >= 400:
                    logger.warning(
                        "bcb_api_error",
                        series_id=series_id,
                        status_code=response.status_code,
                        response_text=response.text[:500],
                        attempt=attempt
                    )
                    response.raise_for_status()
                
                raw_data = response.json()
                
                # VALIDAÇÃO: Resposta vazia da API
                if not raw_data:
                    logger.warning(
                        "api_returned_empty",
                        series_id=series_id,
                        start_date=start_date,
                        end_date=end_date,
                        message="API retornou lista vazia - série pode não ter dados no período"
                    )
                    return []
                
                # Processar e transformar dados
                processed_data = self._process_series_data(raw_data)
                
                # VALIDAÇÃO: Detectar valores constantes suspeitos
                if processed_data and len(processed_data) > 10:
                    unique_values = set(item['value'] for item in processed_data)
                    if len(unique_values) == 1:
                        logger.warning(
                            "suspicious_constant_value",
                            series_id=series_id,
                            constant_value=processed_data[0]['value'],
                            records_count=len(processed_data),
                            message="Todos os registros têm o mesmo valor - pode indicar dados default/placeholder"
                        )
                
                logger.info(
                    "bcb_series_fetched",
                    series_id=series_id,
                    records_count=len(processed_data),
                    unique_values_count=len(set(item['value'] for item in processed_data)) if processed_data else 0,
                    attempt=attempt
                )
                
                return processed_data
            
            except requests.exceptions.HTTPError as e:
                last_exception = e
                # Não fazer retry para erros 4xx (client errors)
                if 400 <= e.response.status_code < 500:
                    logger.error(
                        "bcb_client_error",
                        series_id=series_id,
                        status_code=e.response.status_code,
                        error=str(e)
                    )
                    raise
                
                # Retry para erros 5xx (server errors)
                if attempt < self.max_retries:
                    delay = self.retry_delay * (2 ** (attempt - 1))
                    logger.warning(
                        "bcb_server_error_retrying",
                        series_id=series_id,
                        attempt=attempt,
                        max_retries=self.max_retries,
                        retry_delay=delay,
                        error=str(e)
                    )
                    time.sleep(delay)
                else:
                    logger.error(
                        "bcb_max_retries_exceeded",
                        series_id=series_id,
                        attempts=attempt,
                        error=str(e)
                    )
                    raise
            
            except (requests.exceptions.Timeout, requests.exceptions.RequestException) as e:
                last_exception = e
                if attempt < self.max_retries:
                    delay = self.retry_delay * (2 ** (attempt - 1))
                    logger.warning(
                        "bcb_request_error_retrying",
                        series_id=series_id,
                        attempt=attempt,
                        max_retries=self.max_retries,
                        retry_delay=delay,
                        error=str(e),
                        error_type=type(e).__name__
                    )
                    time.sleep(delay)
                else:
                    logger.error(
                        "bcb_max_retries_exceeded",
                        series_id=series_id,
                        attempts=attempt,
                        error=str(e),
                        error_type=type(e).__name__
                    )
                    raise
        
        # Se chegou aqui, todas as tentativas falharam
        if last_exception:
            raise last_exception
        
        return []
    
    def fetch_multiple_series(
        self,
        series_map: Dict[str, int],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Busca múltiplas séries temporais da API do BCB.
        
        Adiciona pausa de 1 segundo entre requisições para evitar sobrecarga da API.
        
        Args:
            series_map: Dicionário mapeando identificadores para códigos SGS
                       Exemplo: {"ipca": 433, "selic": 432}
            start_date: Data inicial no formato DD/MM/YYYY (opcional)
            end_date: Data final no formato DD/MM/YYYY (opcional)
        
        Returns:
            Dicionário com os identificadores mapeados para os dados das séries
        
        Raises:
            requests.exceptions.HTTPError: Erro HTTP em alguma das requisições
            requests.exceptions.Timeout: Timeout em alguma das requisições
            requests.exceptions.RequestException: Erro genérico em alguma requisição
        
        Example:
            >>> client = BCBClient()
            >>> series_map = {"ipca": 433, "selic": 432}
            >>> data = client.fetch_multiple_series(series_map, "01/01/2023")
            >>> print(list(data.keys()))
            ['ipca', 'selic']
        """
        logger.info(
            "fetching_multiple_bcb_series",
            series_count=len(series_map),
            series_ids=list(series_map.keys()),
            start_date=start_date,
            end_date=end_date
        )
        
        results = {}
        errors = {}
        
        for idx, (series_name, series_id) in enumerate(series_map.items(), 1):
            try:
                logger.debug(
                    "fetching_series",
                    series_name=series_name,
                    series_id=series_id,
                    progress=f"{idx}/{len(series_map)}"
                )
                
                data = self.fetch_series(series_id, start_date, end_date)
                results[series_name] = data
                
                # Pausa entre requisições (exceto na última)
                if idx < len(series_map):
                    time.sleep(1)
            
            except Exception as e:
                logger.error(
                    "error_fetching_series",
                    series_name=series_name,
                    series_id=series_id,
                    error=str(e),
                    error_type=type(e).__name__
                )
                errors[series_name] = str(e)
        
        if errors:
            logger.warning(
                "some_series_failed",
                successful_count=len(results),
                failed_count=len(errors),
                failed_series=list(errors.keys())
            )
        
        logger.info(
            "multiple_series_fetch_completed",
            successful_count=len(results),
            failed_count=len(errors),
            total_count=len(series_map)
        )
        
        return results
    
    def _process_series_data(self, raw_data: List[Dict[str, str]]) -> List[Dict[str, Any]]:
        """
        Processa dados brutos da API BCB.
        
        Converte formato de data DD/MM/YYYY para YYYY-MM-DD e valores com vírgula
        decimal para float.
        
        Args:
            raw_data: Lista de dicionários com 'data' e 'valor' da API
        
        Returns:
            Lista processada com 'date' e 'value'
        
        Raises:
            ValueError: Erro ao processar data ou valor
        """
        processed = []
        hoje = datetime.now().date()
        
        for item in raw_data:
            try:
                # Converter data de DD/MM/YYYY para YYYY-MM-DD
                date_str = item.get("data", "")
                date_obj = datetime.strptime(date_str, "%d/%m/%Y")
                formatted_date = date_obj.strftime("%Y-%m-%d")
                
                # VALIDAÇÃO: Ignorar datas futuras (dados não confiáveis)
                if date_obj.date() > hoje:
                    logger.warning(
                        "future_date_ignored",
                        date=formatted_date,
                        today=str(hoje)
                    )
                    continue
                
                # Converter valor: substituir vírgula por ponto e converter para float
                value_str = item.get("valor", "0")
                value = float(value_str.replace(",", "."))
                
                # VALIDAÇÃO: Ignorar valores inválidos
                if value == 0 or abs(value) > 1_000_000:
                    logger.warning(
                        "invalid_value_ignored",
                        date=formatted_date,
                        value=value,
                        reason="zero or extreme outlier"
                    )
                    continue
                
                processed.append({
                    "date": formatted_date,
                    "value": value
                })
            
            except (ValueError, KeyError) as e:
                logger.warning(
                    "error_processing_data_point",
                    item=item,
                    error=str(e),
                    error_type=type(e).__name__
                )
                # Continuar processando outros pontos
                continue
        
        return processed
