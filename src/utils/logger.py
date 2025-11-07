"""
Módulo de configuração de logging estruturado.

Fornece utilitários para configurar structlog com formatação JSON,
decorators para logging automático de execução, e configuração padronizada.

Exemplo de uso:
    >>> from src.utils.logger import setup_logger, log_execution
    >>> 
    >>> # Configurar logger
    >>> logger = setup_logger(__name__)
    >>> 
    >>> # Usar decorator para logging automático
    >>> @log_execution(logger)
    >>> def processar_dados(items):
    ...     logger.info("processando_items", count=len(items))
    ...     return len(items)
    >>> 
    >>> # Chamar função - logs automáticos de início/fim/tempo
    >>> resultado = processar_dados([1, 2, 3])
"""

import functools
import time
from typing import Any, Callable, Optional

import structlog


def setup_logger(
    name: str,
    level: str = "INFO"
) -> structlog.BoundLogger:
    """
    Configura e retorna um logger structlog com processadores padronizados.
    
    Configura structlog com:
    - Timestamps em formato ISO 8601
    - Nível de log adicionado automaticamente
    - Renderização JSON para produção
    - Context wrapping para adicionar metadados
    
    Args:
        name: Nome do logger (geralmente __name__ do módulo)
        level: Nível de log (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    
    Returns:
        Logger structlog configurado e pronto para uso
    
    Example:
        >>> logger = setup_logger(__name__)
        >>> logger.info("evento_teste", user_id=123, action="login")
    """
    # Configurar processadores do structlog
    structlog.configure(
        processors=[
            # Adicionar log level
            structlog.stdlib.add_log_level,
            # Adicionar timestamp ISO 8601
            structlog.processors.TimeStamper(fmt="iso"),
            # Adicionar informações da stack trace em caso de exceções
            structlog.processors.StackInfoRenderer(),
            # Formatar exceções
            structlog.processors.format_exc_info,
            # Adicionar nome do logger
            structlog.stdlib.add_logger_name,
            # Renderizar como JSON (ideal para produção)
            structlog.processors.JSONRenderer()
        ],
        # Wrapper factory
        wrapper_class=structlog.stdlib.BoundLogger,
        # Context class
        context_class=dict,
        # Logger factory
        logger_factory=structlog.stdlib.LoggerFactory(),
        # Cache logger instances
        cache_logger_on_first_use=True,
    )
    
    # Criar e retornar logger
    logger = structlog.get_logger(name)
    
    return logger


def log_execution(logger: structlog.BoundLogger) -> Callable:
    """
    Decorator para logging automático de execução de funções.
    
    Loga automaticamente:
    - Início da execução da função
    - Fim da execução com tempo decorrido
    - Exceções capturadas com stack trace
    - Contexto da função (nome, argumentos)
    
    Args:
        logger: Logger structlog configurado
    
    Returns:
        Decorator function
    
    Example:
        >>> logger = setup_logger(__name__)
        >>> 
        >>> @log_execution(logger)
        >>> def processar_dados(data_id: int, opcoes: dict):
        ...     # Lógica da função
        ...     return {"status": "ok"}
        >>> 
        >>> # Logs automáticos serão gerados ao chamar
        >>> resultado = processar_dados(123, {"verbose": True})
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            func_name = func.__name__
            module_name = func.__module__
            
            # Preparar contexto de log
            log_context = {
                "function": func_name,
                "module": module_name,
            }
            
            # Adicionar args se houver (limitado para não poluir logs)
            if args:
                # Converter args para formato loggable (limitar tamanho)
                args_repr = []
                for i, arg in enumerate(args[:5]):  # Limitar a 5 primeiros args
                    try:
                        arg_str = repr(arg)
                        # Limitar tamanho de cada argumento
                        if len(arg_str) > 100:
                            arg_str = arg_str[:97] + "..."
                        args_repr.append(arg_str)
                    except Exception:
                        args_repr.append("<not-representable>")
                
                if len(args) > 5:
                    args_repr.append(f"... +{len(args) - 5} more")
                
                log_context["args"] = args_repr
            
            # Adicionar kwargs se houver (limitado)
            if kwargs:
                kwargs_repr = {}
                for key, value in list(kwargs.items())[:10]:  # Limitar a 10 kwargs
                    try:
                        value_str = repr(value)
                        if len(value_str) > 100:
                            value_str = value_str[:97] + "..."
                        kwargs_repr[key] = value_str
                    except Exception:
                        kwargs_repr[key] = "<not-representable>"
                
                if len(kwargs) > 10:
                    kwargs_repr["__more__"] = f"{len(kwargs) - 10} more kwargs"
                
                log_context["kwargs"] = kwargs_repr
            
            # Log de início
            logger.info(
                "function_execution_started",
                **log_context
            )
            
            # Medir tempo de execução
            start_time = time.time()
            
            try:
                # Executar função
                result = func(*args, **kwargs)
                
                # Calcular tempo de execução
                execution_time = time.time() - start_time
                
                # Log de sucesso
                logger.info(
                    "function_execution_completed",
                    execution_time_seconds=round(execution_time, 3),
                    **log_context
                )
                
                return result
            
            except Exception as e:
                # Calcular tempo até a exceção
                execution_time = time.time() - start_time
                
                # Log de erro com contexto completo
                logger.error(
                    "function_execution_failed",
                    execution_time_seconds=round(execution_time, 3),
                    error_type=type(e).__name__,
                    error_message=str(e),
                    exc_info=True,  # Inclui stack trace
                    **log_context
                )
                
                # Re-raise a exceção para não alterar comportamento
                raise
        
        return wrapper
    
    return decorator


def get_logger(name: str, level: str = "INFO") -> structlog.BoundLogger:
    """
    Alias para setup_logger para maior clareza.
    
    Args:
        name: Nome do logger
        level: Nível de log
    
    Returns:
        Logger configurado
    
    Example:
        >>> from src.utils.logger import get_logger
        >>> logger = get_logger(__name__)
    """
    return setup_logger(name, level)


# Criar logger padrão do módulo
logger = setup_logger(__name__)
