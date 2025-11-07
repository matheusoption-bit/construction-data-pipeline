"""
Módulo de configuração da aplicação.

Carrega e valida variáveis de ambiente necessárias para o funcionamento
do pipeline de dados. Usa python-dotenv para carregar arquivo .env.

Exemplo de uso:
    >>> from src.utils.config import Config
    >>> 
    >>> # Acessar configurações
    >>> print(Config.SPREADSHEET_ID)
    >>> print(Config.CREDENTIALS_PATH)
    >>> print(Config.LOG_LEVEL)
    >>> 
    >>> # Verificar se configuração está válida
    >>> Config.validate()
"""

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv


@dataclass
class Config:
    """
    Configuração centralizada da aplicação.
    
    Carrega variáveis de ambiente do arquivo .env e valida configurações
    obrigatórias. Fornece acesso tipado às configurações via propriedades
    de classe.
    
    Variáveis obrigatórias:
        - GOOGLE_SPREADSHEET_ID: ID da planilha Google Sheets
        - GOOGLE_CREDENTIALS_PATH: Caminho para arquivo credentials.json
    
    Variáveis opcionais:
        - TZ: Timezone (padrão: America/Sao_Paulo)
        - LOG_LEVEL: Nível de log (padrão: INFO)
    
    Raises:
        ValueError: Se variáveis obrigatórias não estiverem configuradas
    
    Example:
        >>> from src.utils.config import Config
        >>> print(f"Usando planilha: {Config.SPREADSHEET_ID}")
    """
    
    # Propriedades de classe (carregadas automaticamente)
    SPREADSHEET_ID: str = None
    CREDENTIALS_PATH: str = None
    TZ: str = "America/Sao_Paulo"
    LOG_LEVEL: str = "INFO"
    
    _initialized: bool = False
    _env_loaded: bool = False
    
    @classmethod
    def _load_env(cls) -> None:
        """
        Carrega variáveis de ambiente do arquivo .env.
        
        Procura arquivo .env no diretório raiz do projeto.
        Se não encontrar, usa variáveis de ambiente do sistema.
        """
        if cls._env_loaded:
            return
        
        # Procurar arquivo .env no diretório raiz do projeto
        # Assumindo estrutura: projeto/src/utils/config.py
        current_file = Path(__file__)
        project_root = current_file.parent.parent.parent
        env_path = project_root / ".env"
        
        if env_path.exists():
            load_dotenv(env_path)
            print(f"✓ Arquivo .env carregado: {env_path}")
        else:
            print(f"⚠ Arquivo .env não encontrado em: {env_path}")
            print("  Usando variáveis de ambiente do sistema")
        
        cls._env_loaded = True
    
    @classmethod
    def _initialize(cls) -> None:
        """
        Inicializa a configuração carregando e validando variáveis.
        
        Raises:
            ValueError: Se variáveis obrigatórias estiverem ausentes
        """
        if cls._initialized:
            return
        
        # Carregar .env
        cls._load_env()
        
        # Carregar variáveis obrigatórias
        cls.SPREADSHEET_ID = os.getenv("GOOGLE_SPREADSHEET_ID")
        cls.CREDENTIALS_PATH = os.getenv("GOOGLE_CREDENTIALS_PATH")
        
        # Carregar variáveis opcionais com padrões
        cls.TZ = os.getenv("TZ", "America/Sao_Paulo")
        cls.LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
        
        # Validar configuração
        cls.validate()
        
        cls._initialized = True
    
    @classmethod
    def validate(cls) -> None:
        """
        Valida que todas as variáveis obrigatórias estão configuradas.
        
        Raises:
            ValueError: Se alguma variável obrigatória estiver ausente
        """
        errors = []
        
        if not cls.SPREADSHEET_ID:
            errors.append(
                "GOOGLE_SPREADSHEET_ID não configurado. "
                "Defina no arquivo .env ou como variável de ambiente."
            )
        
        if not cls.CREDENTIALS_PATH:
            errors.append(
                "GOOGLE_CREDENTIALS_PATH não configurado. "
                "Defina no arquivo .env ou como variável de ambiente."
            )
        
        # Validar que arquivo de credenciais existe
        if cls.CREDENTIALS_PATH and not os.path.exists(cls.CREDENTIALS_PATH):
            errors.append(
                f"Arquivo de credenciais não encontrado: {cls.CREDENTIALS_PATH}"
            )
        
        # Validar LOG_LEVEL
        valid_log_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if cls.LOG_LEVEL not in valid_log_levels:
            errors.append(
                f"LOG_LEVEL inválido: {cls.LOG_LEVEL}. "
                f"Valores válidos: {', '.join(valid_log_levels)}"
            )
        
        if errors:
            error_msg = "Erros de configuração encontrados:\n" + "\n".join(
                f"  - {error}" for error in errors
            )
            raise ValueError(error_msg)
    
    @classmethod
    def get_all(cls) -> dict:
        """
        Retorna todas as configurações como dicionário.
        
        Returns:
            Dicionário com todas as configurações
        
        Example:
            >>> from src.utils.config import Config
            >>> config_dict = Config.get_all()
            >>> print(config_dict)
        """
        cls._initialize()
        
        return {
            "SPREADSHEET_ID": cls.SPREADSHEET_ID,
            "CREDENTIALS_PATH": cls.CREDENTIALS_PATH,
            "TZ": cls.TZ,
            "LOG_LEVEL": cls.LOG_LEVEL,
        }
    
    @classmethod
    def print_config(cls) -> None:
        """
        Imprime configurações de forma legível (ocultando informações sensíveis).
        
        Example:
            >>> from src.utils.config import Config
            >>> Config.print_config()
        """
        cls._initialize()
        
        # Ocultar parte do SPREADSHEET_ID
        spreadsheet_id_masked = cls.SPREADSHEET_ID
        if spreadsheet_id_masked and len(spreadsheet_id_masked) > 8:
            spreadsheet_id_masked = (
                spreadsheet_id_masked[:4] + 
                "..." + 
                spreadsheet_id_masked[-4:]
            )
        
        print("\n" + "=" * 50)
        print("CONFIGURAÇÃO DA APLICAÇÃO")
        print("=" * 50)
        print(f"Spreadsheet ID:     {spreadsheet_id_masked}")
        print(f"Credentials Path:   {cls.CREDENTIALS_PATH}")
        print(f"Timezone:           {cls.TZ}")
        print(f"Log Level:          {cls.LOG_LEVEL}")
        print("=" * 50 + "\n")
    
    @classmethod
    def reload(cls) -> None:
        """
        Recarrega configurações do ambiente.
        
        Útil para testes ou quando variáveis de ambiente mudam.
        """
        cls._initialized = False
        cls._env_loaded = False
        cls._initialize()


# Inicializar automaticamente ao importar o módulo
Config._initialize()


# Aliases para conveniência
SPREADSHEET_ID = Config.SPREADSHEET_ID
CREDENTIALS_PATH = Config.CREDENTIALS_PATH
TZ = Config.TZ
LOG_LEVEL = Config.LOG_LEVEL
