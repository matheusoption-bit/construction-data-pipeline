"""
Módulo para validação de qualidade de dados de séries temporais.

Este módulo fornece funções para detectar anomalias, outliers e problemas
de qualidade em dados econômicos e financeiros.

Exemplo de uso:
    >>> import pandas as pd
    >>> from src.etl.quality import (
    ...     check_outliers_zscore,
    ...     check_mom_variation,
    ...     run_quality_checks
    ... )
    >>> 
    >>> # Criar DataFrame de exemplo
    >>> df = pd.DataFrame({
    ...     "date": pd.date_range("2023-01-01", periods=12, freq="MS"),
    ...     "value": [100, 102, 101, 103, 105, 200, 106, 107, 108, 109, 110, 111]
    ... })
    >>> 
    >>> # Detectar outliers
    >>> outliers = check_outliers_zscore(df["value"])
    >>> print(outliers[outliers["is_outlier"]])
    >>> 
    >>> # Executar todos os checks
    >>> flags = run_quality_checks(df, "ipca")
    >>> print(f"Encontrados {len(flags)} problemas")
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
import structlog
from scipy import stats

logger = structlog.get_logger(__name__)


def check_outliers_zscore(
    series: pd.Series,
    threshold: float = 3.0
) -> pd.DataFrame:
    """
    Detecta outliers em uma série usando z-score.
    
    O z-score mede quantos desvios padrão um valor está distante da média.
    Valores com |z-score| > threshold são considerados outliers.
    
    Args:
        series: Série pandas com valores numéricos
        threshold: Limiar de z-score para considerar outlier (padrão: 3.0)
    
    Returns:
        DataFrame com colunas:
        - index: Índice original da série
        - value: Valor original
        - zscore: Z-score calculado
        - is_outlier: Boolean indicando se é outlier
    
    Example:
        >>> data = pd.Series([1, 2, 3, 100, 4, 5])
        >>> outliers = check_outliers_zscore(data, threshold=2.0)
        >>> print(outliers[outliers["is_outlier"]])
    """
    logger.debug(
        "checking_outliers_zscore",
        series_length=len(series),
        threshold=threshold
    )
    
    # Remover valores nulos
    clean_series = series.dropna()
    
    if len(clean_series) < 3:
        logger.warning(
            "insufficient_data_for_zscore",
            series_length=len(clean_series)
        )
        return pd.DataFrame({
            "index": clean_series.index,
            "value": clean_series.values,
            "zscore": np.nan,
            "is_outlier": False
        })
    
    # Calcular z-score
    z_scores = np.abs(stats.zscore(clean_series))
    
    # Criar DataFrame de resultado
    result = pd.DataFrame({
        "index": clean_series.index,
        "value": clean_series.values,
        "zscore": z_scores,
        "is_outlier": z_scores > threshold
    })
    
    outliers_count = result["is_outlier"].sum()
    
    logger.info(
        "outliers_check_completed",
        total_values=len(result),
        outliers_found=outliers_count,
        outlier_percentage=round(outliers_count / len(result) * 100, 2)
    )
    
    return result


def check_mom_variation(
    df: pd.DataFrame,
    value_col: str,
    threshold: float = 0.10
) -> pd.DataFrame:
    """
    Detecta variações mês-a-mês (MoM) acima do threshold.
    
    Calcula a variação percentual entre períodos consecutivos e identifica
    variações anormalmente altas que podem indicar problemas nos dados.
    
    Args:
        df: DataFrame com dados ordenados cronologicamente
        value_col: Nome da coluna com valores numéricos
        threshold: Limiar de variação absoluta (padrão: 0.10 = 10%)
    
    Returns:
        DataFrame com linhas onde variação MoM > threshold, contendo:
        - Colunas originais do DataFrame
        - mom_variation: Variação percentual calculada
        - mom_variation_abs: Valor absoluto da variação
    
    Raises:
        ValueError: Se coluna especificada não existir
        KeyError: Se DataFrame não tiver coluna 'date'
    
    Example:
        >>> df = pd.DataFrame({
        ...     "date": pd.date_range("2023-01-01", periods=5, freq="MS"),
        ...     "value": [100, 102, 150, 152, 154]
        ... })
        >>> high_variations = check_mom_variation(df, "value", threshold=0.20)
    """
    if value_col not in df.columns:
        raise ValueError(f"Coluna '{value_col}' não encontrada no DataFrame")
    
    if "date" not in df.columns:
        raise KeyError("DataFrame deve conter coluna 'date'")
    
    logger.debug(
        "checking_mom_variation",
        rows=len(df),
        value_col=value_col,
        threshold=threshold
    )
    
    # Criar cópia e ordenar por data
    df_sorted = df.sort_values("date").copy()
    
    # Calcular variação MoM
    df_sorted["mom_variation"] = df_sorted[value_col].pct_change()
    df_sorted["mom_variation_abs"] = df_sorted["mom_variation"].abs()
    
    # Filtrar variações acima do threshold
    high_variations = df_sorted[df_sorted["mom_variation_abs"] > threshold].copy()
    
    logger.info(
        "mom_variation_check_completed",
        total_periods=len(df_sorted),
        high_variations_found=len(high_variations),
        threshold_percentage=threshold * 100
    )
    
    return high_variations


def check_negative_values(
    df: pd.DataFrame,
    value_cols: List[str]
) -> pd.DataFrame:
    """
    Detecta valores negativos em colunas que não devem aceitar negativos.
    
    Útil para validar séries que representam índices, preços, taxas positivas, etc.
    
    Args:
        df: DataFrame com dados a validar
        value_cols: Lista de nomes de colunas para verificar
    
    Returns:
        DataFrame com linhas contendo valores negativos, incluindo:
        - Todas as colunas originais
        - problematic_columns: Lista de colunas com valores negativos
    
    Raises:
        ValueError: Se alguma coluna especificada não existir
    
    Example:
        >>> df = pd.DataFrame({
        ...     "date": ["2023-01-01", "2023-02-01"],
        ...     "price": [100, -50],
        ...     "volume": [1000, 1500]
        ... })
        >>> negatives = check_negative_values(df, ["price", "volume"])
    """
    # Validar colunas
    missing_cols = set(value_cols) - set(df.columns)
    if missing_cols:
        raise ValueError(f"Colunas não encontradas: {missing_cols}")
    
    logger.debug(
        "checking_negative_values",
        rows=len(df),
        columns=value_cols
    )
    
    # Identificar linhas com valores negativos
    has_negative = pd.Series([False] * len(df), index=df.index)
    problematic_columns = []
    
    for col in value_cols:
        col_has_negative = df[col] < 0
        has_negative |= col_has_negative
        
        if col_has_negative.any():
            problematic_columns.append(col)
    
    # Filtrar linhas com problemas
    negative_rows = df[has_negative].copy()
    
    if len(negative_rows) > 0:
        # Adicionar coluna indicando quais colunas têm negativos em cada linha
        negative_rows["problematic_columns"] = negative_rows.apply(
            lambda row: [col for col in value_cols if row[col] < 0],
            axis=1
        )
    
    logger.info(
        "negative_values_check_completed",
        total_rows=len(df),
        negative_rows_found=len(negative_rows),
        problematic_columns=problematic_columns
    )
    
    return negative_rows


def calculate_mom_yoy(
    df: pd.DataFrame,
    value_col: str = "value"
) -> pd.DataFrame:
    """
    Calcula variações mês-a-mês (MoM) e ano-a-ano (YoY).
    
    Args:
        df: DataFrame com dados ordenados por data
        value_col: Nome da coluna com valores (padrão: "value")
    
    Returns:
        DataFrame original com colunas adicionais:
        - variacao_mom: Variação percentual em relação ao mês anterior
        - variacao_yoy: Variação percentual em relação a 12 meses atrás
    
    Raises:
        ValueError: Se coluna especificada não existir
    
    Example:
        >>> df = pd.DataFrame({
        ...     "date": pd.date_range("2022-01-01", periods=15, freq="MS"),
        ...     "value": range(100, 115)
        ... })
        >>> df_with_variations = calculate_mom_yoy(df)
    """
    if value_col not in df.columns:
        raise ValueError(f"Coluna '{value_col}' não encontrada no DataFrame")
    
    logger.debug(
        "calculating_mom_yoy",
        rows=len(df),
        value_col=value_col
    )
    
    # Criar cópia
    df_result = df.copy()
    
    # Calcular MoM (mês sobre mês)
    df_result["variacao_mom"] = df_result[value_col].pct_change() * 100
    
    # Calcular YoY (ano sobre ano - 12 períodos)
    df_result["variacao_yoy"] = df_result[value_col].pct_change(periods=12) * 100
    
    # Contar valores calculados
    mom_count = df_result["variacao_mom"].notna().sum()
    yoy_count = df_result["variacao_yoy"].notna().sum()
    
    logger.info(
        "mom_yoy_calculated",
        total_rows=len(df_result),
        mom_values=mom_count,
        yoy_values=yoy_count
    )
    
    return df_result


def run_quality_checks(
    df: pd.DataFrame,
    series_id: str,
    checks: Optional[List[str]] = None
) -> List[Dict[str, Any]]:
    """
    Executa bateria de verificações de qualidade em uma série temporal.
    
    Args:
        df: DataFrame com colunas 'date' e 'value' (mínimo)
        series_id: Identificador da série (ex: "ipca", "selic")
        checks: Lista de checks a executar. Se None, executa todos.
                Opções: ["outliers", "mom_variation", "negative_values"]
    
    Returns:
        Lista de dicionários com flags de qualidade, cada um contendo:
        - series_id: ID da série
        - data_referencia: Data do problema (formato YYYY-MM-DD)
        - tipo_flag: Tipo de problema detectado
        - severidade: "high", "medium" ou "low"
        - valor_observado: Valor que causou o flag
        - desvio_padrao: Desvio padrão da série (para contexto)
        - detalhes: Informações adicionais sobre o problema
    
    Raises:
        ValueError: Se colunas necessárias não existirem
    
    Example:
        >>> df = pd.DataFrame({
        ...     "date": pd.date_range("2023-01-01", periods=12, freq="MS"),
        ...     "value": [100, 102, 101, 103, 105, 200, 106, 107, 108, 109, 110, 111]
        ... })
        >>> flags = run_quality_checks(df, "test_series")
        >>> print(f"Problemas encontrados: {len(flags)}")
    """
    required_cols = {"date", "value"}
    if not required_cols.issubset(df.columns):
        missing = required_cols - set(df.columns)
        raise ValueError(f"DataFrame deve conter colunas {required_cols}. Faltando: {missing}")
    
    if checks is None:
        checks = ["outliers", "mom_variation", "negative_values"]
    
    logger.info(
        "running_quality_checks",
        series_id=series_id,
        rows=len(df),
        checks=checks
    )
    
    flags = []
    
    # Calcular desvio padrão para contexto
    std_dev = float(df["value"].std())
    mean_value = float(df["value"].mean())
    
    # Check 1: Outliers por z-score
    if "outliers" in checks:
        try:
            outliers_df = check_outliers_zscore(df["value"], threshold=3.0)
            outliers = outliers_df[outliers_df["is_outlier"]]
            
            for idx, row in outliers.iterrows():
                original_idx = row["index"]
                date_value = df.loc[original_idx, "date"]
                
                # Converter date para string se necessário
                if isinstance(date_value, pd.Timestamp):
                    date_str = date_value.strftime("%Y-%m-%d")
                else:
                    date_str = str(date_value)
                
                flags.append({
                    "series_id": series_id,
                    "data_referencia": date_str,
                    "tipo_flag": "outlier_zscore",
                    "severidade": "high" if abs(row["zscore"]) > 4 else "medium",
                    "valor_observado": float(row["value"]),
                    "desvio_padrao": std_dev,
                    "detalhes": f"Z-score: {row['zscore']:.2f}, média: {mean_value:.2f}"
                })
            
            logger.info(
                "outliers_check_completed",
                series_id=series_id,
                outliers_found=len(outliers)
            )
        
        except Exception as e:
            logger.error(
                "outliers_check_failed",
                series_id=series_id,
                error=str(e),
                error_type=type(e).__name__
            )
    
    # Check 2: Variações MoM altas
    if "mom_variation" in checks:
        try:
            high_variations = check_mom_variation(df, "value", threshold=0.10)
            
            for idx, row in high_variations.iterrows():
                date_value = row["date"]
                
                if isinstance(date_value, pd.Timestamp):
                    date_str = date_value.strftime("%Y-%m-%d")
                else:
                    date_str = str(date_value)
                
                mom_var = row["mom_variation"]
                severity = "high" if abs(mom_var) > 0.25 else "medium"
                
                flags.append({
                    "series_id": series_id,
                    "data_referencia": date_str,
                    "tipo_flag": "high_mom_variation",
                    "severidade": severity,
                    "valor_observado": float(row["value"]),
                    "desvio_padrao": std_dev,
                    "detalhes": f"Variação MoM: {mom_var*100:.2f}%"
                })
            
            logger.info(
                "mom_variation_check_completed",
                series_id=series_id,
                high_variations_found=len(high_variations)
            )
        
        except Exception as e:
            logger.error(
                "mom_variation_check_failed",
                series_id=series_id,
                error=str(e),
                error_type=type(e).__name__
            )
    
    # Check 3: Valores negativos
    if "negative_values" in checks:
        try:
            negative_df = check_negative_values(df, ["value"])
            
            for idx, row in negative_df.iterrows():
                date_value = row["date"]
                
                if isinstance(date_value, pd.Timestamp):
                    date_str = date_value.strftime("%Y-%m-%d")
                else:
                    date_str = str(date_value)
                
                flags.append({
                    "series_id": series_id,
                    "data_referencia": date_str,
                    "tipo_flag": "negative_value",
                    "severidade": "high",
                    "valor_observado": float(row["value"]),
                    "desvio_padrao": std_dev,
                    "detalhes": f"Valor negativo detectado: {row['value']}"
                })
            
            logger.info(
                "negative_values_check_completed",
                series_id=series_id,
                negative_values_found=len(negative_df)
            )
        
        except Exception as e:
            logger.error(
                "negative_values_check_failed",
                series_id=series_id,
                error=str(e),
                error_type=type(e).__name__
            )
    
    logger.info(
        "quality_checks_completed",
        series_id=series_id,
        total_flags=len(flags),
        checks_executed=checks
    )
    
    return flags
