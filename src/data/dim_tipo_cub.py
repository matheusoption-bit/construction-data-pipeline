"""
Dimensão de tipos de CUB (Custo Unitário Básico).

Este módulo contém os tipos de dados CUB disponíveis no sistema,
incluindo valores absolutos e variações percentuais.

Uso:
    from src.data.dim_tipo_cub import get_tipo_by_codigo, to_dataframe
    
    # Buscar tipo específico
    cub_medio = get_tipo_by_codigo("CUB-medio")
    
    # Buscar por categoria
    variacoes = get_tipos_by_categoria("Variação")
    
    # Converter para DataFrame
    df = to_dataframe()
"""

from typing import List, Optional
import pandas as pd


# Dimensão de tipos de CUB
TIPOS_CUB = [
    {
        "sk_tipo_cub": 1,
        "codigo": "CUB-medio",
        "nome": "Custo Unitário Básico Médio",
        "descricao": "Valor médio do CUB em R$/m² de construção. Representa o custo médio de construção por metro quadrado.",
        "unidade_medida": "R$/m²",
        "categoria": "Valor",
        "fonte_calculo": "ABNT NBR 12721",
        "periodicidade": "Mensal",
        "is_ativo": True
    },
    {
        "sk_tipo_cub": 2,
        "codigo": "Variacao_mensal_%",
        "nome": "Variação Mensal",
        "descricao": "Variação percentual do CUB em relação ao mês anterior. Indica a inflação mensal do setor.",
        "unidade_medida": "%",
        "categoria": "Variação",
        "fonte_calculo": "Comparação mês a mês",
        "periodicidade": "Mensal",
        "is_ativo": True
    },
    {
        "sk_tipo_cub": 3,
        "codigo": "Variacao_12meses_%",
        "nome": "Variação 12 Meses",
        "descricao": "Variação percentual acumulada nos últimos 12 meses. Mostra a inflação anual do setor.",
        "unidade_medida": "%",
        "categoria": "Variação",
        "fonte_calculo": "Acumulado 12 meses",
        "periodicidade": "Mensal",
        "is_ativo": True
    },
    {
        "sk_tipo_cub": 4,
        "codigo": "Variacao_ano_%",
        "nome": "Variação no Ano",
        "descricao": "Variação percentual acumulada desde janeiro do ano corrente. Indica a inflação acumulada no ano.",
        "unidade_medida": "%",
        "categoria": "Variação",
        "fonte_calculo": "Acumulado desde janeiro",
        "periodicidade": "Mensal",
        "is_ativo": True
    }
]


def get_tipo_by_codigo(codigo: str) -> Optional[dict]:
    """
    Busca tipo de CUB por código.
    
    Args:
        codigo: Código do tipo (ex: "CUB-medio", "Variacao_mensal_%")
        
    Returns:
        Dicionário com dados do tipo ou None se não encontrado
        
    Examples:
        >>> cub = get_tipo_by_codigo("CUB-medio")
        >>> cub["nome"]
        'Custo Unitário Básico Médio'
        >>> cub["unidade_medida"]
        'R$/m²'
        >>> cub["categoria"]
        'Valor'
        
        >>> var = get_tipo_by_codigo("Variacao_mensal_%")
        >>> var["nome"]
        'Variação Mensal'
        >>> var["categoria"]
        'Variação'
        
        >>> invalido = get_tipo_by_codigo("INEXISTENTE")
        >>> invalido is None
        True
    """
    codigo_stripped = codigo.strip()
    
    for tipo in TIPOS_CUB:
        if tipo["codigo"] == codigo_stripped:
            return tipo.copy()
    
    return None


def get_tipos_by_categoria(categoria: str) -> List[dict]:
    """
    Busca todos os tipos de uma categoria.
    
    Args:
        categoria: Nome da categoria ("Valor" ou "Variação")
        
    Returns:
        Lista de dicionários com dados dos tipos
        
    Examples:
        >>> valores = get_tipos_by_categoria("Valor")
        >>> len(valores)
        1
        >>> valores[0]["codigo"]
        'CUB-medio'
        
        >>> variacoes = get_tipos_by_categoria("Variação")
        >>> len(variacoes)
        3
        >>> sorted([t["codigo"] for t in variacoes])
        ['Variacao_12meses_%', 'Variacao_ano_%', 'Variacao_mensal_%']
        
        >>> invalida = get_tipos_by_categoria("Desconhecida")
        >>> invalida
        []
    """
    categoria_capitalized = categoria.strip().title()
    
    resultado = [
        tipo.copy()
        for tipo in TIPOS_CUB
        if tipo["categoria"] == categoria_capitalized
    ]
    
    return resultado


def get_all_codigos() -> List[str]:
    """
    Retorna lista com todos os códigos de tipos CUB.
    
    Returns:
        Lista com os 4 códigos de tipos CUB
        
    Examples:
        >>> codigos = get_all_codigos()
        >>> len(codigos)
        4
        >>> "CUB-medio" in codigos
        True
        >>> "Variacao_mensal_%" in codigos
        True
    """
    return [tipo["codigo"] for tipo in TIPOS_CUB]


def get_all_categorias() -> List[str]:
    """
    Retorna lista com todas as categorias de tipos CUB.
    
    Returns:
        Lista com as categorias únicas
        
    Examples:
        >>> categorias = get_all_categorias()
        >>> len(categorias)
        2
        >>> "Valor" in categorias
        True
        >>> "Variação" in categorias
        True
    """
    categorias = list(set(tipo["categoria"] for tipo in TIPOS_CUB))
    return sorted(categorias)


def to_dataframe() -> pd.DataFrame:
    """
    Converte TIPOS_CUB para DataFrame pandas.
    
    Returns:
        DataFrame com todos os tipos de CUB
        
    Examples:
        >>> df = to_dataframe()
        >>> df.shape
        (4, 9)
        >>> df.columns.tolist()
        ['sk_tipo_cub', 'codigo', 'nome', 'descricao', 'unidade_medida',
         'categoria', 'fonte_calculo', 'periodicidade', 'is_ativo']
        >>> df[df["codigo"] == "CUB-medio"]["categoria"].values[0]
        'Valor'
        >>> df["categoria"].value_counts()["Variação"]
        3
    """
    df = pd.DataFrame(TIPOS_CUB)
    
    # Garantir ordem das colunas
    colunas_ordenadas = [
        "sk_tipo_cub",
        "codigo",
        "nome",
        "descricao",
        "unidade_medida",
        "categoria",
        "fonte_calculo",
        "periodicidade",
        "is_ativo"
    ]
    
    return df[colunas_ordenadas]


def get_tipo_info(codigo: str) -> str:
    """
    Retorna informações formatadas sobre um tipo de CUB.
    
    Args:
        codigo: Código do tipo
        
    Returns:
        String formatada com informações do tipo ou mensagem de erro
        
    Examples:
        >>> info = get_tipo_info("CUB-medio")
        >>> "Custo Unitário Básico Médio" in info
        True
        >>> "R$/m²" in info
        True
    """
    tipo = get_tipo_by_codigo(codigo)
    
    if not tipo:
        return f"❌ Tipo '{codigo}' não encontrado."
    
    return f"""
📊 {tipo['nome']}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
• Código: {tipo['codigo']}
• Categoria: {tipo['categoria']}
• Unidade: {tipo['unidade_medida']}
• Periodicidade: {tipo['periodicidade']}
• Fonte: {tipo['fonte_calculo']}
• Descrição: {tipo['descricao']}
• Status: {'✅ Ativo' if tipo['is_ativo'] else '❌ Inativo'}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    """.strip()


def get_statistics() -> dict:
    """
    Retorna estatísticas sobre os tipos de CUB.
    
    Returns:
        Dicionário com estatísticas gerais
        
    Examples:
        >>> stats = get_statistics()
        >>> stats["total_tipos"]
        4
        >>> stats["tipos_por_categoria"]["Valor"]
        1
        >>> stats["tipos_por_categoria"]["Variação"]
        3
        >>> stats["tipos_ativos"]
        4
    """
    df = to_dataframe()
    
    return {
        "total_tipos": len(df),
        "tipos_ativos": int(df["is_ativo"].sum()),
        "tipos_inativos": int((~df["is_ativo"]).sum()),
        "tipos_por_categoria": df["categoria"].value_counts().to_dict(),
        "unidades_medida": df["unidade_medida"].unique().tolist(),
        "periodicidades": df["periodicidade"].unique().tolist(),
        "fontes_calculo": df["fonte_calculo"].unique().tolist()
    }


if __name__ == "__main__":
    # Demonstração de uso
    print("="*80)
    print("  📊 DIMENSÃO DE TIPOS CUB")
    print("="*80 + "\n")
    
    # Buscar tipo específico
    print("🔍 Exemplo 1: Buscar tipo por código")
    cub_medio = get_tipo_by_codigo("CUB-medio")
    if cub_medio:
        print(f"   Código: {cub_medio['codigo']}")
        print(f"   Nome: {cub_medio['nome']}")
        print(f"   Categoria: {cub_medio['categoria']}")
        print(f"   Unidade: {cub_medio['unidade_medida']}")
    print()
    
    # Buscar por categoria
    print("🔍 Exemplo 2: Buscar tipos por categoria")
    variacoes = get_tipos_by_categoria("Variação")
    print(f"   Categoria 'Variação': {len(variacoes)} tipos")
    for tipo in variacoes:
        print(f"   - {tipo['codigo']}: {tipo['nome']}")
    print()
    
    # Converter para DataFrame
    print("📊 Exemplo 3: Converter para DataFrame")
    df = to_dataframe()
    print(f"   Shape: {df.shape}")
    print(f"   Colunas: {len(df.columns)}")
    print()
    
    # Informações detalhadas
    print("📋 Exemplo 4: Informações detalhadas")
    print(get_tipo_info("CUB-medio"))
    print()
    
    # Estatísticas
    print("📈 Estatísticas Gerais:")
    stats = get_statistics()
    print(f"   Total de tipos: {stats['total_tipos']}")
    print(f"   Tipos ativos: {stats['tipos_ativos']}")
    print(f"   Tipos por categoria:")
    for cat, count in sorted(stats['tipos_por_categoria'].items()):
        print(f"      - {cat}: {count}")
    print(f"   Unidades de medida: {', '.join(stats['unidades_medida'])}")
    print()
    
    # Listar todos
    print("📋 Todos os tipos CUB:")
    for tipo in TIPOS_CUB:
        status = "✅" if tipo["is_ativo"] else "❌"
        print(f"   {status} {tipo['codigo']:25s} | {tipo['categoria']:10s} | {tipo['unidade_medida']}")
    print()
    
    print("✅ Módulo pronto para uso!")
