"""
Dimensão de Composição do CUB Médio.

Documenta quais tipos de CUB compõem cada categoria de CUB médio
segundo metodologia CBIC/Sinduscons.

Fonte: NBR 12721:2006 + Metodologia CBIC
"""

COMPOSICAO_CUB_MEDIO = [
    # CUB MÉDIO RESIDENCIAL
    {
        "sk_composicao": 1,
        "categoria_cub_medio": "Residencial",
        "tipo_cub_incluido": "R1-N",
        "peso_ponderacao": 0.20,
        "metodologia": "Média ponderada conforme NBR 12721:2006",
        "fonte_dados": "CBIC - Sinduscon",
        "vigencia_inicio": "2006-01-01",
        "vigencia_fim": None,
        "is_ativo": True
    },
    {
        "sk_composicao": 2,
        "categoria_cub_medio": "Residencial",
        "tipo_cub_incluido": "R8-N",
        "peso_ponderacao": 0.30,
        "metodologia": "Média ponderada conforme NBR 12721:2006",
        "fonte_dados": "CBIC - Sinduscon",
        "vigencia_inicio": "2006-01-01",
        "vigencia_fim": None,
        "is_ativo": True
    },
    {
        "sk_composicao": 3,
        "categoria_cub_medio": "Residencial",
        "tipo_cub_incluido": "R16-N",
        "peso_ponderacao": 0.20,
        "metodologia": "Média ponderada conforme NBR 12721:2006",
        "fonte_dados": "CBIC - Sinduscon",
        "vigencia_inicio": "2006-01-01",
        "vigencia_fim": None,
        "is_ativo": True
    },
    {
        "sk_composicao": 4,
        "categoria_cub_medio": "Residencial",
        "tipo_cub_incluido": "PP4-N",
        "peso_ponderacao": 0.15,
        "metodologia": "Média ponderada conforme NBR 12721:2006",
        "fonte_dados": "CBIC - Sinduscon",
        "vigencia_inicio": "2006-01-01",
        "vigencia_fim": None,
        "is_ativo": True
    },
    {
        "sk_composicao": 5,
        "categoria_cub_medio": "Residencial",
        "tipo_cub_incluido": "PIS",
        "peso_ponderacao": 0.10,
        "metodologia": "Média ponderada conforme NBR 12721:2006",
        "fonte_dados": "CBIC - Sinduscon",
        "vigencia_inicio": "2006-01-01",
        "vigencia_fim": None,
        "is_ativo": True
    },
    {
        "sk_composicao": 6,
        "categoria_cub_medio": "Residencial",
        "tipo_cub_incluido": "RP1Q",
        "peso_ponderacao": 0.05,
        "metodologia": "Média ponderada conforme NBR 12721:2006",
        "fonte_dados": "CBIC - Sinduscon",
        "vigencia_inicio": "2006-01-01",
        "vigencia_fim": None,
        "is_ativo": True
    },
    
    # CUB MÉDIO COMERCIAL
    {
        "sk_composicao": 7,
        "categoria_cub_medio": "Comercial",
        "tipo_cub_incluido": "CSL8-N",
        "peso_ponderacao": 0.40,
        "metodologia": "Média ponderada conforme NBR 12721:2006",
        "fonte_dados": "CBIC - Sinduscon",
        "vigencia_inicio": "2006-01-01",
        "vigencia_fim": None,
        "is_ativo": True
    },
    {
        "sk_composicao": 8,
        "categoria_cub_medio": "Comercial",
        "tipo_cub_incluido": "CSL16-N",
        "peso_ponderacao": 0.30,
        "metodologia": "Média ponderada conforme NBR 12721:2006",
        "fonte_dados": "CBIC - Sinduscon",
        "vigencia_inicio": "2006-01-01",
        "vigencia_fim": None,
        "is_ativo": True
    },
    {
        "sk_composicao": 9,
        "categoria_cub_medio": "Comercial",
        "tipo_cub_incluido": "CAL8-N",
        "peso_ponderacao": 0.30,
        "metodologia": "Média ponderada conforme NBR 12721:2006",
        "fonte_dados": "CBIC - Sinduscon",
        "vigencia_inicio": "2006-01-01",
        "vigencia_fim": None,
        "is_ativo": True
    },
    
    # CUB MÉDIO INDUSTRIAL
    {
        "sk_composicao": 10,
        "categoria_cub_medio": "Industrial",
        "tipo_cub_incluido": "GI",
        "peso_ponderacao": 1.00,
        "metodologia": "Tipo único para categoria",
        "fonte_dados": "CBIC - Sinduscon",
        "vigencia_inicio": "2006-01-01",
        "vigencia_fim": None,
        "is_ativo": True
    }
]


def get_tipos_por_categoria(categoria: str) -> list:
    """
    Retorna tipos que compõem uma categoria específica.
    
    Args:
        categoria: "Residencial", "Comercial" ou "Industrial"
    
    Returns:
        Lista de dicts com tipo_cub e peso
    """
    return [
        {
            "tipo_cub": c["tipo_cub_incluido"],
            "peso": c["peso_ponderacao"]
        }
        for c in COMPOSICAO_CUB_MEDIO
        if c["categoria_cub_medio"] == categoria and c["is_ativo"]
    ]


def calcular_cub_medio(valores_por_tipo: dict, categoria: str) -> float:
    """
    Calcula CUB médio de uma categoria baseado nos valores dos tipos.
    
    Args:
        valores_por_tipo: Dict {tipo_cub: valor}
        categoria: "Residencial", "Comercial" ou "Industrial"
    
    Returns:
        Valor do CUB médio ponderado
    
    Exemplo:
        >>> valores = {"R1-N": 2500.00, "R8-N": 2800.00, ...}
        >>> calcular_cub_medio(valores, "Residencial")
        2650.50
    """
    composicao = get_tipos_por_categoria(categoria)
    
    valor_medio = sum(
        valores_por_tipo.get(item["tipo_cub"], 0) * item["peso"]
        for item in composicao
    )
    
    return round(valor_medio, 2)
