"""
Dimensão de localidades (estados brasileiros).

Este módulo contém os dados de todos os 27 estados do Brasil,
incluindo informações geográficas, demográficas e administrativas.

Uso:
    from src.data.dim_localidade import get_localidade_by_uf, to_dataframe
    
    # Buscar estado específico
    sp = get_localidade_by_uf("SP")
    
    # Buscar por região
    estados_sul = get_localidades_by_regiao("Sul")
    
    # Converter para DataFrame
    df = to_dataframe()
"""

from typing import List, Optional
import pandas as pd


# Dimensão de localidades - Estados brasileiros
LOCALIDADES = [
    {
        "sk_localidade": 1,
        "uf": "AC",
        "nome_uf": "Acre",
        "regiao": "Norte",
        "sigla_regiao": "N",
        "capital": "Rio Branco",
        "populacao_2024": 906876,
        "area_km2": 164173.0,
        "is_ativo": True,
        "data_criacao": "1962-06-15",
        "codigo_ibge": 12
    },
    {
        "sk_localidade": 2,
        "uf": "AL",
        "nome_uf": "Alagoas",
        "regiao": "Nordeste",
        "sigla_regiao": "NE",
        "capital": "Maceió",
        "populacao_2024": 3365351,
        "area_km2": 27843.3,
        "is_ativo": True,
        "data_criacao": "1817-09-16",
        "codigo_ibge": 27
    },
    {
        "sk_localidade": 3,
        "uf": "AP",
        "nome_uf": "Amapá",
        "regiao": "Norte",
        "sigla_regiao": "N",
        "capital": "Macapá",
        "populacao_2024": 877613,
        "area_km2": 142470.8,
        "is_ativo": True,
        "data_criacao": "1988-10-05",
        "codigo_ibge": 16
    },
    {
        "sk_localidade": 4,
        "uf": "AM",
        "nome_uf": "Amazonas",
        "regiao": "Norte",
        "sigla_regiao": "N",
        "capital": "Manaus",
        "populacao_2024": 4269995,
        "area_km2": 1559167.9,
        "is_ativo": True,
        "data_criacao": "1850-09-05",
        "codigo_ibge": 13
    },
    {
        "sk_localidade": 5,
        "uf": "BA",
        "nome_uf": "Bahia",
        "regiao": "Nordeste",
        "sigla_regiao": "NE",
        "capital": "Salvador",
        "populacao_2024": 14985284,
        "area_km2": 564733.1,
        "is_ativo": True,
        "data_criacao": "1534-06-08",
        "codigo_ibge": 29
    },
    {
        "sk_localidade": 6,
        "uf": "CE",
        "nome_uf": "Ceará",
        "regiao": "Nordeste",
        "sigla_regiao": "NE",
        "capital": "Fortaleza",
        "populacao_2024": 9240580,
        "area_km2": 148894.4,
        "is_ativo": True,
        "data_criacao": "1799-07-28",
        "codigo_ibge": 23
    },
    {
        "sk_localidade": 7,
        "uf": "DF",
        "nome_uf": "Distrito Federal",
        "regiao": "Centro-Oeste",
        "sigla_regiao": "CO",
        "capital": "Brasília",
        "populacao_2024": 3094325,
        "area_km2": 5760.8,
        "is_ativo": True,
        "data_criacao": "1960-04-21",
        "codigo_ibge": 53
    },
    {
        "sk_localidade": 8,
        "uf": "ES",
        "nome_uf": "Espírito Santo",
        "regiao": "Sudeste",
        "sigla_regiao": "SE",
        "capital": "Vitória",
        "populacao_2024": 4108508,
        "area_km2": 46074.4,
        "is_ativo": True,
        "data_criacao": "1534-06-23",
        "codigo_ibge": 32
    },
    {
        "sk_localidade": 9,
        "uf": "GO",
        "nome_uf": "Goiás",
        "regiao": "Centro-Oeste",
        "sigla_regiao": "CO",
        "capital": "Goiânia",
        "populacao_2024": 7206589,
        "area_km2": 340242.8,
        "is_ativo": True,
        "data_criacao": "1744-05-11",
        "codigo_ibge": 52
    },
    {
        "sk_localidade": 10,
        "uf": "MA",
        "nome_uf": "Maranhão",
        "regiao": "Nordeste",
        "sigla_regiao": "NE",
        "capital": "São Luís",
        "populacao_2024": 7153262,
        "area_km2": 329642.2,
        "is_ativo": True,
        "data_criacao": "1621-08-10",
        "codigo_ibge": 21
    },
    {
        "sk_localidade": 11,
        "uf": "MT",
        "nome_uf": "Mato Grosso",
        "regiao": "Centro-Oeste",
        "sigla_regiao": "CO",
        "capital": "Cuiabá",
        "populacao_2024": 3658649,
        "area_km2": 903206.9,
        "is_ativo": True,
        "data_criacao": "1748-05-09",
        "codigo_ibge": 51
    },
    {
        "sk_localidade": 12,
        "uf": "MS",
        "nome_uf": "Mato Grosso do Sul",
        "regiao": "Centro-Oeste",
        "sigla_regiao": "CO",
        "capital": "Campo Grande",
        "populacao_2024": 2839188,
        "area_km2": 357142.1,
        "is_ativo": True,
        "data_criacao": "1977-10-11",
        "codigo_ibge": 50
    },
    {
        "sk_localidade": 13,
        "uf": "MG",
        "nome_uf": "Minas Gerais",
        "regiao": "Sudeste",
        "sigla_regiao": "SE",
        "capital": "Belo Horizonte",
        "populacao_2024": 21411923,
        "area_km2": 586513.9,
        "is_ativo": True,
        "data_criacao": "1709-07-02",
        "codigo_ibge": 31
    },
    {
        "sk_localidade": 14,
        "uf": "PA",
        "nome_uf": "Pará",
        "regiao": "Norte",
        "sigla_regiao": "N",
        "capital": "Belém",
        "populacao_2024": 8777124,
        "area_km2": 1245870.7,
        "is_ativo": True,
        "data_criacao": "1621-08-10",
        "codigo_ibge": 15
    },
    {
        "sk_localidade": 15,
        "uf": "PB",
        "nome_uf": "Paraíba",
        "regiao": "Nordeste",
        "sigla_regiao": "NE",
        "capital": "João Pessoa",
        "populacao_2024": 4059905,
        "area_km2": 56467.2,
        "is_ativo": True,
        "data_criacao": "1585-08-05",
        "codigo_ibge": 25
    },
    {
        "sk_localidade": 16,
        "uf": "PR",
        "nome_uf": "Paraná",
        "regiao": "Sul",
        "sigla_regiao": "S",
        "capital": "Curitiba",
        "populacao_2024": 11597484,
        "area_km2": 199307.9,
        "is_ativo": True,
        "data_criacao": "1853-12-19",
        "codigo_ibge": 41
    },
    {
        "sk_localidade": 17,
        "uf": "PE",
        "nome_uf": "Pernambuco",
        "regiao": "Nordeste",
        "sigla_regiao": "NE",
        "capital": "Recife",
        "populacao_2024": 9674793,
        "area_km2": 98068.0,
        "is_ativo": True,
        "data_criacao": "1534-03-12",
        "codigo_ibge": 26
    },
    {
        "sk_localidade": 18,
        "uf": "PI",
        "nome_uf": "Piauí",
        "regiao": "Nordeste",
        "sigla_regiao": "NE",
        "capital": "Teresina",
        "populacao_2024": 3289290,
        "area_km2": 251755.5,
        "is_ativo": True,
        "data_criacao": "1811-08-19",
        "codigo_ibge": 22
    },
    {
        "sk_localidade": 19,
        "uf": "RJ",
        "nome_uf": "Rio de Janeiro",
        "regiao": "Sudeste",
        "sigla_regiao": "SE",
        "capital": "Rio de Janeiro",
        "populacao_2024": 17463349,
        "area_km2": 43750.4,
        "is_ativo": True,
        "data_criacao": "1565-03-01",
        "codigo_ibge": 33
    },
    {
        "sk_localidade": 20,
        "uf": "RN",
        "nome_uf": "Rio Grande do Norte",
        "regiao": "Nordeste",
        "sigla_regiao": "NE",
        "capital": "Natal",
        "populacao_2024": 3560903,
        "area_km2": 52809.6,
        "is_ativo": True,
        "data_criacao": "1597-08-11",
        "codigo_ibge": 24
    },
    {
        "sk_localidade": 21,
        "uf": "RS",
        "nome_uf": "Rio Grande do Sul",
        "regiao": "Sul",
        "sigla_regiao": "S",
        "capital": "Porto Alegre",
        "populacao_2024": 11466630,
        "area_km2": 281707.2,
        "is_ativo": True,
        "data_criacao": "1807-09-19",
        "codigo_ibge": 43
    },
    {
        "sk_localidade": 22,
        "uf": "RO",
        "nome_uf": "Rondônia",
        "regiao": "Norte",
        "sigla_regiao": "N",
        "capital": "Porto Velho",
        "populacao_2024": 1815278,
        "area_km2": 237765.3,
        "is_ativo": True,
        "data_criacao": "1981-12-22",
        "codigo_ibge": 11
    },
    {
        "sk_localidade": 23,
        "uf": "RR",
        "nome_uf": "Roraima",
        "regiao": "Norte",
        "sigla_regiao": "N",
        "capital": "Boa Vista",
        "populacao_2024": 652713,
        "area_km2": 224273.8,
        "is_ativo": True,
        "data_criacao": "1988-10-05",
        "codigo_ibge": 14
    },
    {
        "sk_localidade": 24,
        "uf": "SC",
        "nome_uf": "Santa Catarina",
        "regiao": "Sul",
        "sigla_regiao": "S",
        "capital": "Florianópolis",
        "populacao_2024": 7338473,
        "area_km2": 95730.9,
        "is_ativo": True,
        "data_criacao": "1738-03-11",
        "codigo_ibge": 42
    },
    {
        "sk_localidade": 25,
        "uf": "SP",
        "nome_uf": "São Paulo",
        "regiao": "Sudeste",
        "sigla_regiao": "SE",
        "capital": "São Paulo",
        "populacao_2024": 46649132,
        "area_km2": 248219.5,
        "is_ativo": True,
        "data_criacao": "1532-01-22",
        "codigo_ibge": 35
    },
    {
        "sk_localidade": 26,
        "uf": "SE",
        "nome_uf": "Sergipe",
        "regiao": "Nordeste",
        "sigla_regiao": "NE",
        "capital": "Aracaju",
        "populacao_2024": 2338474,
        "area_km2": 21938.2,
        "is_ativo": True,
        "data_criacao": "1820-07-08",
        "codigo_ibge": 28
    },
    {
        "sk_localidade": 27,
        "uf": "TO",
        "nome_uf": "Tocantins",
        "regiao": "Norte",
        "sigla_regiao": "N",
        "capital": "Palmas",
        "populacao_2024": 1607363,
        "area_km2": 277423.6,
        "is_ativo": True,
        "data_criacao": "1988-10-05",
        "codigo_ibge": 17
    }
]


def get_localidade_by_uf(uf: str) -> Optional[dict]:
    """
    Busca localidade por sigla da UF.
    
    Args:
        uf: Sigla do estado (ex: "SP", "RJ", "MG")
        
    Returns:
        Dicionário com dados da localidade ou None se não encontrado
        
    Examples:
        >>> sp = get_localidade_by_uf("SP")
        >>> sp["nome_uf"]
        'São Paulo'
        >>> sp["regiao"]
        'Sudeste'
        >>> sp["populacao_2024"]
        46649132
        
        >>> invalido = get_localidade_by_uf("XX")
        >>> invalido is None
        True
    """
    uf_upper = uf.upper().strip()
    
    for localidade in LOCALIDADES:
        if localidade["uf"] == uf_upper:
            return localidade.copy()
    
    return None


def get_localidades_by_regiao(regiao: str) -> List[dict]:
    """
    Busca todas as localidades de uma região.
    
    Args:
        regiao: Nome da região ("Norte", "Nordeste", "Centro-Oeste", "Sudeste", "Sul")
        
    Returns:
        Lista de dicionários com dados das localidades
        
    Examples:
        >>> sul = get_localidades_by_regiao("Sul")
        >>> len(sul)
        3
        >>> sorted([loc["uf"] for loc in sul])
        ['PR', 'RS', 'SC']
        
        >>> nordeste = get_localidades_by_regiao("Nordeste")
        >>> len(nordeste)
        9
        
        >>> invalida = get_localidades_by_regiao("Atlantis")
        >>> invalida
        []
    """
    regiao_capitalized = regiao.strip().title()
    
    resultado = [
        localidade.copy()
        for localidade in LOCALIDADES
        if localidade["regiao"] == regiao_capitalized
    ]
    
    return resultado


def get_all_ufs() -> List[str]:
    """
    Retorna lista com todas as siglas de UF.
    
    Returns:
        Lista ordenada com as 27 siglas de UF
        
    Examples:
        >>> ufs = get_all_ufs()
        >>> len(ufs)
        27
        >>> ufs[0]
        'AC'
        >>> "SP" in ufs
        True
    """
    return sorted([loc["uf"] for loc in LOCALIDADES])


def get_all_regioes() -> List[str]:
    """
    Retorna lista com todas as regiões do Brasil.
    
    Returns:
        Lista com as 5 regiões brasileiras
        
    Examples:
        >>> regioes = get_all_regioes()
        >>> len(regioes)
        5
        >>> "Sudeste" in regioes
        True
    """
    regioes = list(set(loc["regiao"] for loc in LOCALIDADES))
    return sorted(regioes)


def to_dataframe() -> pd.DataFrame:
    """
    Converte LOCALIDADES para DataFrame pandas.
    
    Returns:
        DataFrame com todas as localidades
        
    Examples:
        >>> df = to_dataframe()
        >>> df.shape
        (27, 11)
        >>> df.columns.tolist()
        ['sk_localidade', 'uf', 'nome_uf', 'regiao', 'sigla_regiao', 
         'capital', 'populacao_2024', 'area_km2', 'is_ativo', 
         'data_criacao', 'codigo_ibge']
        >>> df[df["uf"] == "SP"]["populacao_2024"].values[0]
        46649132
        >>> df["regiao"].value_counts()["Nordeste"]
        9
    """
    df = pd.DataFrame(LOCALIDADES)
    
    # Garantir ordem das colunas
    colunas_ordenadas = [
        "sk_localidade",
        "uf",
        "nome_uf",
        "regiao",
        "sigla_regiao",
        "capital",
        "populacao_2024",
        "area_km2",
        "is_ativo",
        "data_criacao",
        "codigo_ibge"
    ]
    
    return df[colunas_ordenadas]


def get_statistics() -> dict:
    """
    Retorna estatísticas sobre as localidades.
    
    Returns:
        Dicionário com estatísticas gerais
        
    Examples:
        >>> stats = get_statistics()
        >>> stats["total_estados"]
        27
        >>> stats["total_populacao"] > 200000000
        True
        >>> stats["estados_por_regiao"]["Nordeste"]
        9
    """
    df = to_dataframe()
    
    return {
        "total_estados": len(df),
        "total_populacao": int(df["populacao_2024"].sum()),
        "area_total_km2": float(df["area_km2"].sum()),
        "populacao_media": int(df["populacao_2024"].mean()),
        "area_media_km2": float(df["area_km2"].mean()),
        "estados_por_regiao": df["regiao"].value_counts().to_dict(),
        "maior_populacao": {
            "uf": df.loc[df["populacao_2024"].idxmax(), "uf"],
            "nome": df.loc[df["populacao_2024"].idxmax(), "nome_uf"],
            "populacao": int(df["populacao_2024"].max())
        },
        "menor_populacao": {
            "uf": df.loc[df["populacao_2024"].idxmin(), "uf"],
            "nome": df.loc[df["populacao_2024"].idxmin(), "nome_uf"],
            "populacao": int(df["populacao_2024"].min())
        },
        "maior_area": {
            "uf": df.loc[df["area_km2"].idxmax(), "uf"],
            "nome": df.loc[df["area_km2"].idxmax(), "nome_uf"],
            "area_km2": float(df["area_km2"].max())
        },
        "menor_area": {
            "uf": df.loc[df["area_km2"].idxmin(), "uf"],
            "nome": df.loc[df["area_km2"].idxmin(), "nome_uf"],
            "area_km2": float(df["area_km2"].min())
        }
    }


if __name__ == "__main__":
    # Demonstração de uso
    print("="*80)
    print("  📍 DIMENSÃO DE LOCALIDADES - ESTADOS BRASILEIROS")
    print("="*80 + "\n")
    
    # Buscar estado específico
    print("🔍 Exemplo 1: Buscar estado por UF")
    sp = get_localidade_by_uf("SP")
    if sp:
        print(f"   UF: {sp['uf']}")
        print(f"   Nome: {sp['nome_uf']}")
        print(f"   Região: {sp['regiao']}")
        print(f"   Capital: {sp['capital']}")
        print(f"   População: {sp['populacao_2024']:,}")
        print(f"   Área: {sp['area_km2']:,.1f} km²")
    print()
    
    # Buscar por região
    print("🔍 Exemplo 2: Buscar estados por região")
    sul = get_localidades_by_regiao("Sul")
    print(f"   Região Sul: {len(sul)} estados")
    for estado in sul:
        print(f"   - {estado['uf']}: {estado['nome_uf']}")
    print()
    
    # Converter para DataFrame
    print("📊 Exemplo 3: Converter para DataFrame")
    df = to_dataframe()
    print(f"   Shape: {df.shape}")
    print(f"   Colunas: {len(df.columns)}")
    print()
    
    # Estatísticas
    print("📈 Estatísticas Gerais:")
    stats = get_statistics()
    print(f"   Total de estados: {stats['total_estados']}")
    print(f"   População total: {stats['total_populacao']:,}")
    print(f"   Área total: {stats['area_total_km2']:,.1f} km²")
    print(f"   Estados por região:")
    for regiao, count in sorted(stats['estados_por_regiao'].items()):
        print(f"      - {regiao}: {count}")
    print()
    
    print("✅ Módulo pronto para uso!")
