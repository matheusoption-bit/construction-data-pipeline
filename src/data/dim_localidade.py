"""
Dimensão de Localidades (Estados Brasileiros).

27 estados + informações regionais e Sinduscons responsáveis.
"""

LOCALIDADES = [
    # REGIÃO NORTE
    {
        "sk_localidade": 1,
        "uf": "AC",
        "nome_estado": "Acre",
        "regiao": "Norte",
        "capital": "Rio Branco",
        "sinduscon": "SINDUSCON-AC",
        "codigo_ibge": 12,
        "is_ativo": True
    },
    {
        "sk_localidade": 2,
        "uf": "AM",
        "nome_estado": "Amazonas",
        "regiao": "Norte",
        "capital": "Manaus",
        "sinduscon": "SINDUSCON-NORTE",
        "codigo_ibge": 13,
        "is_ativo": True
    },
    {
        "sk_localidade": 3,
        "uf": "AP",
        "nome_estado": "Amapá",
        "regiao": "Norte",
        "capital": "Macapá",
        "sinduscon": "SINDUSCON-AP",
        "codigo_ibge": 16,
        "is_ativo": True
    },
    {
        "sk_localidade": 4,
        "uf": "PA",
        "nome_estado": "Pará",
        "regiao": "Norte",
        "capital": "Belém",
        "sinduscon": "SINDUSCON-PA",
        "codigo_ibge": 15,
        "is_ativo": True
    },
    {
        "sk_localidade": 5,
        "uf": "RO",
        "nome_estado": "Rondônia",
        "regiao": "Norte",
        "capital": "Porto Velho",
        "sinduscon": "SINDUSCON-RO",
        "codigo_ibge": 11,
        "is_ativo": True
    },
    {
        "sk_localidade": 6,
        "uf": "RR",
        "nome_estado": "Roraima",
        "regiao": "Norte",
        "capital": "Boa Vista",
        "sinduscon": "SINDUSCON-RR",
        "codigo_ibge": 14,
        "is_ativo": True
    },
    {
        "sk_localidade": 7,
        "uf": "TO",
        "nome_estado": "Tocantins",
        "regiao": "Norte",
        "capital": "Palmas",
        "sinduscon": "SINDUSCON-TO",
        "codigo_ibge": 17,
        "is_ativo": True
    },
    
    # REGIÃO NORDESTE
    {
        "sk_localidade": 8,
        "uf": "AL",
        "nome_estado": "Alagoas",
        "regiao": "Nordeste",
        "capital": "Maceió",
        "sinduscon": "SINDUSCON-AL",
        "codigo_ibge": 27,
        "is_ativo": True
    },
    {
        "sk_localidade": 9,
        "uf": "BA",
        "nome_estado": "Bahia",
        "regiao": "Nordeste",
        "capital": "Salvador",
        "sinduscon": "SINDUSCON-BA",
        "codigo_ibge": 29,
        "is_ativo": True
    },
    {
        "sk_localidade": 10,
        "uf": "CE",
        "nome_estado": "Ceará",
        "regiao": "Nordeste",
        "capital": "Fortaleza",
        "sinduscon": "SINDUSCON-CE",
        "codigo_ibge": 23,
        "is_ativo": True
    },
    {
        "sk_localidade": 11,
        "uf": "MA",
        "nome_estado": "Maranhão",
        "regiao": "Nordeste",
        "capital": "São Luís",
        "sinduscon": "SINDUSCON-MA",
        "codigo_ibge": 21,
        "is_ativo": True
    },
    {
        "sk_localidade": 12,
        "uf": "PB",
        "nome_estado": "Paraíba",
        "regiao": "Nordeste",
        "capital": "João Pessoa",
        "sinduscon": "SINDUSCON-PB",
        "codigo_ibge": 25,
        "is_ativo": True
    },
    {
        "sk_localidade": 13,
        "uf": "PE",
        "nome_estado": "Pernambuco",
        "regiao": "Nordeste",
        "capital": "Recife",
        "sinduscon": "SINDUSCON-PE",
        "codigo_ibge": 26,
        "is_ativo": True
    },
    {
        "sk_localidade": 14,
        "uf": "PI",
        "nome_estado": "Piauí",
        "regiao": "Nordeste",
        "capital": "Teresina",
        "sinduscon": "SINDUSCON-PI",
        "codigo_ibge": 22,
        "is_ativo": True
    },
    {
        "sk_localidade": 15,
        "uf": "RN",
        "nome_estado": "Rio Grande do Norte",
        "regiao": "Nordeste",
        "capital": "Natal",
        "sinduscon": "SINDUSCON-RN",
        "codigo_ibge": 24,
        "is_ativo": True
    },
    {
        "sk_localidade": 16,
        "uf": "SE",
        "nome_estado": "Sergipe",
        "regiao": "Nordeste",
        "capital": "Aracaju",
        "sinduscon": "SINDUSCON-SE",
        "codigo_ibge": 28,
        "is_ativo": True
    },
    
    # REGIÃO CENTRO-OESTE
    {
        "sk_localidade": 17,
        "uf": "DF",
        "nome_estado": "Distrito Federal",
        "regiao": "Centro-Oeste",
        "capital": "Brasília",
        "sinduscon": "SINDUSCON-DF",
        "codigo_ibge": 53,
        "is_ativo": True
    },
    {
        "sk_localidade": 18,
        "uf": "GO",
        "nome_estado": "Goiás",
        "regiao": "Centro-Oeste",
        "capital": "Goiânia",
        "sinduscon": "SINDUSCON-GO",
        "codigo_ibge": 52,
        "is_ativo": True
    },
    {
        "sk_localidade": 19,
        "uf": "MT",
        "nome_estado": "Mato Grosso",
        "regiao": "Centro-Oeste",
        "capital": "Cuiabá",
        "sinduscon": "SINDUSCON-MT",
        "codigo_ibge": 51,
        "is_ativo": True
    },
    {
        "sk_localidade": 20,
        "uf": "MS",
        "nome_estado": "Mato Grosso do Sul",
        "regiao": "Centro-Oeste",
        "capital": "Campo Grande",
        "sinduscon": "SINDUSCON-MS",
        "codigo_ibge": 50,
        "is_ativo": True
    },
    
    # REGIÃO SUDESTE
    {
        "sk_localidade": 21,
        "uf": "ES",
        "nome_estado": "Espírito Santo",
        "regiao": "Sudeste",
        "capital": "Vitória",
        "sinduscon": "SINDUSCON-ES",
        "codigo_ibge": 32,
        "is_ativo": True
    },
    {
        "sk_localidade": 22,
        "uf": "MG",
        "nome_estado": "Minas Gerais",
        "regiao": "Sudeste",
        "capital": "Belo Horizonte",
        "sinduscon": "SINDUSCON-MG",
        "codigo_ibge": 31,
        "is_ativo": True
    },
    {
        "sk_localidade": 23,
        "uf": "RJ",
        "nome_estado": "Rio de Janeiro",
        "regiao": "Sudeste",
        "capital": "Rio de Janeiro",
        "sinduscon": "SINDUSCON-RJ",
        "codigo_ibge": 33,
        "is_ativo": True
    },
    {
        "sk_localidade": 24,
        "uf": "SP",
        "nome_estado": "São Paulo",
        "regiao": "Sudeste",
        "capital": "São Paulo",
        "sinduscon": "SINDUSCON-SP",
        "codigo_ibge": 35,
        "is_ativo": True
    },
    
    # REGIÃO SUL
    {
        "sk_localidade": 25,
        "uf": "PR",
        "nome_estado": "Paraná",
        "regiao": "Sul",
        "capital": "Curitiba",
        "sinduscon": "SINDUSCON-PR",
        "codigo_ibge": 41,
        "is_ativo": True
    },
    {
        "sk_localidade": 26,
        "uf": "RS",
        "nome_estado": "Rio Grande do Sul",
        "regiao": "Sul",
        "capital": "Porto Alegre",
        "sinduscon": "SINDUSCON-RS",
        "codigo_ibge": 43,
        "is_ativo": True
    },
    {
        "sk_localidade": 27,
        "uf": "SC",
        "nome_estado": "Santa Catarina",
        "regiao": "Sul",
        "capital": "Florianópolis",
        "sinduscon": "SINDUSCON-SC",
        "codigo_ibge": 42,
        "is_ativo": True
    }
]
