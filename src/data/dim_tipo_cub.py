"""
Dimensão de Tipos de CUB segundo NBR 12721:2006.

Define todos os tipos de projetos-padrão para cálculo do CUB.
"""

TIPOS_CUB = [
    # RESIDENCIAIS UNIFAMILIARES
    {
        "sk_tipo": 1,
        "codigo_tipo": "R1-N",
        "nome_tipo": "Residência Unifamiliar - Padrão Normal",
        "categoria": "Residencial",
        "subcategoria": "Unifamiliar",
        "padrao_acabamento": "Normal",
        "area_referencia_m2": 136.56,
        "descricao": "Casa térrea ou sobrado padrão normal",
        "fonte": "NBR 12721:2006",
        "is_ativo": True
    },
    {
        "sk_tipo": 2,
        "codigo_tipo": "R1-B",
        "nome_tipo": "Residência Unifamiliar - Padrão Baixo",
        "categoria": "Residencial",
        "subcategoria": "Unifamiliar",
        "padrao_acabamento": "Baixo",
        "area_referencia_m2": 136.56,
        "descricao": "Casa térrea ou sobrado padrão baixo",
        "fonte": "NBR 12721:2006",
        "is_ativo": True
    },
    {
        "sk_tipo": 3,
        "codigo_tipo": "R1-A",
        "nome_tipo": "Residência Unifamiliar - Padrão Alto",
        "categoria": "Residencial",
        "subcategoria": "Unifamiliar",
        "padrao_acabamento": "Alto",
        "area_referencia_m2": 136.56,
        "descricao": "Casa térrea ou sobrado padrão alto",
        "fonte": "NBR 12721:2006",
        "is_ativo": True
    },
    
    # RESIDENCIAIS MULTIFAMILIARES - 8 PAVIMENTOS
    {
        "sk_tipo": 4,
        "codigo_tipo": "R8-N",
        "nome_tipo": "Residencial Multifamiliar 8 Pavimentos - Normal",
        "categoria": "Residencial",
        "subcategoria": "Multifamiliar",
        "padrao_acabamento": "Normal",
        "area_referencia_m2": 1291.56,
        "descricao": "Edifício residencial de 8 pavimentos padrão normal",
        "fonte": "NBR 12721:2006",
        "is_ativo": True
    },
    {
        "sk_tipo": 5,
        "codigo_tipo": "R8-B",
        "nome_tipo": "Residencial Multifamiliar 8 Pavimentos - Baixo",
        "categoria": "Residencial",
        "subcategoria": "Multifamiliar",
        "padrao_acabamento": "Baixo",
        "area_referencia_m2": 1291.56,
        "descricao": "Edifício residencial de 8 pavimentos padrão baixo",
        "fonte": "NBR 12721:2006",
        "is_ativo": True
    },
    {
        "sk_tipo": 6,
        "codigo_tipo": "R8-A",
        "nome_tipo": "Residencial Multifamiliar 8 Pavimentos - Alto",
        "categoria": "Residencial",
        "subcategoria": "Multifamiliar",
        "padrao_acabamento": "Alto",
        "area_referencia_m2": 1291.56,
        "descricao": "Edifício residencial de 8 pavimentos padrão alto",
        "fonte": "NBR 12721:2006",
        "is_ativo": True
    },
    
    # RESIDENCIAIS MULTIFAMILIARES - 16 PAVIMENTOS
    {
        "sk_tipo": 7,
        "codigo_tipo": "R16-N",
        "nome_tipo": "Residencial Multifamiliar 16 Pavimentos - Normal",
        "categoria": "Residencial",
        "subcategoria": "Multifamiliar",
        "padrao_acabamento": "Normal",
        "area_referencia_m2": 2464.80,
        "descricao": "Edifício residencial de 16 pavimentos padrão normal",
        "fonte": "NBR 12721:2006",
        "is_ativo": True
    },
    {
        "sk_tipo": 8,
        "codigo_tipo": "R16-B",
        "nome_tipo": "Residencial Multifamiliar 16 Pavimentos - Baixo",
        "categoria": "Residencial",
        "subcategoria": "Multifamiliar",
        "padrao_acabamento": "Baixo",
        "area_referencia_m2": 2464.80,
        "descricao": "Edifício residencial de 16 pavimentos padrão baixo",
        "fonte": "NBR 12721:2006",
        "is_ativo": True
    },
    {
        "sk_tipo": 9,
        "codigo_tipo": "R16-A",
        "nome_tipo": "Residencial Multifamiliar 16 Pavimentos - Alto",
        "categoria": "Residencial",
        "subcategoria": "Multifamiliar",
        "padrao_acabamento": "Alto",
        "area_referencia_m2": 2464.80,
        "descricao": "Edifício residencial de 16 pavimentos padrão alto",
        "fonte": "NBR 12721:2006",
        "is_ativo": True
    },
    
    # PROJETOS POPULARES
    {
        "sk_tipo": 10,
        "codigo_tipo": "PP4-N",
        "nome_tipo": "Projeto Popular 4 Pavimentos - Normal",
        "categoria": "Residencial",
        "subcategoria": "Popular",
        "padrao_acabamento": "Normal",
        "area_referencia_m2": 436.00,
        "descricao": "Edifício residencial popular de 4 pavimentos",
        "fonte": "NBR 12721:2006",
        "is_ativo": True
    },
    {
        "sk_tipo": 11,
        "codigo_tipo": "PIS",
        "nome_tipo": "Projeto de Interesse Social",
        "categoria": "Residencial",
        "subcategoria": "Social",
        "padrao_acabamento": "Baixo",
        "area_referencia_m2": 41.00,
        "descricao": "Habitação de interesse social",
        "fonte": "NBR 12721:2006",
        "is_ativo": True
    },
    {
        "sk_tipo": 12,
        "codigo_tipo": "RP1Q",
        "nome_tipo": "Residência Popular 1 Quarto",
        "categoria": "Residencial",
        "subcategoria": "Popular",
        "padrao_acabamento": "Baixo",
        "area_referencia_m2": 50.00,
        "descricao": "Residência popular de 1 quarto",
        "fonte": "NBR 12721:2006",
        "is_ativo": True
    },
    
    # COMERCIAIS SALAS/LOJAS
    {
        "sk_tipo": 13,
        "codigo_tipo": "CSL8-N",
        "nome_tipo": "Comercial Salas/Lojas 8 Pavimentos - Normal",
        "categoria": "Comercial",
        "subcategoria": "Salas e Lojas",
        "padrao_acabamento": "Normal",
        "area_referencia_m2": 1200.00,
        "descricao": "Edifício comercial de salas/lojas 8 pavimentos",
        "fonte": "NBR 12721:2006",
        "is_ativo": True
    },
    {
        "sk_tipo": 14,
        "codigo_tipo": "CSL16-N",
        "nome_tipo": "Comercial Salas/Lojas 16 Pavimentos - Normal",
        "categoria": "Comercial",
        "subcategoria": "Salas e Lojas",
        "padrao_acabamento": "Normal",
        "area_referencia_m2": 2400.00,
        "descricao": "Edifício comercial de salas/lojas 16 pavimentos",
        "fonte": "NBR 12721:2006",
        "is_ativo": True
    },
    
    # COMERCIAIS ANDAR LIVRE
    {
        "sk_tipo": 15,
        "codigo_tipo": "CAL8-N",
        "nome_tipo": "Comercial Andar Livre 8 Pavimentos - Normal",
        "categoria": "Comercial",
        "subcategoria": "Andar Livre",
        "padrao_acabamento": "Normal",
        "area_referencia_m2": 1400.00,
        "descricao": "Edifício comercial de andar livre 8 pavimentos",
        "fonte": "NBR 12721:2006",
        "is_ativo": True
    },
    
    # GALPÕES INDUSTRIAIS
    {
        "sk_tipo": 16,
        "codigo_tipo": "GI",
        "nome_tipo": "Galpão Industrial",
        "categoria": "Industrial",
        "subcategoria": "Galpão",
        "padrao_acabamento": "Normal",
        "area_referencia_m2": 1000.00,
        "descricao": "Galpão industrial padrão",
        "fonte": "NBR 12721:2006",
        "is_ativo": True
    },
    
    # COMERCIAIS ADICIONAIS (expandindo além dos 10 principais)
    {
        "sk_tipo": 17,
        "codigo_tipo": "CSL8-B",
        "nome_tipo": "Comercial Salas/Lojas 8 Pavimentos - Baixo",
        "categoria": "Comercial",
        "subcategoria": "Salas e Lojas",
        "padrao_acabamento": "Baixo",
        "area_referencia_m2": 1200.00,
        "descricao": "Edifício comercial de salas/lojas 8 pavimentos padrão baixo",
        "fonte": "NBR 12721:2006",
        "is_ativo": True
    },
    {
        "sk_tipo": 18,
        "codigo_tipo": "CSL8-A",
        "nome_tipo": "Comercial Salas/Lojas 8 Pavimentos - Alto",
        "categoria": "Comercial",
        "subcategoria": "Salas e Lojas",
        "padrao_acabamento": "Alto",
        "area_referencia_m2": 1200.00,
        "descricao": "Edifício comercial de salas/lojas 8 pavimentos padrão alto",
        "fonte": "NBR 12721:2006",
        "is_ativo": True
    },
    {
        "sk_tipo": 19,
        "codigo_tipo": "CAL8-B",
        "nome_tipo": "Comercial Andar Livre 8 Pavimentos - Baixo",
        "categoria": "Comercial",
        "subcategoria": "Andar Livre",
        "padrao_acabamento": "Baixo",
        "area_referencia_m2": 1400.00,
        "descricao": "Edifício comercial de andar livre 8 pavimentos padrão baixo",
        "fonte": "NBR 12721:2006",
        "is_ativo": True
    },
    {
        "sk_tipo": 20,
        "codigo_tipo": "CAL8-A",
        "nome_tipo": "Comercial Andar Livre 8 Pavimentos - Alto",
        "categoria": "Comercial",
        "subcategoria": "Andar Livre",
        "padrao_acabamento": "Alto",
        "area_referencia_m2": 1400.00,
        "descricao": "Edifício comercial de andar livre 8 pavimentos padrão alto",
        "fonte": "NBR 12721:2006",
        "is_ativo": True
    }
]
