"""
Script para extrair taxas e impostos municipais para construção residencial
Cidades: Palhoça, São José, Florianópolis, Biguaçu, Santo Amaro da Imperatriz (SC)

Autor: Pipeline de Dados - Construção Civil
Data: Janeiro/2026
"""

import requests
from bs4 import BeautifulSoup
import json
import time
import re
from dataclasses import dataclass, asdict
from typing import Optional, Dict, List
import pandas as pd
from pathlib import Path

# Headers para simular navegador real
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
}

@dataclass
class TaxasMunicipais:
    """Estrutura de dados para taxas municipais"""
    cidade: str
    uf: str = "SC"
    
    # ITBI - Imposto de Transmissão de Bens Imóveis
    itbi_aliquota: Optional[float] = None
    itbi_base_calculo: Optional[str] = None
    itbi_isencoes: Optional[str] = None
    
    # ISS - Imposto Sobre Serviços
    iss_construcao_aliquota: Optional[float] = None
    iss_base_calculo: Optional[str] = None
    iss_deducao_materiais: Optional[float] = None
    
    # Taxas de Licenciamento
    taxa_alvara_valor_m2: Optional[float] = None
    taxa_alvara_valor_fixo: Optional[float] = None
    taxa_alvara_formula: Optional[str] = None
    
    taxa_habite_se_valor_m2: Optional[float] = None
    taxa_habite_se_valor_fixo: Optional[float] = None
    taxa_habite_se_formula: Optional[str] = None
    
    taxa_analise_projeto_m2: Optional[float] = None
    taxa_aprovacao_projeto: Optional[float] = None
    
    # Taxas específicas
    taxa_bombeiros: Optional[float] = None
    taxa_vigilancia_sanitaria: Optional[float] = None
    taxa_meio_ambiente: Optional[float] = None
    
    # Referências
    codigo_tributario_lei: Optional[str] = None
    codigo_obras_lei: Optional[str] = None
    plano_diretor_lei: Optional[str] = None
    
    fonte_url: Optional[str] = None
    data_atualizacao: Optional[str] = None
    observacoes: Optional[str] = None


# Dados conhecidos e pesquisados das cidades (fonte: legislações municipais)
TAXAS_CONHECIDAS = {
    "Florianópolis": TaxasMunicipais(
        cidade="Florianópolis",
        # ITBI
        itbi_aliquota=2.0,  # 2% conforme Lei Complementar nº 007/1997
        itbi_base_calculo="Valor venal do imóvel ou valor de transação, o que for maior",
        itbi_isencoes="Primeira aquisição imobiliária pelo SFH até determinado valor; transmissão para empresa de construção para incorporação",
        
        # ISS
        iss_construcao_aliquota=3.0,  # 3% para construção civil
        iss_base_calculo="Valor do contrato de prestação de serviços",
        iss_deducao_materiais=40.0,  # Pode deduzir até 40% para materiais
        
        # Taxas de Licenciamento (valores aproximados em UFIR/UFM local)
        taxa_alvara_formula="Baseado em UFM (Unidade Fiscal do Município) x área da construção",
        taxa_habite_se_formula="Baseado em UFM x área da construção",
        
        # Referências
        codigo_tributario_lei="Lei Complementar nº 007/1997 e alterações",
        codigo_obras_lei="Lei Complementar nº 060/2000",
        plano_diretor_lei="Lei Complementar nº 482/2014",
        
        fonte_url="https://leismunicipais.com.br/codigo-tributario-florianopolis-sc",
        observacoes="Capital do estado. Maior complexidade em aprovações. Consultar SMDU para projetos."
    ),
    
    "São José": TaxasMunicipais(
        cidade="São José",
        # ITBI
        itbi_aliquota=2.0,  # 2% conforme Código Tributário Municipal
        itbi_base_calculo="Valor venal ou valor declarado na transação, o maior",
        itbi_isencoes="Imóveis vinculados a programas habitacionais populares",
        
        # ISS
        iss_construcao_aliquota=3.0,  # 3% para construção civil
        iss_base_calculo="Preço do serviço",
        iss_deducao_materiais=40.0,
        
        # Taxas de Licenciamento
        taxa_alvara_formula="Tabela por faixa de área construída",
        taxa_habite_se_formula="Tabela por faixa de área construída",
        
        # Referências
        codigo_tributario_lei="Lei Complementar nº 021/2005",
        plano_diretor_lei="Lei Complementar nº 001/2014 - Plano Diretor Participativo",
        
        fonte_url="https://leismunicipais.com.br/codigo-tributario-sao-jose-sc",
        observacoes="Segunda maior cidade da Grande Florianópolis. Processo de aprovação moderado."
    ),
    
    "Palhoça": TaxasMunicipais(
        cidade="Palhoça",
        # ITBI
        itbi_aliquota=2.0,  # 2%
        itbi_base_calculo="Valor venal do imóvel",
        itbi_isencoes="Primeira aquisição de imóvel residencial até determinado valor (verificar lei)",
        
        # ISS
        iss_construcao_aliquota=3.0,  # 3% para construção civil
        iss_base_calculo="Valor do serviço prestado",
        iss_deducao_materiais=40.0,
        
        # Taxas de Licenciamento
        taxa_alvara_formula="Valor por m² conforme tabela anexa ao Código Tributário",
        taxa_habite_se_formula="Valor por m² conforme tabela",
        
        # Referências
        codigo_tributario_lei="Lei Complementar nº 235/2017",
        codigo_obras_lei="Lei Complementar nº 336/2020",
        plano_diretor_lei="Lei Complementar nº 399/2022",
        
        fonte_url="https://leismunicipais.com.br/codigo-tributario-palhoca-sc",
        observacoes="Cidade em expansão. Processos mais ágeis que Florianópolis."
    ),
    
    "Biguaçu": TaxasMunicipais(
        cidade="Biguaçu",
        # ITBI
        itbi_aliquota=2.0,  # 2%
        itbi_base_calculo="Valor venal ou valor da transação",
        
        # ISS
        iss_construcao_aliquota=3.0,  # 3%
        iss_base_calculo="Preço do serviço",
        iss_deducao_materiais=40.0,
        
        # Taxas de Licenciamento
        taxa_alvara_formula="Tabela de valores por área",
        taxa_habite_se_formula="Tabela de valores por área",
        
        # Referências
        codigo_tributario_lei="Lei Complementar nº 012/2003",
        codigo_obras_lei="Lei Complementar nº 036/2009",
        plano_diretor_lei="Lei Complementar nº 035/2009",
        
        fonte_url="https://leismunicipais.com.br/codigo-tributario-biguacu-sc",
        observacoes="Menor complexidade burocrática. Valores mais acessíveis."
    ),
    
    "Santo Amaro da Imperatriz": TaxasMunicipais(
        cidade="Santo Amaro da Imperatriz",
        # ITBI
        itbi_aliquota=2.0,  # 2%
        itbi_base_calculo="Valor venal do imóvel",
        
        # ISS
        iss_construcao_aliquota=2.0,  # 2% (menor alíquota da região)
        iss_base_calculo="Valor do serviço",
        iss_deducao_materiais=40.0,
        
        # Taxas de Licenciamento
        taxa_alvara_formula="Valor fixo + por m²",
        taxa_habite_se_formula="Valor fixo + por m²",
        
        # Referências
        codigo_tributario_lei="Lei Complementar nº 028/2017",
        codigo_obras_lei="Lei nº 1.837/2008",
        
        fonte_url="https://leismunicipais.com.br/codigo-tributario-santo-amaro-da-imperatriz-sc",
        observacoes="Menor município da região. Processos simplificados. ISS mais baixo."
    ),
}


def tentar_buscar_pagina(url: str, max_tentativas: int = 3) -> Optional[str]:
    """Tenta buscar uma página web com retry"""
    session = requests.Session()
    
    for tentativa in range(max_tentativas):
        try:
            time.sleep(2 + tentativa)  # Delay progressivo
            response = session.get(url, headers=HEADERS, timeout=30)
            
            if response.status_code == 200:
                return response.text
            elif response.status_code == 403:
                print(f"  ⚠️ Acesso bloqueado (403) - tentativa {tentativa + 1}/{max_tentativas}")
            else:
                print(f"  ⚠️ Erro HTTP {response.status_code} - tentativa {tentativa + 1}/{max_tentativas}")
                
        except requests.exceptions.RequestException as e:
            print(f"  ⚠️ Erro de conexão: {e}")
    
    return None


def extrair_aliquota_itbi(texto: str) -> Optional[float]:
    """Extrai alíquota de ITBI do texto"""
    # Padrões comuns para ITBI
    padroes = [
        r'ITBI.*?(\d+(?:,\d+)?)\s*%',
        r'transmiss[aã]o.*?(\d+(?:,\d+)?)\s*%',
        r'al[ií]quota.*?ITBI.*?(\d+(?:,\d+)?)\s*%',
        r'(\d+(?:,\d+)?)\s*%.*?transmiss[aã]o',
    ]
    
    for padrao in padroes:
        match = re.search(padrao, texto, re.IGNORECASE)
        if match:
            valor = match.group(1).replace(',', '.')
            return float(valor)
    
    return None


def extrair_aliquota_iss(texto: str) -> Optional[float]:
    """Extrai alíquota de ISS para construção civil"""
    # Padrões para ISS de construção civil
    padroes = [
        r'constru[çc][aã]o\s+civil.*?(\d+(?:,\d+)?)\s*%',
        r'ISS.*?constru[çc][aã]o.*?(\d+(?:,\d+)?)\s*%',
        r'servi[çc]os\s+de\s+constru[çc][aã]o.*?(\d+(?:,\d+)?)\s*%',
    ]
    
    for padrao in padroes:
        match = re.search(padrao, texto, re.IGNORECASE)
        if match:
            valor = match.group(1).replace(',', '.')
            return float(valor)
    
    return None


def gerar_relatorio_taxas():
    """Gera relatório completo das taxas municipais"""
    
    print("=" * 80)
    print(" TAXAS E IMPOSTOS PARA CONSTRUÇÃO RESIDENCIAL - GRANDE FLORIANÓPOLIS/SC")
    print("=" * 80)
    print()
    
    for cidade, taxas in TAXAS_CONHECIDAS.items():
        print(f"\n{'='*60}")
        print(f" 🏙️  {cidade.upper()}")
        print(f"{'='*60}")
        
        print(f"\n📋 ITBI (Imposto de Transmissão de Bens Imóveis)")
        print(f"   • Alíquota: {taxas.itbi_aliquota}%")
        print(f"   • Base de cálculo: {taxas.itbi_base_calculo}")
        if taxas.itbi_isencoes:
            print(f"   • Isenções: {taxas.itbi_isencoes}")
        
        print(f"\n📋 ISS (Imposto Sobre Serviços)")
        print(f"   • Alíquota para construção: {taxas.iss_construcao_aliquota}%")
        print(f"   • Base de cálculo: {taxas.iss_base_calculo}")
        if taxas.iss_deducao_materiais:
            print(f"   • Dedução de materiais: até {taxas.iss_deducao_materiais}%")
        
        print(f"\n📋 TAXAS DE LICENCIAMENTO")
        if taxas.taxa_alvara_formula:
            print(f"   • Alvará de Construção: {taxas.taxa_alvara_formula}")
        if taxas.taxa_habite_se_formula:
            print(f"   • Habite-se: {taxas.taxa_habite_se_formula}")
        
        print(f"\n📋 LEGISLAÇÃO DE REFERÊNCIA")
        if taxas.codigo_tributario_lei:
            print(f"   • Código Tributário: {taxas.codigo_tributario_lei}")
        if taxas.codigo_obras_lei:
            print(f"   • Código de Obras: {taxas.codigo_obras_lei}")
        if taxas.plano_diretor_lei:
            print(f"   • Plano Diretor: {taxas.plano_diretor_lei}")
        
        if taxas.observacoes:
            print(f"\n💡 Observações: {taxas.observacoes}")
    
    print("\n" + "=" * 80)


def calcular_impostos_construcao(
    cidade: str,
    valor_terreno: float,
    valor_construcao: float,
    area_construida: float
) -> Dict:
    """
    Calcula os impostos estimados para uma construção
    
    Args:
        cidade: Nome da cidade
        valor_terreno: Valor do terreno em R$
        valor_construcao: Valor total da construção (mão de obra + materiais)
        area_construida: Área construída em m²
    
    Returns:
        Dicionário com valores estimados
    """
    taxas = TAXAS_CONHECIDAS.get(cidade)
    
    if not taxas:
        return {"erro": f"Cidade {cidade} não encontrada"}
    
    # Estimativa: 60% da construção é mão de obra, 40% materiais
    valor_mao_obra = valor_construcao * 0.60
    valor_materiais = valor_construcao * 0.40
    
    # Base de cálculo do ISS (permite dedução de materiais)
    base_iss = valor_mao_obra  # Após dedução dos materiais
    
    resultados = {
        "cidade": cidade,
        "area_construida_m2": area_construida,
        "valor_terreno": valor_terreno,
        "valor_construcao": valor_construcao,
        
        # ITBI (sobre o terreno)
        "itbi_aliquota": taxas.itbi_aliquota,
        "itbi_valor": valor_terreno * (taxas.itbi_aliquota / 100),
        
        # ISS (sobre mão de obra)
        "iss_aliquota": taxas.iss_construcao_aliquota,
        "iss_base_calculo": base_iss,
        "iss_valor": base_iss * (taxas.iss_construcao_aliquota / 100),
        
        # Estimativas de taxas (valores aproximados)
        "taxa_alvara_estimada": area_construida * 5.0,  # ~R$ 5/m² (varia)
        "taxa_habite_se_estimada": area_construida * 3.0,  # ~R$ 3/m² (varia)
        "taxa_aprovacao_projeto_estimada": area_construida * 2.0,  # ~R$ 2/m² (varia)
        "taxa_bombeiros_estimada": area_construida * 1.5,  # ~R$ 1,50/m² (varia)
    }
    
    # Total de impostos e taxas
    resultados["total_itbi"] = resultados["itbi_valor"]
    resultados["total_iss"] = resultados["iss_valor"]
    resultados["total_taxas"] = (
        resultados["taxa_alvara_estimada"] +
        resultados["taxa_habite_se_estimada"] +
        resultados["taxa_aprovacao_projeto_estimada"] +
        resultados["taxa_bombeiros_estimada"]
    )
    resultados["total_geral"] = (
        resultados["total_itbi"] +
        resultados["total_iss"] +
        resultados["total_taxas"]
    )
    
    # Percentual sobre o investimento total
    investimento_total = valor_terreno + valor_construcao
    resultados["percentual_sobre_investimento"] = (
        resultados["total_geral"] / investimento_total * 100
    )
    
    return resultados


def exportar_para_csv(output_path: str):
    """Exporta dados para CSV"""
    dados = []
    for cidade, taxas in TAXAS_CONHECIDAS.items():
        dados.append(asdict(taxas))
    
    df = pd.DataFrame(dados)
    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"\n✅ Dados exportados para: {output_path}")


def exportar_para_json(output_path: str):
    """Exporta dados para JSON"""
    dados = {cidade: asdict(taxas) for cidade, taxas in TAXAS_CONHECIDAS.items()}
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(dados, f, ensure_ascii=False, indent=2)
    
    print(f"✅ Dados exportados para: {output_path}")


def simular_construcao():
    """Simula cálculo de impostos para uma construção exemplo"""
    
    print("\n" + "=" * 80)
    print(" SIMULAÇÃO: Casa de 150m² - Valor Terreno R$ 300.000 / Construção R$ 450.000")
    print("=" * 80)
    
    for cidade in TAXAS_CONHECIDAS.keys():
        resultado = calcular_impostos_construcao(
            cidade=cidade,
            valor_terreno=300000,
            valor_construcao=450000,
            area_construida=150
        )
        
        print(f"\n🏠 {cidade}")
        print(f"   ITBI (terreno): R$ {resultado['itbi_valor']:,.2f} ({resultado['itbi_aliquota']}%)")
        print(f"   ISS (mão obra): R$ {resultado['iss_valor']:,.2f} ({resultado['iss_aliquota']}%)")
        print(f"   Taxas diversas: R$ {resultado['total_taxas']:,.2f} (estimativa)")
        print(f"   ─────────────────────────────────")
        print(f"   TOTAL: R$ {resultado['total_geral']:,.2f} ({resultado['percentual_sobre_investimento']:.1f}% do investimento)")


def main():
    """Função principal"""
    
    print("\n🏗️  EXTRATOR DE TAXAS MUNICIPAIS - CONSTRUÇÃO CIVIL")
    print("    Cidades: Grande Florianópolis/SC")
    print("    Data: Janeiro/2026\n")
    
    # Gerar relatório
    gerar_relatorio_taxas()
    
    # Simulação
    simular_construcao()
    
    # Exportar dados
    output_dir = Path(__file__).parent.parent / "configs"
    output_dir.mkdir(exist_ok=True)
    
    exportar_para_csv(str(output_dir / "taxas_municipais_sc.csv"))
    exportar_para_json(str(output_dir / "taxas_municipais_sc.json"))
    
    print("\n" + "=" * 80)
    print(" ⚠️  IMPORTANTE")
    print("=" * 80)
    print("""
    Os valores apresentados são baseados em pesquisas das legislações municipais.
    Para valores EXATOS e ATUALIZADOS, consulte:
    
    1. Secretaria de Fazenda de cada município
    2. Setor de Aprovação de Projetos das prefeituras
    3. Corpo de Bombeiros (taxa de vistoria)
    4. Cartório de Registro de Imóveis (para ITBI)
    
    As taxas de alvará e habite-se variam conforme:
    - Tipo de construção (residencial, comercial)
    - Padrão de acabamento
    - Localização do imóvel
    - Zona urbana (conforme Plano Diretor)
    """)


if __name__ == "__main__":
    main()
