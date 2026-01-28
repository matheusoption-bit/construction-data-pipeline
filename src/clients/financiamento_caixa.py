"""
Cliente para dados de Financiamento Imobiliário da Caixa Econômica Federal.

FONTE OFICIAL: Caixa Econômica Federal
- Portal: https://www.caixa.gov.br/voce/habitacao/
- Simuladores: https://www8.caixa.gov.br/siopiinternet-web/simulaOperacaoInternet.do

PROGRAMAS DE FINANCIAMENTO:
- Minha Casa Minha Vida (MCMV)
- SBPE (Sistema Brasileiro de Poupança e Empréstimo)
- FGTS (Fundo de Garantia)
- Pró-Cotista
- Casa Verde e Amarela

PARÂMETROS PRINCIPAIS:
- Taxa de juros efetiva anual
- Prazo máximo (meses)
- Percentual de financiamento
- Renda mínima/máxima por faixa

Autor: Pipeline de Dados
Data: 2026-01-28
"""

import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional
import structlog

logger = structlog.get_logger(__name__)


class FinanciamentoCaixaClient:
    """
    Cliente para parâmetros de financiamento habitacional da Caixa.
    
    Fornece dados de taxas, prazos e condições dos programas
    de financiamento imobiliário da Caixa Econômica Federal.
    """
    
    # Parâmetros oficiais de financiamento (Atualizados em Jan/2026)
    # Fonte: https://www.caixa.gov.br/voce/habitacao/
    
    PROGRAMAS_FINANCIAMENTO = {
        'MCMV_FAIXA_1': {
            'nome': 'Minha Casa Minha Vida - Faixa 1',
            'taxa_juros_aa': 4.00,  # % ao ano
            'prazo_max_meses': 420,
            'percentual_financ_max': 100.0,
            'renda_max': 2640.00,  # R$ (2 salários mínimos)
            'valor_imovel_max': 170000.00,
            'entrada_min_pct': 0.0,
            'sistema_amortizacao': 'SAC',
            'tipo': 'SUBSIDIADO'
        },
        'MCMV_FAIXA_2': {
            'nome': 'Minha Casa Minha Vida - Faixa 2',
            'taxa_juros_aa': 4.75,
            'prazo_max_meses': 420,
            'percentual_financ_max': 90.0,
            'renda_max': 4400.00,  # Até R$ 4.400
            'valor_imovel_max': 264000.00,
            'entrada_min_pct': 10.0,
            'sistema_amortizacao': 'SAC',
            'tipo': 'SUBSIDIADO'
        },
        'MCMV_FAIXA_3': {
            'nome': 'Minha Casa Minha Vida - Faixa 3',
            'taxa_juros_aa': 7.66,
            'prazo_max_meses': 420,
            'percentual_financ_max': 80.0,
            'renda_max': 8000.00,
            'valor_imovel_max': 350000.00,
            'entrada_min_pct': 20.0,
            'sistema_amortizacao': 'SAC',
            'tipo': 'SUBSIDIADO'
        },
        'SBPE_TAXA_REFERENCIAL': {
            'nome': 'SBPE - Taxa Referencial (TR)',
            'taxa_juros_aa': 10.49,  # TR + 10.49%
            'prazo_max_meses': 420,
            'percentual_financ_max': 80.0,
            'renda_max': None,  # Sem limite
            'valor_imovel_max': None,  # Sem limite
            'entrada_min_pct': 20.0,
            'sistema_amortizacao': 'SAC',
            'tipo': 'MERCADO'
        },
        'SBPE_POUPANCA': {
            'nome': 'SBPE - Poupança CAIXA',
            'taxa_juros_aa': 9.99,  # Poupança + spread
            'prazo_max_meses': 420,
            'percentual_financ_max': 80.0,
            'renda_max': None,
            'valor_imovel_max': None,
            'entrada_min_pct': 20.0,
            'sistema_amortizacao': 'SAC',
            'tipo': 'MERCADO'
        },
        'SBPE_IPCA': {
            'nome': 'SBPE - IPCA',
            'taxa_juros_aa': 4.95,  # IPCA + 4.95%
            'prazo_max_meses': 360,
            'percentual_financ_max': 80.0,
            'renda_max': None,
            'valor_imovel_max': None,
            'entrada_min_pct': 20.0,
            'sistema_amortizacao': 'SAC',
            'tipo': 'MERCADO'
        },
        'SBPE_TAXA_FIXA': {
            'nome': 'SBPE - Taxa Fixa',
            'taxa_juros_aa': 11.49,
            'prazo_max_meses': 360,
            'percentual_financ_max': 80.0,
            'renda_max': None,
            'valor_imovel_max': None,
            'entrada_min_pct': 20.0,
            'sistema_amortizacao': 'SAC',
            'tipo': 'MERCADO'
        },
        'PRO_COTISTA_FGTS': {
            'nome': 'Pró-Cotista FGTS',
            'taxa_juros_aa': 8.66,
            'prazo_max_meses': 420,
            'percentual_financ_max': 80.0,
            'renda_max': None,
            'valor_imovel_max': 1500000.00,
            'entrada_min_pct': 20.0,
            'sistema_amortizacao': 'SAC',
            'tipo': 'FGTS',
            'requisito': 'Mínimo 3 anos de FGTS'
        },
        'CONSTRUCAO_INDIVIDUAL': {
            'nome': 'Construção em Terreno Próprio',
            'taxa_juros_aa': 10.99,
            'prazo_max_meses': 420,
            'percentual_financ_max': 70.0,
            'renda_max': None,
            'valor_imovel_max': None,
            'entrada_min_pct': 30.0,
            'sistema_amortizacao': 'SAC',
            'tipo': 'MERCADO'
        }
    }
    
    def get_all_parameters(self) -> pd.DataFrame:
        """
        Retorna todos os parâmetros de financiamento em formato de tabela.
        
        Returns:
            DataFrame com parâmetros no schema fin_params_caixa
        """
        records = []
        
        for codigo, params in self.PROGRAMAS_FINANCIAMENTO.items():
            records.append({
                'id_parametro': len(records) + 1,
                'codigo_programa': codigo,
                'tipo_financiamento': params['nome'],
                'taxa_juros_aa': params['taxa_juros_aa'],
                'prazo_max_meses': params['prazo_max_meses'],
                'percentual_financ_max': params['percentual_financ_max'],
                'entrada_min_pct': params['entrada_min_pct'],
                'renda_max': params.get('renda_max'),
                'valor_imovel_max': params.get('valor_imovel_max'),
                'sistema_amortizacao': params['sistema_amortizacao'],
                'tipo': params['tipo'],
                'data_vigencia': datetime.now().strftime('%Y-%m-%d'),
                'fonte': 'CAIXA'
            })
        
        return pd.DataFrame(records)
    
    def get_parameters_by_type(self, tipo: str) -> pd.DataFrame:
        """
        Retorna parâmetros filtrados por tipo.
        
        Args:
            tipo: SUBSIDIADO, MERCADO ou FGTS
        """
        df = self.get_all_parameters()
        return df[df['tipo'] == tipo]
    
    def get_mcmv_parameters(self) -> pd.DataFrame:
        """Retorna apenas parâmetros do Minha Casa Minha Vida."""
        df = self.get_all_parameters()
        return df[df['codigo_programa'].str.startswith('MCMV')]
    
    def simulate_financing(
        self,
        valor_imovel: float,
        renda_familiar: float,
        programa: str = None
    ) -> Dict:
        """
        Simula financiamento básico.
        
        Args:
            valor_imovel: Valor do imóvel em R$
            renda_familiar: Renda familiar bruta em R$
            programa: Código do programa (opcional, seleciona melhor opção)
            
        Returns:
            Dict com detalhes da simulação
        """
        # Encontra programa adequado
        if programa is None:
            programa = self._select_best_program(valor_imovel, renda_familiar)
        
        params = self.PROGRAMAS_FINANCIAMENTO.get(programa)
        if not params:
            return {'erro': f'Programa {programa} não encontrado'}
        
        # Calcula valores
        percentual_financ = params['percentual_financ_max']
        valor_financiado = valor_imovel * (percentual_financ / 100)
        entrada = valor_imovel - valor_financiado
        
        taxa_mensal = params['taxa_juros_aa'] / 100 / 12
        prazo = params['prazo_max_meses']
        
        # Parcela SAC (primeira parcela)
        amortizacao = valor_financiado / prazo
        juros = valor_financiado * taxa_mensal
        primeira_parcela = amortizacao + juros
        
        # Parcela máxima (30% da renda)
        parcela_max_renda = renda_familiar * 0.30
        
        return {
            'programa': params['nome'],
            'valor_imovel': valor_imovel,
            'valor_financiado': round(valor_financiado, 2),
            'entrada_necessaria': round(entrada, 2),
            'taxa_juros_aa': params['taxa_juros_aa'],
            'prazo_meses': prazo,
            'primeira_parcela': round(primeira_parcela, 2),
            'ultima_parcela': round(amortizacao + (amortizacao * taxa_mensal), 2),
            'parcela_max_renda': round(parcela_max_renda, 2),
            'aprovado': primeira_parcela <= parcela_max_renda
        }
    
    def _select_best_program(self, valor_imovel: float, renda: float) -> str:
        """Seleciona melhor programa baseado em valor e renda."""
        # MCMV tem prioridade se elegível
        if renda <= 2640 and valor_imovel <= 170000:
            return 'MCMV_FAIXA_1'
        elif renda <= 4400 and valor_imovel <= 264000:
            return 'MCMV_FAIXA_2'
        elif renda <= 8000 and valor_imovel <= 350000:
            return 'MCMV_FAIXA_3'
        elif valor_imovel <= 1500000:
            return 'PRO_COTISTA_FGTS'
        else:
            return 'SBPE_TAXA_REFERENCIAL'


def create_fin_params_data() -> pd.DataFrame:
    """
    Cria dados de parâmetros de financiamento para carga no DW.
    
    Formato compatível com schema fin_params_caixa.
    """
    client = FinanciamentoCaixaClient()
    df = client.get_all_parameters()
    
    # Seleciona colunas do schema
    output = pd.DataFrame()
    output['id_parametro'] = df['id_parametro']
    output['tipo_financiamento'] = df['tipo_financiamento']
    output['taxa_juros_aa'] = df['taxa_juros_aa']
    output['prazo_max_meses'] = df['prazo_max_meses']
    
    return output


if __name__ == "__main__":
    # Exemplo de uso
    client = FinanciamentoCaixaClient()
    
    # Listar todos os parâmetros
    print("=== PARÂMETROS DE FINANCIAMENTO CAIXA ===\n")
    df = client.get_all_parameters()
    print(df[['tipo_financiamento', 'taxa_juros_aa', 'prazo_max_meses', 'tipo']].to_string())
    
    # Simulação
    print("\n\n=== SIMULAÇÃO DE FINANCIAMENTO ===")
    resultado = client.simulate_financing(
        valor_imovel=300000,
        renda_familiar=8000
    )
    for k, v in resultado.items():
        print(f"  {k}: {v}")
