#!/usr/bin/env python3
"""
🧮 FASE 2 - INTEGRAÇÃO CBIC: Fatores Regionais Empíricos
========================================================

Este script integra dados reais do fact_cub_por_uf com os fatores regionais teóricos
do dim_metodo para calcular fatores empíricos baseados no mercado real.

METODOLOGIA:
1. Extrai médias dos últimos 12 meses do fact_cub_por_uf
2. Calcula fatores regionais reais usando SP como baseline (fator = 1.00)
3. Compara fatores teóricos vs reais
4. Atualiza dim_metodo_regional_completo com fatores empíricos
5. Gera relatório de discrepâncias significativas (>5%)

ENTRADA:
- fact_cub_por_uf.md (4.598 linhas)
- dim_metodo_regional_completo_20251114_175753.csv (270 linhas)

SAÍDA:
- dim_metodo_regional_empirico.csv (270 linhas com fatores reais)
- relatorio_comparacao_fatores.csv (análise teórico vs real)
- dim_metodo_regional_completo_FASE2.csv (versão final integrada)

Autor: matheusoption-bit
Data: 2025-11-14
Criticidade: ALTA - Apresentação 15/11/2025
"""

import os
import sys
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import numpy as np

# Constantes
CBIC_DATA_PATH = "docs/fact_cub_por_uf.md"
DIM_METODO_COMPLETO_PATH = "configs/dim_metodo_regional_completo_20251114_175753.csv"
OUTPUT_DIR = "configs"
BASELINE_UF = "SP"  # São Paulo como referência nacional
MESES_ANALISE = 12  # Últimos 12 meses para cálculo médias
THRESHOLD_DISCREPANCIA = 0.05  # 5% de diferença significativa

def load_cbic_data() -> pd.DataFrame:
    """
    Carrega dados do fact_cub_por_uf e prepara para análise.
    
    Returns:
        DataFrame com dados CBIC limpos e processados
    """
    print("🔄 Carregando dados CBIC do fact_cub_por_uf...")
    
    try:
        # Lê o arquivo tab-separated
        df = pd.read_csv(CBIC_DATA_PATH, sep='\t')
        
        # Converte data_referencia para datetime
        df['data_referencia'] = pd.to_datetime(df['data_referencia'])
        
        # Filtra apenas CUB-medio (padrão nacional)
        df = df[df['tipo_cub'] == 'CUB-medio'].copy()
        
        # Remove valores nulos ou zero
        df = df[(df['valor'] > 0) & (df['valor'].notna())].copy()
        
        print(f"✅ Dados CBIC carregados: {len(df):,} registros")
        print(f"📅 Período: {df['data_referencia'].min()} a {df['data_referencia'].max()}")
        print(f"🗺️  Estados: {df['uf'].nunique()}")
        
        return df
        
    except Exception as e:
        print(f"❌ Erro ao carregar dados CBIC: {str(e)}")
        raise

def calculate_recent_averages(df_cbic: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula médias dos últimos 12 meses por UF.
    
    Args:
        df_cbic: DataFrame com dados CBIC
        
    Returns:
        DataFrame com médias por UF
    """
    print(f"📊 Calculando médias dos últimos {MESES_ANALISE} meses...")
    
    # Encontra a data mais recente
    data_maxima = df_cbic['data_referencia'].max()
    data_corte = data_maxima - pd.DateOffset(months=MESES_ANALISE)
    
    print(f"📅 Analisando período: {data_corte.strftime('%Y-%m-%d')} a {data_maxima.strftime('%Y-%m-%d')}")
    
    # Filtra últimos 12 meses
    df_recente = df_cbic[df_cbic['data_referencia'] >= data_corte].copy()
    
    # Calcula médias por UF
    medias_uf = df_recente.groupby('uf').agg({
        'valor': ['mean', 'count', 'std'],
        'data_referencia': ['min', 'max']
    }).round(2)
    
    # Achatar colunas multi-level
    medias_uf.columns = ['valor_medio', 'num_registros', 'desvio_padrao', 'data_inicio', 'data_fim']
    medias_uf = medias_uf.reset_index()
    
    # Adicionar coeficiente de variação
    medias_uf['coef_variacao'] = (medias_uf['desvio_padrao'] / medias_uf['valor_medio']).round(4)
    
    print(f"✅ Médias calculadas para {len(medias_uf)} estados")
    
    return medias_uf

def calculate_empirical_factors(medias_uf: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula fatores regionais empíricos usando SP como baseline.
    
    Args:
        medias_uf: DataFrame com médias por UF
        
    Returns:
        DataFrame com fatores regionais reais
    """
    print(f"🧮 Calculando fatores regionais empíricos (baseline: {BASELINE_UF})...")
    
    # Encontra valor de SP (baseline)
    valor_sp = medias_uf[medias_uf['uf'] == BASELINE_UF]['valor_medio'].iloc[0]
    print(f"💰 Valor baseline {BASELINE_UF}: R$ {valor_sp:,.2f}/m²")
    
    # Calcula fatores regionais
    fatores = medias_uf.copy()
    fatores['fator_regional_real'] = (fatores['valor_medio'] / valor_sp).round(4)
    fatores['variacao_percentual'] = ((fatores['fator_regional_real'] - 1) * 100).round(2)
    
    # Ordena por fator (menores custos primeiro)
    fatores = fatores.sort_values('fator_regional_real')
    
    print("🏆 Top 5 estados com MENORES custos:")
    for _, row in fatores.head().iterrows():
        print(f"   {row['uf']}: {row['fator_regional_real']:.3f} ({row['variacao_percentual']:+.1f}%)")
    
    print("💸 Top 5 estados com MAIORES custos:")
    for _, row in fatores.tail().iterrows():
        print(f"   {row['uf']}: {row['fator_regional_real']:.3f} ({row['variacao_percentual']:+.1f}%)")
    
    return fatores

def load_theoretical_factors() -> pd.DataFrame:
    """
    Carrega fatores teóricos do dim_metodo_regional_completo.
    
    Returns:
        DataFrame com fatores teóricos
    """
    print("📚 Carregando fatores teóricos existentes...")
    
    df_teorico = pd.read_csv(DIM_METODO_COMPLETO_PATH)
    
    # Verificar qual coluna de fator existe
    fator_col = 'fator_regional_custo_novo' if 'fator_regional_custo_novo' in df_teorico.columns else 'fator_regional_custo'
    
    # Extrai fatores únicos por UF (pega apenas o primeiro método como referência)
    fatores_teoricos = df_teorico[df_teorico['id_metodo'] == 'MET_01'][
        ['uf', fator_col, 'nome_uf', 'regiao']
    ].copy()
    
    fatores_teoricos = fatores_teoricos.rename(columns={
        fator_col: 'fator_regional_teorico'
    })
    
    print(f"✅ Fatores teóricos carregados para {len(fatores_teoricos)} estados")
    
    return fatores_teoricos

def compare_theoretical_vs_empirical(fatores_reais: pd.DataFrame, 
                                   fatores_teoricos: pd.DataFrame) -> pd.DataFrame:
    """
    Compara fatores teóricos vs empíricos e identifica discrepâncias.
    
    Args:
        fatores_reais: DataFrame com fatores calculados dos dados CBIC
        fatores_teoricos: DataFrame com fatores teóricos atuais
        
    Returns:
        DataFrame com comparação completa
    """
    print("🔍 Comparando fatores teóricos vs empíricos...")
    
    # Merge dos dados
    comparacao = pd.merge(
        fatores_teoricos, 
        fatores_reais[['uf', 'fator_regional_real', 'valor_medio', 'num_registros']], 
        on='uf', 
        how='outer'
    )
    
    # Calcula diferenças
    comparacao['diferenca_absoluta'] = (
        comparacao['fator_regional_real'] - comparacao['fator_regional_teorico']
    ).round(4)
    
    comparacao['diferenca_percentual'] = (
        (comparacao['fator_regional_real'] / comparacao['fator_regional_teorico'] - 1) * 100
    ).round(2)
    
    # Identifica discrepâncias significativas
    comparacao['discrepancia_significativa'] = (
        abs(comparacao['diferenca_absoluta']) > THRESHOLD_DISCREPANCIA
    )
    
    # Classifica ajuste recomendado
    def classificar_ajuste(row):
        if pd.isna(row['diferenca_absoluta']):
            return "DADOS_INSUFICIENTES"
        elif abs(row['diferenca_absoluta']) <= 0.02:  # 2%
            return "MANTER_ATUAL"
        elif abs(row['diferenca_absoluta']) <= 0.05:  # 5%
            return "AJUSTE_LEVE"
        else:
            return "REVISAO_NECESSARIA"
    
    comparacao['recomendacao'] = comparacao.apply(classificar_ajuste, axis=1)
    
    # Ordena por magnitude da discrepância
    comparacao = comparacao.sort_values('diferenca_absoluta', key=abs, ascending=False)
    
    # Log dos resultados
    discrepancias = comparacao[comparacao['discrepancia_significativa'] == True]
    print(f"⚠️  Estados com discrepâncias significativas (>{THRESHOLD_DISCREPANCIA:.1%}): {len(discrepancias)}")
    
    for _, row in discrepancias.head(10).iterrows():
        print(
            f"   {row['uf']} ({row['nome_uf']}): "
            f"Teórico {row['fator_regional_teorico']:.3f} → "
            f"Real {row['fator_regional_real']:.3f} "
            f"({row['diferenca_percentual']:+.1f}%) - {row['recomendacao']}"
        )
    
    return comparacao

def generate_updated_dim_metodo(df_teorico: pd.DataFrame, 
                               comparacao: pd.DataFrame) -> pd.DataFrame:
    """
    Gera nova versão do dim_metodo com fatores empíricos integrados.
    
    Args:
        df_teorico: DataFrame original do dim_metodo
        comparacao: DataFrame com comparação teórico vs empírico
        
    Returns:
        DataFrame atualizado com fatores empíricos
    """
    print("🔄 Gerando dim_metodo atualizado com fatores empíricos...")
    
    # Cria cópia do dataset original
    df_atualizado = df_teorico.copy()
    
    # Cria dicionário de fatores empíricos
    fatores_empiricos = comparacao.set_index('uf')['fator_regional_real'].to_dict()
    recomendacoes = comparacao.set_index('uf')['recomendacao'].to_dict()
    
    # Função para decidir qual fator usar
    def escolher_fator(uf, fator_atual):
        if uf not in recomendacoes:
            return fator_atual, "MANTER_TEORICO"
        
        recomendacao = recomendacoes[uf]
        fator_empirico = fatores_empiricos.get(uf, fator_atual)
        
        if recomendacao in ["REVISAO_NECESSARIA", "AJUSTE_LEVE"]:
            return fator_empirico, "EMPIRICO_APLICADO"
        else:
            return fator_atual, "TEORICO_MANTIDO"
    
    # Aplica novos fatores
    def atualizar_linha(row):
        uf = row['uf']
        # Usar a coluna que existe
        fator_col = 'fator_regional_custo_novo' if 'fator_regional_custo_novo' in row.index else 'fator_regional_custo'
        fator_atual = row[fator_col]
        novo_fator, origem = escolher_fator(uf, fator_atual)
        
        row['fator_regional_custo_final'] = novo_fator
        row['origem_fator'] = origem
        row['fator_custo_regional_calc_final'] = round(row['fator_custo_base'] * novo_fator, 4)
        
        return row
    
    df_atualizado = df_atualizado.apply(atualizar_linha, axis=1)
    
    # Atualiza metadados
    df_atualizado['fonte_primaria'] = df_atualizado['fonte_primaria'].str.replace(
        'CBIC/SINAPI', 'CBIC_EMPIRICO/SINAPI'
    )
    df_atualizado['data_atualizacao_cub'] = datetime.now().strftime("%Y-%m-%d")
    
    # Atualiza notas importantes
    def atualizar_nota(row):
        uf = row['uf']
        if uf in recomendacoes and recomendacoes[uf] == "EMPIRICO_APLICADO":
            diferenca = comparacao[comparacao['uf'] == uf]['diferenca_percentual'].iloc[0]
            return f"Fator empírico CBIC ({diferenca:+.1f}% vs teórico)"
        else:
            return row['nota_importante']
    
    df_atualizado['nota_importante'] = df_atualizado.apply(atualizar_nota, axis=1)
    
    # Contabiliza mudanças
    mudancas = len(df_atualizado[df_atualizado['origem_fator'] == 'EMPIRICO_APLICADO'])
    total_linhas = len(df_atualizado)
    
    print(f"✅ Fatores atualizados: {mudancas}/{total_linhas} linhas ({mudancas/total_linhas*100:.1f}%)")
    
    return df_atualizado

def save_results(fatores_reais: pd.DataFrame, 
                comparacao: pd.DataFrame,
                dim_metodo_atualizado: pd.DataFrame):
    """
    Salva todos os resultados da Fase 2.
    
    Args:
        fatores_reais: Fatores empíricos calculados
        comparacao: Comparação teórico vs empírico
        dim_metodo_atualizado: Dataset final atualizado
    """
    print("💾 Salvando resultados da Fase 2...")
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # 1. Fatores empíricos
    fatores_path = f"{OUTPUT_DIR}/fatores_regionais_empiricos_{timestamp}.csv"
    fatores_reais.to_csv(fatores_path, index=False, encoding='utf-8')
    print(f"📊 Fatores empíricos salvos: {fatores_path}")
    
    # 2. Relatório de comparação
    comparacao_path = f"{OUTPUT_DIR}/relatorio_comparacao_fatores_{timestamp}.csv"
    comparacao.to_csv(comparacao_path, index=False, encoding='utf-8')
    print(f"📋 Relatório comparação salvo: {comparacao_path}")
    
    # 3. Dim_metodo atualizado
    dim_metodo_path = f"{OUTPUT_DIR}/dim_metodo_regional_FASE2_{timestamp}.csv"
    dim_metodo_atualizado.to_csv(dim_metodo_path, index=False, encoding='utf-8')
    print(f"🎯 Dim_metodo Fase 2 salvo: {dim_metodo_path}")
    
    # 4. Cópia como versão "latest"
    latest_path = f"{OUTPUT_DIR}/dim_metodo_regional_completo_LATEST.csv"
    dim_metodo_atualizado.to_csv(latest_path, index=False, encoding='utf-8')
    print(f"🔄 Versão latest atualizada: {latest_path}")

def generate_executive_summary(comparacao: pd.DataFrame):
    """
    Gera resumo executivo dos resultados da Fase 2.
    
    Args:
        comparacao: DataFrame com comparação teórico vs empírico
    """
    print("\n" + "="*70)
    print("📊 RESUMO EXECUTIVO - FASE 2: INTEGRAÇÃO CBIC")
    print("="*70)
    
    # Estatísticas gerais
    total_estados = len(comparacao)
    discrepancias_significativas = len(comparacao[comparacao['discrepancia_significativa'] == True])
    revisoes_necessarias = len(comparacao[comparacao['recomendacao'] == 'REVISAO_NECESSARIA'])
    ajustes_leves = len(comparacao[comparacao['recomendacao'] == 'AJUSTE_LEVE'])
    manter_atual = len(comparacao[comparacao['recomendacao'] == 'MANTER_ATUAL'])
    
    print(f"🗺️  Estados analisados: {total_estados}")
    print(f"⚠️  Discrepâncias significativas (>{THRESHOLD_DISCREPANCIA:.1%}): {discrepancias_significativas}")
    print(f"🔴 Revisões necessárias: {revisoes_necessarias}")
    print(f"🟡 Ajustes leves: {ajustes_leves}")
    print(f"🟢 Manter atuais: {manter_atual}")
    
    # Top insights
    print("\n🏆 TOP INSIGHTS:")
    
    maior_subestimacao = comparacao.loc[comparacao['diferenca_absoluta'].idxmax()]
    print(f"• Maior subestimação: {maior_subestimacao['nome_uf']} "
                f"({maior_subestimacao['diferenca_percentual']:+.1f}%)")
    
    maior_superestimacao = comparacao.loc[comparacao['diferenca_absoluta'].idxmin()]
    print(f"• Maior superestimação: {maior_superestimacao['nome_uf']} "
                f"({maior_superestimacao['diferenca_percentual']:+.1f}%)")
    
    # Análise por região
    print("\n🗺️  ANÁLISE POR REGIÃO:")
    regioes = comparacao.groupby('regiao').agg({
        'diferenca_absoluta': ['mean', 'std'],
        'discrepancia_significativa': 'sum'
    }).round(3)
    
    for regiao in regioes.index:
        media_diff = regioes.loc[regiao, ('diferenca_absoluta', 'mean')]
        discrepancias = int(regioes.loc[regiao, ('discrepancia_significativa', 'sum')])
        print(f"• {regiao}: diferença média {media_diff:+.3f}, "
                    f"{discrepancias} discrepâncias significativas")
    
    print("\n✅ FASE 2 CONCLUÍDA COM SUCESSO!")
    print("="*70 + "\n")

def main():
    """Função principal da Fase 2."""
    print("🚀 INICIANDO FASE 2: INTEGRAÇÃO CBIC COM FATORES EMPÍRICOS")
    print(f"📅 Data/Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # Passo 1: Carregar dados CBIC
        df_cbic = load_cbic_data()
        
        # Passo 2: Calcular médias recentes
        medias_uf = calculate_recent_averages(df_cbic)
        
        # Passo 3: Calcular fatores empíricos
        fatores_reais = calculate_empirical_factors(medias_uf)
        
        # Passo 4: Carregar fatores teóricos
        fatores_teoricos = load_theoretical_factors()
        
        # Passo 5: Comparar teórico vs empírico
        comparacao = compare_theoretical_vs_empirical(fatores_reais, fatores_teoricos)
        
        # Passo 6: Carregar dim_metodo original
        df_teorico = pd.read_csv(DIM_METODO_COMPLETO_PATH)
        
        # Passo 7: Gerar versão atualizada
        dim_metodo_atualizado = generate_updated_dim_metodo(df_teorico, comparacao)
        
        # Passo 8: Salvar resultados
        save_results(fatores_reais, comparacao, dim_metodo_atualizado)
        
        # Passo 9: Resumo executivo
        generate_executive_summary(comparacao)
        
        print("🎉 FASE 2 CONCLUÍDA COM SUCESSO!")
        
    except Exception as e:
        print(f"❌ Erro na Fase 2: {str(e)}")
        raise

if __name__ == "__main__":
    main()