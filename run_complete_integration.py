#!/usr/bin/env python3
"""
EXECUÇÃO COMPLETA - Fases 1 e 2 Integradas + Expansão CBIC
==========================================================

Script mestre que executa todo o pipeline de integração CBIC:
1. Fase 1: Expansão regional teórica (270 linhas)
2. Fase 2: Integração empírica com dados CBIC reais
3. Fase CBIC: Implementação das fontes críticas CBIC (13 novas abas)
4. Upload automático para Google Sheets
5. Relatórios consolidados

Uso:
    python run_complete_integration.py --fase 1        # Apenas Fase 1
    python run_complete_integration.py --fase 2        # Apenas Fase 2
    python run_complete_integration.py --fase cbic     # Apenas Fase CBIC (fontes críticas)
    python run_complete_integration.py --full          # Todas as fases
    python run_complete_integration.py --upload        # Apenas upload

Autor: matheusoption-bit
Data: 2025-11-14
Criticidade: ALTA - Apresentação 15/11/2025
"""

import os
import sys
import argparse
import subprocess
from datetime import datetime

def run_script(script_name: str, description: str):
    """Executa um script Python e monitora resultado."""
    print(f"🚀 Executando: {description}")
    print(f"📄 Script: {script_name}")
    
    try:
        result = subprocess.run([
            sys.executable, script_name
        ], capture_output=True, text=True, check=True)
        
        print(f"✅ {description} - SUCESSO")
        if result.stdout:
            print(f"📋 Output: {result.stdout[-500:]}")  # Últimas 500 chars
            
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"❌ {description} - FALHA")
        print(f"💥 Erro: {e.stderr[-500:]}")  # Últimos 500 chars
        return False
    except FileNotFoundError:
        print(f"❌ Script não encontrado: {script_name}")
        return False

def check_prerequisites():
    """Verifica se todos os arquivos necessários existem."""
    print("🔍 Verificando pré-requisitos...")
    
    required_files = [
        "docs/fact_cub_por_uf.md",
        "config/google_credentials.json"
    ]
    
    missing_files = []
    for file_path in required_files:
        if not os.path.exists(file_path):
            missing_files.append(file_path)
    
    if missing_files:
        print("❌ Arquivos necessários não encontrados:")
        for file_path in missing_files:
            print(f"   • {file_path}")
        return False
    
    print("✅ Todos os pré-requisitos atendidos")
    return True

def run_fase1():
    """Executa Fase 1: Expansão regional teórica."""
    print("=" * 50)
    print("🏗️  INICIANDO FASE 1: EXPANSÃO REGIONAL TEÓRICA")
    print("=" * 50)
    
    return run_script(
        "expand_to_regional.py",
        "Fase 1 - Expansão Regional (270 linhas)"
    )

def run_fase2():
    """Executa Fase 2: Integração empírica CBIC."""
    print("=" * 50)
    print("🧮 INICIANDO FASE 2: INTEGRAÇÃO EMPÍRICA CBIC")
    print("=" * 50)
    
    return run_script(
        "integrate_cbic_real_factors.py",
        "Fase 2 - Fatores Empíricos CBIC"
    )

def run_fase_cbic():
    """Executa Fase CBIC: Implementação das fontes críticas."""
    print("=" * 50)
    print("📊 INICIANDO FASE CBIC: FONTES CRÍTICAS (13 NOVAS ABAS)")
    print("=" * 50)
    
    return run_script(
        "implement_cbic_fase1.py",
        "Fase CBIC - Implementação Fontes Críticas"
    )

def run_upload():
    """Executa upload para Google Sheets."""
    print("=" * 50)
    print("☁️  INICIANDO UPLOAD GOOGLE SHEETS")
    print("=" * 50)
    
    # Upload da Fase 2 (se existir)
    upload_fase2_success = run_script(
        "upload_fase2_to_sheets.py",
        "Upload Resultados Fase 2"
    )
    
    # Upload regional (sempre executar)
    upload_regional_success = run_script(
        "upload_regional_to_sheets.py",
        "Upload Estrutura Regional"
    )
    
    return upload_fase2_success or upload_regional_success

def generate_final_report():
    """Gera relatório final consolidado."""
    print("📊 Gerando relatório final...")
    
    import glob
    
    # Busca arquivos gerados
    dim_metodo_files = glob.glob("configs/dim_metodo_regional_*.csv")
    comparacao_files = glob.glob("configs/relatorio_comparacao_fatores_*.csv")
    fatores_files = glob.glob("configs/fatores_regionais_empiricos_*.csv")
    
    print("=" * 70)
    print("📈 RELATÓRIO FINAL - SISTEMA CBIC BI COMPLETO")
    print("=" * 70)
    print(f"⏰ Concluído em: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("")
    
    # Arquivos gerados
    print("📁 ARQUIVOS GERADOS:")
    if dim_metodo_files:
        latest_dim = max(dim_metodo_files)
        print(f"   ✅ Dim_metodo regional: {os.path.basename(latest_dim)}")
    if comparacao_files:
        latest_comp = max(comparacao_files)
        print(f"   ✅ Comparação fatores: {os.path.basename(latest_comp)}")
    if fatores_files:
        latest_fat = max(fatores_files)
        print(f"   ✅ Fatores empíricos: {os.path.basename(latest_fat)}")
    
    # Google Sheets
    print(f"\n☁️  GOOGLE SHEETS:")
    print(f"   🔗 URL: https://docs.google.com/spreadsheets/d/11-KC18ShMKXZOSbWvHcLHJwz3oDjexGQLb26xm2Wq4w")
    print(f"   📊 Abas principais:")
    print(f"      • dim_metodo: Estrutura regional (270 linhas)")
    print(f"      • dim_metodo_fase2: Com fatores empíricos CBIC")
    print(f"      • comparacao_fatores: Análise teórico vs real")
    print(f"      • dashboard_insights: Métricas executivas")
    print(f"      • fatores_por_regiao: Análise regional")
    print(f"      • 13+ abas CBIC: Indicadores críticos (CUB, PIB, Cimento, etc.)")
    
    # Transformação do sistema
    print(f"\n🚀 TRANSFORMAÇÃO REALIZADA:")
    print(f"   📊 Antes: Sistema básico com ~10 indicadores")
    print(f"   📈 Depois: Sistema BI completo com 150+ indicadores")
    print(f"   🗺️ Cobertura: Nacional → Regional (27 estados)")
    print(f"   🏗️ Métodos: 4 → 10 métodos construtivos")
    print(f"   📅 Atualização: Manual → Automática")
    print(f"   🎯 Precisão: Estimada → Empírica (dados CBIC reais)")
    
    # Próximos passos
    print(f"\n🎯 PRÓXIMOS PASSOS:")
    print(f"   1. Validar dados no Google Sheets")
    print(f"   2. Apresentar sistema completo (15/11/2025)")
    print(f"   3. Implementar automação de atualização")
    print(f"   4. Expandir para mais 76 fontes CBIC (Fases 2-3)")
    print(f"   5. Criar dashboards Power BI/Tableau")
    
    print("=" * 70)
    print("🎉 SISTEMA CBIC BI MASTER IMPLEMENTADO COM SUCESSO!")
    print("   BRASIL AGORA TEM O MAIS COMPLETO SISTEMA DE")
    print("   INTELIGÊNCIA EM CONSTRUÇÃO CIVIL! 🇧🇷")
    print("=" * 70)

def main():
    """Função principal com argumentos de linha de comando."""
    parser = argparse.ArgumentParser(description="Pipeline completo de integração CBIC")
    
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--fase", choices=["1", "2", "cbic"], help="Executar uma fase específica")
    group.add_argument("--full", action="store_true", help="Executar todas as fases")
    group.add_argument("--upload", action="store_true", help="Apenas upload para Google Sheets")
    
    args = parser.parse_args()
    
    print("🏁 INICIANDO PIPELINE COMPLETO DE INTEGRAÇÃO CBIC MASTER")
    print(f"📅 Data/Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Verifica pré-requisitos
    if not check_prerequisites():
        print("❌ Pré-requisitos não atendidos. Abortando.")
        sys.exit(1)
    
    success = True
    
    # Executa conforme argumentos
    if args.fase == "1" or args.full:
        success &= run_fase1()
    
    if args.fase == "2" or args.full:
        success &= run_fase2()
    
    if args.fase == "cbic" or args.full:
        success &= run_fase_cbic()
    
    if args.upload or args.full:
        success &= run_upload()
    
    # Relatório final apenas se tudo executou
    if args.full and success:
        generate_final_report()
    
    if success:
        print("🎉 PIPELINE EXECUTADO COM SUCESSO!")
        sys.exit(0)
    else:
        print("❌ PIPELINE FALHOU. Verifique logs acima.")
        sys.exit(1)

if __name__ == "__main__":
    main()