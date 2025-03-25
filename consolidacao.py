import os
import pandas as pd
from pathlib import Path
import time

def read_csv_safe(file_path, delimiter=";"):
    """Tenta ler um CSV com o delimitador correto e tratamento de erros."""
    try:
        return pd.read_csv(file_path, delimiter=delimiter, encoding="utf-8")
    except Exception as e:
        print(f"Erro ao ler o arquivo {file_path.name}: {str(e)}")
        raise

def consolidar_dados():
    """Processa os arquivos consolidados e gera os dados finais."""
    BASE_DIR = Path(__file__).parent
    INVENTARIO_DIR = BASE_DIR / "inventário"

    if not INVENTARIO_DIR.exists():
        raise FileNotFoundError(f"Pasta 'inventário' não encontrada em {BASE_DIR}")

    for pasta in INVENTARIO_DIR.iterdir():
        if pasta.is_dir():
            print(f"\nProcessando: {pasta.name}")

            dados_dir = pasta / "dados_processados"
            output_file = dados_dir / "output.csv"
            consolidados_file = dados_dir / "dados_consolidados.csv"
            dados_tela_file = dados_dir / "arquivo_exibir.csv"

            if not dados_dir.exists():
                print(f"  ⚠️ Pasta 'dados_processados' não encontrada em {pasta}")
                continue

            try:
                # Verifique o conteúdo bruto dos arquivos
                with open(output_file, 'r', encoding='utf-8') as f:
                    print("\nConteúdo bruto de output.csv:")
                    print(f.readlines()[:5])  # Mostra as primeiras 5 linhas

                with open(consolidados_file, 'r', encoding='utf-8') as f:
                    print("\nConteúdo bruto de dados_consolidados.csv:")
                    print(f.readlines()[:5])  # Mostra as primeiras 5 linhas

                # Leitura dos arquivos com o delimitador correto
                print(f"  Lendo '{output_file.name}'...")
                df_output = read_csv_safe(output_file, delimiter=";")

                print(f"  Lendo '{consolidados_file.name}'...")
                df_consolidados = read_csv_safe(consolidados_file, delimiter=";")

                # Limpar espaços ou caracteres invisíveis nos nomes das colunas
                df_output.columns = df_output.columns.str.strip()
                df_consolidados.columns = df_consolidados.columns.str.strip()

                # Confirme os nomes das colunas
                print("\nColunas de output.csv após leitura:", df_output.columns.tolist())
                print("Primeiras linhas de output.csv:\n", df_output.head())

                print("\nColunas de dados_consolidados.csv após leitura:", df_consolidados.columns.tolist())
                print("Primeiras linhas de dados_consolidados.csv:\n", df_consolidados.head())

                # Verificar se as colunas necessárias existem
                if "GTIN" not in df_output.columns:
                    raise KeyError(f"A coluna 'GTIN' está ausente no arquivo '{output_file.name}'")
                if "CÓD. BARRAS" not in df_consolidados.columns:
                    raise KeyError(f"A coluna 'CÓD. BARRAS' está ausente no arquivo '{consolidados_file.name}'")

                # Garantir que os tipos das colunas sejam compatíveis
                df_output["GTIN"] = pd.to_numeric(df_output["GTIN"], errors="coerce")
                df_consolidados["CÓD. BARRAS"] = pd.to_numeric(df_consolidados["CÓD. BARRAS"], errors="coerce")

                # Combinar os dados
                print("  Combinando os dados...")
                df_tela = pd.merge(
                    df_output,
                    df_consolidados,
                    left_on="GTIN",
                    right_on="CÓD. BARRAS",
                    how="outer",
                    suffixes=("_output", "_consolidado")
                )

                # Salvar os dados combinados
                print(f"  Salvando dados combinados em '{dados_tela_file.name}'...")
                df_tela.to_csv(dados_tela_file, index=False, sep=";")
                print(f"  ✅ Arquivo '{dados_tela_file.name}' gerado com sucesso!")

            except Exception as e:
                print(f"  ❌ Erro crítico ao processar {pasta.name}: {str(e)}")
                continue

if __name__ == "__main__":
    while True:
        try:
            consolidar_dados()
        except Exception as e:
            print(f"Erro ao executar consolidação: {str(e)}")
        print("\nAguardando 5 minutos para a próxima execução...\n")
        time.sleep(300)