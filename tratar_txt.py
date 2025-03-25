import os
import csv
import logging
import pandas as pd

# Configuração de logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


class FileProcessor:
    def __init__(self):
        pass

    def process_txt_to_csv(self, input_path, output_path):
        """
        Processa um arquivo TXT e salva como CSV com ';' como separador.
        :param input_path: Caminho do arquivo TXT.
        :param output_path: Caminho onde o CSV gerado será salvo.
        :return: Um DataFrame contendo os dados processados.
        """
        try:
            logging.info(f"Iniciando processamento do arquivo TXT: {input_path}")

            # Lista para armazenar os dados processados
            dados_tratados = []

            # Abrir o arquivo TXT e processar linha por linha
            with open(input_path, 'r', encoding='latin-1') as file:  # Codificação ajustada para compatibilidade
                linhas = file.readlines()

            for idx, linha in enumerate(linhas):
                try:
                    # Remover espaços extras e quebras de linha
                    linha = linha.strip()

                    # Dividir a linha em partes usando os espaços
                    partes = linha.split()

                    # Garantir que a linha tenha pelo menos 7 colunas
                    if len(partes) < 7:
                        logging.warning(f"Linha ignorada ({idx + 1}): {linha}")
                        continue

                    # Identificar as colunas
                    gtin = partes[0]
                    codigo_interno = partes[1]
                    secao = partes[-1]
                    custo = partes[-2]
                    estoque = partes[-3]
                    preco = partes[-4]
                    descricao = " ".join(partes[2:-4])

                    # Adiciona os dados tratados à lista
                    dados_tratados.append([gtin, codigo_interno, descricao, preco, estoque, custo, secao])

                except Exception as e:
                    logging.error(f"Erro ao processar linha {idx + 1}: {e}")

            # Converte os dados para um DataFrame
            df = pd.DataFrame(dados_tratados, columns=["GTIN", "Código Interno", "Descrição", "Preço", "Estoque", "Custo", "Seção"])

            # Salvar os dados processados no arquivo CSV
            df.to_csv(output_path, index=False, sep=";", encoding="utf-8")
            logging.info(f"Arquivo TXT processado com sucesso! CSV salvo em: {output_path}")

            return df

        except Exception as e:
            logging.error(f"Erro ao processar o arquivo TXT: {e}")
            raise


def main():
    """
    Função principal para processar arquivos TXT.
    """
    # Inicializa o processador de arquivos
    processor = FileProcessor()

    # Caminhos de exemplo (personalize conforme necessário)
    input_path = input("Digite o caminho do arquivo TXT que deseja processar: ").strip()
    output_folder = os.path.dirname(input_path)  # Salva o CSV na mesma pasta do TXT
    output_path = os.path.join(output_folder, "output.csv")  # Nome do arquivo CSV gerado

    # Verifica se o arquivo TXT existe
    if not os.path.exists(input_path):
        logging.error(f"O arquivo {input_path} não foi encontrado. Certifique-se de que o caminho está correto.")
        return

    # Processa o arquivo TXT e salva como CSV
    try:
        df = processor.process_txt_to_csv(input_path, output_path)
        print("\n=== Resumo do processamento ===")
        print(df.head())  # Exibe as 5 primeiras linhas do DataFrame
    except Exception as e:
        logging.error(f"Erro durante o processamento: {e}")


if __name__ == "__main__":
    main()