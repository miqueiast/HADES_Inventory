import os
import csv
import logging
import pandas as pd
from tkinter import Tk, filedialog, messagebox

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

            # Remove zeros à esquerda das colunas numéricas, exceto para o valor '0'
            colunas_para_tratar = ["GTIN", "Código Interno", "Preço", "Estoque", "Custo", "Seção"]
            for coluna in colunas_para_tratar:
                if coluna in df.columns:
                    df[coluna] = df[coluna].apply(lambda x: str(int(x)) if str(x).isdigit() else x)

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

    # Configuração do Tkinter para abrir janelas de diálogo
    root = Tk()
    root.withdraw()  # Oculta a janela principal do Tkinter

    # Selecionar o arquivo TXT
    input_path = filedialog.askopenfilename(
        title="Selecione o arquivo TXT",
        filetypes=[("Arquivos TXT", "*.txt")]
    )

    if not input_path:  # Se o usuário não selecionar um arquivo
        messagebox.showerror("Erro", "Nenhum arquivo TXT foi selecionado. O programa será encerrado.")
        return

    logging.info(f"Arquivo TXT selecionado: {input_path}")

    # Selecionar a pasta dentro de 'inventário'
    inventory_folder = os.path.join(os.getcwd(), "inventário")
    if not os.path.exists(inventory_folder):
        messagebox.showerror("Erro", f"A pasta 'inventário' não foi encontrada no diretório atual: {os.getcwd()}")
        return

    selected_folder = filedialog.askdirectory(
        title="Selecione uma pasta em 'inventário'",
        initialdir=inventory_folder
    )

    if not selected_folder:  # Se o usuário não selecionar uma pasta
        messagebox.showerror("Erro", "Nenhuma pasta foi selecionada. O programa será encerrado.")
        return

    logging.info(f"Pasta selecionada: {selected_folder}")

    # Caminho para salvar o arquivo CSV na subpasta 'dados_processados'
    output_folder = os.path.join(selected_folder, "dados_processados")
    os.makedirs(output_folder, exist_ok=True)  # Cria a pasta 'dados_processados' se não existir
    output_path = os.path.join(output_folder, "output.csv")

    # Processa o arquivo TXT e salva como CSV
    try:
        df = processor.process_txt_to_csv(input_path, output_path)
        messagebox.showinfo("Sucesso", f"Arquivo processado com sucesso! CSV salvo em: {output_path}")
        print("\n=== Resumo do processamento ===")
        print(df.head())  # Exibe as 5 primeiras linhas do DataFrame
    except Exception as e:
        logging.error(f"Erro durante o processamento: {e}")
        messagebox.showerror("Erro", f"Ocorreu um erro durante o processamento: {e}")


if __name__ == "__main__":
    main()