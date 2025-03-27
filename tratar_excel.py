import os
import pandas as pd
from tkinter import Tk, filedialog

def selecionar_e_processar_arquivo():
    # Ocultar a janela raiz do Tkinter
    Tk().withdraw()
    
    # Selecionar o arquivo Excel ou CSV
    arquivo = filedialog.askopenfilename(
        title="Selecione o arquivo Excel ou CSV",
        filetypes=[("Arquivos Excel", "*.xlsx;*.xls"), ("Arquivos CSV", "*.csv")]
    )
    
    if not arquivo:
        print("Nenhum arquivo foi selecionado.")
        return

    # Verificar se a pasta "Inventário" existe no diretório atual
    pasta_inventario = os.path.join(os.getcwd(), "Inventário")
    if not os.path.exists(pasta_inventario):
        print(f'A pasta "Inventário" não foi encontrada no diretório: {os.getcwd()}')
        return

    # Solicitar ao usuário a subpasta do inventário que deseja usar
    subpastas = [f for f in os.listdir(pasta_inventario) if os.path.isdir(os.path.join(pasta_inventario, f))]
    if not subpastas:
        print(f'Não há subpastas na pasta "Inventário".')
        return

    print("Subpastas disponíveis na pasta 'Inventário':")
    for i, subpasta in enumerate(subpastas):
        print(f"{i + 1}. {subpasta}")

    escolha = input("Digite o número da subpasta que deseja usar: ")
    try:
        escolha = int(escolha) - 1
        if escolha < 0 or escolha >= len(subpastas):
            raise ValueError
        subpasta_escolhida = subpastas[escolha]
    except ValueError:
        print("Escolha inválida.")
        return

    caminho_subpasta = os.path.join(pasta_inventario, subpasta_escolhida)

    # Carregar o arquivo Excel ou CSV
    try:
        if arquivo.endswith(".csv"):
            # Detectar o separador automaticamente
            with open(arquivo, 'r', encoding='utf-8') as f:
                primeiro_caractere = f.read(1024).splitlines()[0]
                separador = ';' if ';' in primeiro_caractere else ','

            df = pd.read_csv(arquivo, sep=separador, encoding='utf-8')
        else:
            df = pd.read_excel(arquivo)

        print("Arquivo carregado com sucesso!")
    except Exception as e:
        print(f"Erro ao carregar o arquivo: {e}")
        return

    # Tratar as colunas (ajustar nomes, remover acentos, etc.)
    df.columns = df.columns.str.strip()  # Remover espaços extras
    df.columns = df.columns.str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
    df.columns = df.columns.str.replace(' ', '_').str.lower()  # Substituir espaços por underline e passar para minúsculas

    # Verificar se as colunas esperadas estão presentes
    colunas_esperadas = ['loja_key', 'operador', 'endereco', 'cod._barras', 'qnt._contada']
    if not all(coluna in df.columns for coluna in colunas_esperadas):
        print("As colunas do arquivo não correspondem às esperadas:")
        print(f"Colunas esperadas: {colunas_esperadas}")
        print(f"Colunas encontradas: {list(df.columns)}")
        return

    # Salvar o arquivo processado na subpasta escolhida
    try:
        nome_arquivo_saida = os.path.basename(arquivo)
        caminho_saida = os.path.join(caminho_subpasta, nome_arquivo_saida)

        # Salvar como Excel
        df.to_excel(caminho_saida, index=False)
        print(f"Arquivo processado e salvo em: {caminho_saida}")
    except Exception as e:
        print(f"Erro ao salvar o arquivo processado: {e}")


# Chamar a função
if __name__ == "__main__":
    selecionar_e_processar_arquivo()