import os
import time
import pandas as pd

def criar_inventario():
    # Solicita os dados ao usuário
    nome_inventario = input("Digite o nome do inventário: ").strip()
    nome_loja = input("Digite o nome da loja: ").strip()
    nome_pessoa = input("Digite o nome da pessoa: ").strip()
    
    # Define o caminho principal da pasta "Inventário"
    pasta_principal = "Inventário"
    
    # Cria a pasta principal "Inventário" se ela ainda não existir
    if not os.path.exists(pasta_principal):
        os.makedirs(pasta_principal)
    
    # Define o caminho da nova pasta do inventário
    caminho_inventario = os.path.join(pasta_principal, nome_inventario)
    
    # Verifica se a pasta do inventário já existe
    if os.path.exists(caminho_inventario):
        print(f"A pasta com o nome '{nome_inventario}' já existe!")
        return
    
    # Cria a nova pasta do inventário
    os.makedirs(caminho_inventario)
    
    # Cria os arquivos "txt" e "dados_processados" dentro da pasta do inventário
    arquivo_txt = os.path.join(caminho_inventario, "txt")
    pasta_dados_processados = os.path.join(caminho_inventario, "dados_processados")
    os.makedirs(arquivo_txt)
    os.makedirs(pasta_dados_processados)
    
    # Cria um arquivo de texto com as informações fornecidas
    arquivo_info = os.path.join(caminho_inventario, "informacoes.txt")
    with open(arquivo_info, "w") as arquivo:
        arquivo.write(f"Nome do Inventário: {nome_inventario}\n")
        arquivo.write(f"Nome da Loja: {nome_loja}\n")
        arquivo.write(f"Nome da Pessoa: {nome_pessoa}\n")
    
    print(f"Pasta do inventário '{nome_inventario}' criada com sucesso!")
    print(f"As informações foram salvas em '{arquivo_info}'.")
    
    # Cria a pasta na área de trabalho do usuário
    area_de_trabalho = os.path.join(os.path.expanduser("~"), "Desktop")  # Caminho para a área de trabalho
    pasta_dados_contagem = os.path.join(area_de_trabalho, f"{nome_inventario} - Inserir Dados de Contagem")
    os.makedirs(pasta_dados_contagem)
    
    print(f"A pasta '{pasta_dados_contagem}' foi criada na área de trabalho!")
    
    # Inicia a observação da pasta
    observar_pasta(pasta_dados_contagem, pasta_dados_processados)

def observar_pasta(pasta_contagem, pasta_dados_processados):
    print(f"Observando a pasta '{pasta_contagem}' para arquivos .xlsx...")
    arquivos_existentes = set(os.listdir(pasta_contagem))  # Lista inicial de arquivos na pasta
    
    df_consolidado = pd.DataFrame()  # DataFrame para consolidar todos os dados
    
    while True:
        time.sleep(2)  # Aguarda 2 segundos entre verificações
        arquivos_atualizados = set(os.listdir(pasta_contagem))
        novos_arquivos = arquivos_atualizados - arquivos_existentes  # Detecta novos arquivos
        
        for arquivo in novos_arquivos:
            if arquivo.endswith(".xlsx"):  # Verifica se é um arquivo .xlsx
                caminho_arquivo = os.path.join(pasta_contagem, arquivo)
                print(f"Novo arquivo detectado: {caminho_arquivo}")
                
                # Processa o novo arquivo e atualiza o consolidado
                df_consolidado = processar_arquivo_excel(caminho_arquivo, df_consolidado)
                
                # Salva o DataFrame consolidado como CSV
                salvar_consolidado(df_consolidado, pasta_dados_processados)
        
        arquivos_existentes = arquivos_atualizados  # Atualiza a lista de arquivos conhecidos

def processar_arquivo_excel(caminho_arquivo, df_consolidado):
    try:
        # Lê o arquivo Excel
        df = pd.read_excel(caminho_arquivo)
        
        # Verifica se as colunas esperadas estão presentes
        colunas_esperadas = ["LOJA KEY", "OPERADOR", "ENDEREÇO", "CÓD. BARRAS", "QNT. CONTADA"]
        if not all(coluna in df.columns for coluna in colunas_esperadas):
            print(f"Erro: O arquivo '{caminho_arquivo}' não contém as colunas necessárias.")
            return df_consolidado
        
        # Converte os tipos de dados das colunas
        df["LOJA KEY"] = df["LOJA KEY"].astype(int)
        df["OPERADOR"] = df["OPERADOR"].astype(str)
        df["ENDEREÇO"] = df["ENDEREÇO"].astype(int)
        df["CÓD. BARRAS"] = df["CÓD. BARRAS"].astype(int)
        df["QNT. CONTADA"] = df["QNT. CONTADA"].astype(float)
        
        # Agrupa os dados somando "QNT. CONTADA" e combinando "OPERADOR"
        df_agrupado = (
            df.groupby(["LOJA KEY", "ENDEREÇO", "CÓD. BARRAS"], as_index=False)
            .agg({
                "QNT. CONTADA": "sum",
                "OPERADOR": lambda x: "/".join(sorted(set(x)))  # Combina operadores únicos
            })
        )
        
        # Concatena com o DataFrame consolidado
        df_consolidado = pd.concat([df_consolidado, df_agrupado], ignore_index=True)
        
        # Reagrupa para evitar duplicações no consolidado
        df_consolidado = (
            df_consolidado.groupby(["LOJA KEY", "ENDEREÇO", "CÓD. BARRAS"], as_index=False)
            .agg({
                "QNT. CONTADA": "sum",
                "OPERADOR": lambda x: "/".join(sorted(set(x)))  # Combina operadores únicos novamente
            })
        )
        
        print(f"Arquivo '{caminho_arquivo}' processado e adicionado ao consolidado.")
        return df_consolidado
    
    except Exception as e:
        print(f"Erro ao processar o arquivo '{caminho_arquivo}': {e}")
        return df_consolidado

def salvar_consolidado(df_consolidado, pasta_dados_processados):
    try:
        # Define o nome do arquivo consolidado
        caminho_csv = os.path.join(pasta_dados_processados, "dados_consolidados.csv")
        
        # Salva o DataFrame consolidado no formato CSV com separador ";"
        df_consolidado.to_csv(caminho_csv, index=False, sep=";")
        print(f"Arquivo consolidado salvo em: {caminho_csv}")
    except Exception as e:
        print(f"Erro ao salvar o arquivo consolidado: {e}")

# Executa o programa
if __name__ == "__main__":
    criar_inventario()