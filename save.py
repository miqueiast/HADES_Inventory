import os
from datetime import datetime

def listar_inventarios(pasta_principal):
    """Lista todos os inventários disponíveis na pasta principal."""
    if not os.path.exists(pasta_principal):
        print("Nenhum inventário encontrado. Certifique-se de criar um inventário primeiro.")
        return []
    inventarios = [nome for nome in os.listdir(pasta_principal) if os.path.isdir(os.path.join(pasta_principal, nome))]
    return inventarios

def acessar_inventario(pasta_principal):
    """Permite ao usuário selecionar um inventário para acessar."""
    inventarios = listar_inventarios(pasta_principal)
    if not inventarios:
        return None

    print("\nInventários disponíveis:")
    for i, inventario in enumerate(inventarios, start=1):
        print(f"{i}. {inventario}")

    while True:
        try:
            escolha = int(input("\nDigite o número do inventário que deseja acessar: "))
            if 1 <= escolha <= len(inventarios):
                return inventarios[escolha - 1]
            else:
                print("Opção inválida. Tente novamente.")
        except ValueError:
            print("Por favor, digite um número válido.")

def registrar_log(caminho_inventario, mensagem):
    """Registra uma mensagem no arquivo de log dentro da pasta do inventário."""
    arquivo_log = os.path.join(caminho_inventario, "log.txt")
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(arquivo_log, "a") as log:
        log.write(f"[{timestamp}] {mensagem}\n")

def explorar_inventario(caminho_inventario):
    """Permite ao usuário explorar o inventário sem fazer modificações."""
    print(f"\nVocê está acessando o inventário: {os.path.basename(caminho_inventario)}")
    print("Ações disponíveis:")
    print("1. Listar arquivos na pasta")
    print("2. Sair do inventário")

    while True:
        escolha = input("\nEscolha uma ação: ").strip()
        if escolha == "1":
            arquivos = os.listdir(caminho_inventario)
            print("\nArquivos na pasta:")
            for arquivo in arquivos:
                print(f"- {arquivo}")
            registrar_log(caminho_inventario, "Listagem de arquivos realizada.")
        elif escolha == "2":
            print("Saindo do inventário...")
            registrar_log(caminho_inventario, "Usuário saiu do inventário.")
            break
        else:
            print("Opção inválida. Tente novamente.")

def executar_no_inventario(caminho_inventario):
    """Permite realizar operações no inventário."""
    print(f"\nVocê está agora na pasta do inventário: {os.path.basename(caminho_inventario)}")
    print("Ações disponíveis:")
    print("1. Criar um arquivo")
    print("2. Listar arquivos na pasta")
    print("3. Sair")

    while True:
        escolha = input("\nEscolha uma ação: ").strip()
        if escolha == "1":
            nome_arquivo = input("Digite o nome do arquivo a ser criado (com extensão): ").strip()
            caminho_arquivo = os.path.join(caminho_inventario, nome_arquivo)
            if os.path.exists(caminho_arquivo):
                print("Arquivo já existe!")
            else:
                with open(caminho_arquivo, "w") as arquivo:
                    conteudo = input("Digite o conteúdo do arquivo: ")
                    arquivo.write(conteudo)
                print(f"Arquivo '{nome_arquivo}' criado com sucesso!")
                registrar_log(caminho_inventario, f"Arquivo '{nome_arquivo}' criado.")
        elif escolha == "2":
            arquivos = os.listdir(caminho_inventario)
            print("\nArquivos na pasta:")
            for arquivo in arquivos:
                print(f"- {arquivo}")
            registrar_log(caminho_inventario, "Listagem de arquivos realizada.")
        elif escolha == "3":
            print("Saindo do inventário...")
            registrar_log(caminho_inventario, "Usuário saiu do inventário.")
            break
        else:
            print("Opção inválida. Tente novamente.")

def main():
    pasta_principal = "Inventário"

    print("=== Sistema de Gestão de Inventários ===")
    inventario_selecionado = acessar_inventario(pasta_principal)
    if inventario_selecionado:
        caminho_inventario = os.path.join(pasta_principal, inventario_selecionado)
        registrar_log(caminho_inventario, "Inventário acessado.")
        
        print("\nEscolha o modo de acesso:")
        print("1. Explorar (somente visualizar)")
        print("2. Modificar (criar ou alterar arquivos)")

        while True:
            modo = input("\nEscolha o modo (1 ou 2): ").strip()
            if modo == "1":
                explorar_inventario(caminho_inventario)
                break
            elif modo == "2":
                executar_no_inventario(caminho_inventario)
                break
            else:
                print("Opção inválida. Tente novamente.")
    else:
        print("Nenhum inventário foi acessado. Encerrando o programa.")

if __name__ == "__main__":
    main()