import requests
import json

# --- 1. CONFIGURAÇÕES ---

# O endereço da sua API online no Render
API_BASE_URL = "https://api-minipreco-inventario-hades.onrender.com"

# COLE AQUI O SEU TOKEN EXCLUSIVO QUE ESTÁ NO ARQUIVO settings.py DA SUA API
TOKEN_EXCLUSIVO_GET = "token_exclusivo_da_sua_maquina_idealmente_de_env_var"


# --- 2. FUNÇÃO PARA BUSCAR E EXIBIR OS DADOS ---

def verificar_dados_na_api(api_url: str, token: str):
    """
    Busca todos os dados de inventário na API e os exibe no terminal.
    """
    get_endpoint = f"{api_url}/inventario/dados"
    
    # O cabeçalho de autorização usa o token exclusivo
    headers = {
        "Authorization": f"Bearer {token}"
    }

    print(f"Buscando dados salvos na API em {get_endpoint}...")
    
    # Verifica se o token de exemplo foi substituído
    if "COLE_AQUI" in token:
        print("\n!!! ATENÇÃO !!!")
        print("Você precisa editar o script e substituir o valor da variável 'TOKEN_EXCLUSIVO_GET' pelo seu token real.")
        return

    try:
        # A requisição para buscar dados é do tipo GET
        response = requests.get(get_endpoint, headers=headers)

        # Se a resposta for 200 OK, os dados foram recebidos
        if response.status_code == 200:
            resultado = response.json()
            total_registros = resultado.get("total_registros", 0)
            dados = resultado.get("dados", [])

            print("\n--- SUCESSO! Dados recebidos da API. ---")
            print(f"Total de registros encontrados: {total_registros}\n")

            if not dados:
                print("Nenhum dado de inventário foi encontrado no servidor.")
            else:
                # Itera sobre cada registro e o exibe de forma formatada
                for i, registro in enumerate(dados):
                    print(f"--- Registro #{i + 1} ---")
                    # json.dumps com indent=2 é uma ótima forma de "embelezar" a saída
                    print(json.dumps(registro, indent=2, ensure_ascii=False))
                    print("-" * 20)

        elif response.status_code == 401:
            print("\n!!! ERRO DE AUTORIZAÇÃO (401) !!!")
            print("O token fornecido é inválido. Verifique se você copiou o valor correto do seu arquivo settings.py.")
            print(f"Resposta da API: {response.text}")
        else:
            print(f"\n!!! ERRO AO BUSCAR DADOS. Status: {response.status_code}")
            print(f"Resposta da API: {response.text}")

    except requests.exceptions.RequestException as e:
        print(f"\n!!! Erro de conexão: {e}")


# --- 3. EXECUÇÃO DO SCRIPT ---

if __name__ == "__main__":
    verificar_dados_na_api(API_BASE_URL, TOKEN_EXCLUSIVO_GET)