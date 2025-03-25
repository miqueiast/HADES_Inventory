import os
import time
import pandas as pd
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import traceback

class ExcelHandler(FileSystemEventHandler):
    def __init__(self, pasta_dados_processados):
        self.pasta_dados_processados = pasta_dados_processados
        self.df_consolidado = pd.DataFrame()
        
    def on_created(self, event):
        if not event.is_directory and event.src_path.lower().endswith('.xlsx'):
            print(f"\nNovo arquivo detectado: {event.src_path}")
            try:
                self.df_consolidado = processar_arquivo_excel(event.src_path, self.df_consolidado)
                salvar_consolidado(self.df_consolidado, self.pasta_dados_processados)
            except Exception as e:
                print(f"Erro ao processar arquivo: {e}\n{traceback.format_exc()}")

def criar_inventario():
    # Solicita os dados ao usuário
    nome_inventario = input("Digite o nome do inventário: ").strip()
    nome_loja = input("Digite o nome da loja: ").strip()
    nome_pessoa = input("Digite o nome da pessoa: ").strip()
    
    # Define o caminho principal da pasta "Inventário"
    pasta_principal = "Inventário"
    caminho_inventario = os.path.join(pasta_principal, nome_inventario)
    pasta_dados_processados = os.path.join(caminho_inventario, "dados_processados")
    
    try:
        # Verifica e cria as pastas necessárias
        os.makedirs(pasta_principal, exist_ok=True)
        
        if os.path.exists(caminho_inventario):
            print(f"\nErro: A pasta '{nome_inventario}' já existe!")
            return
        
        os.makedirs(caminho_inventario)
        os.makedirs(os.path.join(caminho_inventario, "txt"))
        os.makedirs(pasta_dados_processados)
        
        # Cria arquivo de informações
        with open(os.path.join(caminho_inventario, "informacoes.txt"), 'w', encoding='utf-8') as f:
            f.write(f"Nome do Inventário: {nome_inventario}\n")
            f.write(f"Nome da Loja: {nome_loja}\n")
            f.write(f"Responsável: {nome_pessoa}\n")
        
        # Cria pasta na área de trabalho
        desktop = os.path.join(os.path.expanduser("~"), "Desktop")
        pasta_contagem = os.path.join(desktop, f"{nome_inventario} - Inserir Dados de Contagem")
        os.makedirs(pasta_contagem, exist_ok=True)
        
        print(f"\nInventário '{nome_inventario}' criado com sucesso!")
        print(f"Pasta de contagem criada em: {pasta_contagem}")
        
        # Inicia o monitoramento
        observar_pasta(pasta_contagem, pasta_dados_processados)
        
    except Exception as e:
        print(f"\nErro ao criar inventário: {e}\n{traceback.format_exc()}")

def observar_pasta(pasta_contagem, pasta_dados_processados):
    print(f"\nMonitorando pasta: {pasta_contagem}")
    print("Pressione Ctrl+C para parar o monitoramento...")
    
    event_handler = ExcelHandler(pasta_dados_processados)
    observer = Observer()
    observer.schedule(event_handler, pasta_contagem, recursive=False)
    observer.start()
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        print("\nMonitoramento encerrado pelo usuário")
    finally:
        observer.join()

def processar_arquivo_excel(caminho_arquivo, df_consolidado):
    try:
        # Lê o arquivo Excel com tratamento de tipos
        df = pd.read_excel(caminho_arquivo).astype(str)
        
        # Verifica colunas obrigatórias
        required_cols = ["LOJA KEY", "OPERADOR", "ENDEREÇO", "CÓD. BARRAS", "QNT. CONTADA"]
        missing_cols = [col for col in required_cols if col not in df.columns]
        
        if missing_cols:
            raise ValueError(f"Colunas faltando: {', '.join(missing_cols)}")
        
        # Converte para os tipos corretos
        df["LOJA KEY"] = pd.to_numeric(df["LOJA KEY"], errors='coerce').fillna(0).astype(int)
        df["ENDEREÇO"] = pd.to_numeric(df["ENDEREÇO"], errors='coerce').fillna(0).astype(int)
        df["CÓD. BARRAS"] = pd.to_numeric(df["CÓD. BARRAS"], errors='coerce').fillna(0).astype(int)
        df["QNT. CONTADA"] = pd.to_numeric(df["QNT. CONTADA"], errors='coerce').fillna(0.0).astype(float)
        df["OPERADOR"] = df["OPERADOR"].astype(str)
        
        # Processa os dados
        df_agrupado = (
            df.groupby(["LOJA KEY", "ENDEREÇO", "CÓD. BARRAS"], as_index=False)
            .agg({
                "QNT. CONTADA": "sum",
                "OPERADOR": lambda x: "/".join(sorted(set(x.dropna().astype(str))))
            })
        )
        
        # Combina com o consolidado
        if df_consolidado.empty:
            df_consolidado = df_agrupado
        else:
            df_consolidado = pd.concat([df_consolidado, df_agrupado])
            df_consolidado = (
                df_consolidado.groupby(["LOJA KEY", "ENDEREÇO", "CÓD. BARRAS"], as_index=False)
                .agg({
                    "QNT. CONTADA": "sum",
                    "OPERADOR": lambda x: "/".join(sorted(set(x.dropna().astype(str))))
                })
            )
        
        print(f"Arquivo processado: {os.path.basename(caminho_arquivo)}")
        return df_consolidado
        
    except Exception as e:
        raise ValueError(f"Erro no processamento: {str(e)}")

def salvar_consolidado(df_consolidado, pasta_dados_processados):
    try:
        if df_consolidado.empty:
            print("Nenhum dado para salvar")
            return
            
        caminho_csv = os.path.join(pasta_dados_processados, "dados_consolidados.csv")
        df_consolidado.to_csv(caminho_csv, index=False, sep=";")
        print(f"Dados consolidados salvos em: {caminho_csv}")
    except Exception as e:
        raise ValueError(f"Erro ao salvar consolidado: {str(e)}")

if __name__ == "__main__":
    print("=== SISTEMA DE INVENTÁRIO ===")
    criar_inventario()