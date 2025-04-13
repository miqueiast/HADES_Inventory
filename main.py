#main.py
import tkinter as tk
import logging 
from ui.main_window import MainWindow
from utils.logger import setup_logger

def main():
    """
    Função principal que inicia a aplicação HADES.
    
    Responsabilidades:
    - Criar e iniciar a janela principal da aplicação
    - Gerenciar erros não tratados e exibir mensagens ao usuário
    - Garantir que erros críticos sejam registrados adequadamente
    """
    try:
        # Cria a instância da janela principal
        # (Observação: o comentário sobre "Removido o parâmetro root" sugere uma refatoração recente)
        app = MainWindow()  # Removido o parâmetro root
        
        # Inicia o loop principal de eventos da interface gráfica
        app.mainloop()
        
    except Exception as e:
        # Registra erros críticos no log com traceback completo
        logging.critical(f"Falha na aplicação: {e}", exc_info=True)
        
        # Exibe uma mensagem de erro amigável para o usuário
        tk.messagebox.showerror("Erro Fatal", f"Ocorreu um erro crítico:\n{str(e)}")

# Ponto de entrada padrão para scripts Python
if __name__ == "__main__":
    main()