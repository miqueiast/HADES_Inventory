import tkinter as tk
from tkinter import messagebox
import logging
from ui.main_window import MainWindow

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
        app = MainWindow()
        
        # Inicia o loop principal de eventos da interface gráfica
        app.mainloop()
        
    except Exception as e:
        # Registra erros críticos no log com traceback completo
        logging.critical(f"Falha crítica na aplicação: {e}", exc_info=True)
        
        # Exibe uma mensagem de erro amigável para o usuário
        # Criar uma janela raiz temporária para mostrar o erro se a principal falhar na inicialização
        root = tk.Tk()
        root.withdraw()
        messagebox.showerror("Erro Fatal", f"Ocorreu um erro crítico e a aplicação precisa ser fechada:\n\n{str(e)}\n\nVerifique o arquivo 'inventario_hades.log' para detalhes.")
        root.destroy()

if __name__ == "__main__":
    # É uma boa prática configurar o logging básico aqui,
    # caso a MainWindow falhe antes de configurar seu próprio logger.
    logging.basicConfig(level=logging.INFO, 
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        handlers=[logging.FileHandler("inventario_hades.log"), logging.StreamHandler()])
    main()