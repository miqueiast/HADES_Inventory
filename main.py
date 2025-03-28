#!/usr/bin/env python3
import tkinter as tk
import logging
from ui.main_window import MainWindow
from utils.logger import setup_logger

def main():
    try:
        app = MainWindow()  # Removido o parâmetro root
        app.mainloop()
    except Exception as e:
        logging.critical(f"Falha na aplicação: {e}", exc_info=True)
        tk.messagebox.showerror("Erro Fatal", f"Ocorreu um erro crítico:\n{str(e)}")

if __name__ == "__main__":
    main()