import tkinter as tk
from tkinter import ttk, filedialog
from pathlib import Path
from typing import Optional

class ImportDialog(tk.Toplevel):
    def __init__(self, parent):
        super().__init__(parent)
        self.title("Incluir Novos Dados")
        self.geometry("500x300")
        self.resizable(False, False)
        
        self.file_path: Optional[str] = None
        
        self.create_widgets()
        self.center_on_parent()
        
    def create_widgets(self):
        """Cria os componentes da interface"""
        # Frame principal
        main_frame = ttk.Frame(self, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Título
        ttk.Label(
            main_frame, 
            text="Selecione o arquivo Excel com os dados de contagem",
            font=('Helvetica', 10, 'bold')
        ).pack(pady=(0, 20))
        
        # Área de informação
        info_frame = ttk.LabelFrame(main_frame, text="Informações", padding="10")
        info_frame.pack(fill=tk.X, pady=5)
        
        ttk.Label(
            info_frame,
            text="O arquivo deve conter as seguintes colunas nesta ordem:",
            wraplength=400
        ).pack(anchor=tk.W)
        
        columns_info = ttk.Label(
            info_frame,
            text="LOJA KEY, OPERADOR, ENDERE€O, CàD. BARRAS, QNT. CONTADA",
            font=('Courier', 9),
            foreground="blue"
        )
        columns_info.pack(anchor=tk.W, pady=5)
        
        # Botão de seleção de arquivo
        file_frame = ttk.Frame(main_frame)
        file_frame.pack(fill=tk.X, pady=15)
        
        ttk.Button(
            file_frame,
            text="Selecionar Arquivo Excel",
            command=self.select_file
        ).pack(side=tk.LEFT)
        
        self.file_label = ttk.Label(
            file_frame,
            text="Nenhum arquivo selecionado",
            foreground="gray"
        )
        self.file_label.pack(side=tk.LEFT, padx=10)
        
        # Botões de ação
        button_frame = ttk.Frame(main_frame)
        button_frame.pack(fill=tk.X, pady=(20, 0))
        
        ttk.Button(
            button_frame,
            text="Cancelar",
            command=self.cancel
        ).pack(side=tk.RIGHT, padx=5)
        
        self.import_btn = ttk.Button(
            button_frame,
            text="Importar",
            state=tk.DISABLED,
            command=self.confirm
        )
        self.import_btn.pack(side=tk.RIGHT)
        
    def select_file(self):
        """Abre diálogo para selecionar arquivo"""
        file_types = [
            ("Arquivos Excel", "*.xlsx *.xls"),
            ("Todos os arquivos", "*.*")
        ]
        
        file_path = filedialog.askopenfilename(
            title="Selecione o arquivo de contagem",
            filetypes=file_types
        )
        
        if file_path:
            self.file_path = file_path
            self.file_label.config(
                text=Path(file_path).name,
                foreground="black"
            )
            self.import_btn.config(state=tk.NORMAL)
        else:
            self.file_label.config(
                text="Nenhum arquivo selecionado",
                foreground="gray"
            )
            self.import_btn.config(state=tk.DISABLED)
    
    def center_on_parent(self):
        """Centraliza a janela em relação à janela pai"""
        self.update_idletasks()
        parent_x = self.master.winfo_x()
        parent_y = self.master.winfo_y()
        parent_width = self.master.winfo_width()
        parent_height = self.master.winfo_height()
        
        width = self.winfo_width()
        height = self.winfo_height()
        
        x = parent_x + (parent_width // 2) - (width // 2)
        y = parent_y + (parent_height // 2) - (height // 2)
        
        self.geometry(f"+{x}+{y}")
    
    def cancel(self):
        """Fecha o diálogo sem confirmar"""
        self.file_path = None
        self.destroy()
    
    def confirm(self):
        """Fecha o diálogo confirmando a importação"""
        self.destroy()
    
    def show(self):
        """Exibe o diálogo e espera pela resposta"""
        self.grab_set()
        self.wait_window(self)
        return self.file_path