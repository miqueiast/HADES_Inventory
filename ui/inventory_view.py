import tkinter as tk
from tkinter import ttk
import pandas as pd
from typing import Optional
from pandastable import Table, TableModel

class InventoryView(ttk.Frame):
    def __init__(self, master, **kwargs):
        super().__init__(master, **kwargs)
        self.configure(style='Card.TFrame')
        
        # Cria frame para a tabela
        self.table_frame = ttk.Frame(self)
        self.table_frame.pack(fill=tk.BOTH, expand=True)
        
        # Cria pandastable
        self.table = None
        self.current_data = None
        
        # Barra de status
        self.status_var = tk.StringVar()
        self.status_bar = ttk.Label(self, textvariable=self.status_var, 
                                   relief=tk.SUNKEN, anchor=tk.W)
        self.status_bar.pack(fill=tk.X)
        
    def display_data(self, data: pd.DataFrame):
        """Exibe dados na tabela"""
        self.current_data = data
        
        # Destroi tabela existente
        for widget in self.table_frame.winfo_children():
            widget.destroy()
            
        # Cria nova tabela
        f = tk.Frame(self.table_frame)
        f.pack(fill=tk.BOTH, expand=True)
        
        self.table = Table(
            f, 
            dataframe=data,
            showtoolbar=True,
            showstatusbar=True,
            editable=False,
            enable_menus=True
        )
        
        # Configura cores para linhas com diferença
        self.setup_colors()
        
        self.table.show()
        self.status_var.set(f"Exibindo {len(data)} registros")
        
    def setup_colors(self):
        """Configura cores para linhas com diferença"""
        if not self.table or not self.current_data:
            return
            
        model = self.table.model
        
        # Verifica se temos coluna de diferença
        if 'DIFERENCA' in self.current_data.columns:
            for i, row in self.current_data.iterrows():
                if row['DIFERENCA'] > 0:
                    # Linha vermelha para diferenças positivas
                    model.setRowColors(i, 'white', '#ffcccc')
                elif row['DIFERENCA'] < 0:
                    # Linha amarela para diferenças negativas
                    model.setRowColors(i, 'black', '#ffffcc')