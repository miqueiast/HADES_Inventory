import tkinter as tk
from tkinter import ttk
import pandas as pd
from typing import Optional, Dict
from pandastable import Table, TableModel

class InventoryView(ttk.Frame):
    def __init__(self, master, **kwargs):
        super().__init__(master, **kwargs)
        self.grid_rowconfigure(0, weight=1)
        self.grid_columnconfigure(0, weight=1)
        
        # Frame container para a tabela
        self.table_container = ttk.Frame(self)
        self.table_container.grid(row=0, column=0, sticky="nsew")
        self.table_container.grid_rowconfigure(0, weight=1)
        self.table_container.grid_columnconfigure(0, weight=1)
        
        # Frame para o pandastable
        self.table_frame = tk.Frame(self.table_container)
        self.table_frame.grid(row=0, column=0, sticky="nsew")
        
        # Barra de status
        self.status_frame = ttk.Frame(self)
        self.status_frame.grid(row=1, column=0, sticky="ew")
        
        self.status_var = tk.StringVar(value="Pronto")
        self.status_label = ttk.Label(
            self.status_frame,
            textvariable=self.status_var,
            relief=tk.SUNKEN,
            anchor=tk.W
        )
        self.status_label.pack(fill=tk.X, ipady=2)
        
        # Variáveis de estado
        self.table = None
        self.current_data = None
    
    def display_data(self, data: pd.DataFrame):
        """Exibe os dados na tabela"""
        if not isinstance(data, pd.DataFrame) or data.empty:
            self.status_var.set("Nenhum dado para exibir")
            return
            
        self.current_data = data
        
        # Limpa a tabela existente
        for widget in self.table_frame.winfo_children():
            widget.destroy()
        
        # Cria nova tabela
        self.table = Table(
            self.table_frame,
            dataframe=data,
            showtoolbar=True,
            showstatusbar=False,
            editable=True,
            enable_menus=True,
            width=800,
            height=400
        )
        self.table.show()
        
        # Configura cores e formatação
        self._apply_styling()
        
        # Atualiza status
        self.status_var.set(f"Exibindo {len(data)} registros | {len(data.columns)} colunas")
    
    def _apply_styling(self):
        """Aplica formatação condicional à tabela"""
        if not hasattr(self, 'table') or self.table is None:
            return
            
        if not hasattr(self, 'current_data') or self.current_data is None:
            return
            
        if not isinstance(self.current_data, pd.DataFrame) or self.current_data.empty:
            return
            
        model = self.table.model
        
        # Verifica se temos coluna de diferença
        if 'DIFERENCA' in self.current_data.columns:
            for i, row in self.current_data.iterrows():
                try:
                    diff = row['DIFERENCA']
                    
                    if pd.notna(diff):
                        if diff > 0:
                            # Excesso - fundo vermelho (texto branco)
                            model.setRowColors(i, 'white', '#ff0000')
                        elif diff < 0:
                            # Faltante - fundo amarelo (texto preto)
                            model.setRowColors(i, 'black', '#ffff00')
                except Exception:
                    continue
        
        # Formata colunas numéricas
        numeric_cols = ['Estoque', 'QNT_CONTADA', 'DIFERENCA']
        for col in numeric_cols:
            if col in self.current_data.columns:
                try:
                    col_idx = self.current_data.columns.get_loc(col)
                    for i in range(len(self.current_data)):
                        val = self.current_data.iloc[i][col]
                        if pd.notna(val):
                            model.setValueAt(f"{val:,.0f}", i, col_idx)
                except Exception:
                    continue
    
    def get_selected_row(self) -> Optional[Dict]:
        """Retorna a linha atualmente selecionada"""
        if not hasattr(self, 'table') or self.table is None:
            return None
            
        if not hasattr(self, 'current_data') or self.current_data is None:
            return None
            
        try:
            row = self.table.getSelectedRow()
            if row >= 0 and isinstance(self.current_data, pd.DataFrame):
                return self.current_data.iloc[row].to_dict()
            return None
        except Exception:
            return None