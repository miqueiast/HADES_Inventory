import tkinter as tk
from tkinter import ttk
import pandas as pd
from typing import Optional, Dict, List, Any
import logging
import numpy as np
from datetime import datetime

class InventoryView(ttk.Frame):
    def __init__(self, master, **kwargs):
        super().__init__(master, **kwargs)
        self.logger = logging.getLogger(f"{__name__}.InventoryView")
        self._setup_ui()
        self._setup_state()
        
    def _setup_ui(self):
        """Configura todos os componentes da interface"""
        self.grid_rowconfigure(0, weight=1)
        self.grid_columnconfigure(0, weight=1)
        
        # Configuração da Treeview com scrollbars duplos
        self._setup_treeview()
        self._setup_statusbar()
        
    def _setup_state(self):
        """Configura variáveis de estado"""
        self.current_data = None
        self._sort_column = 'DIFERENCA'
        self._sort_reverse = True
        self._style_tags_configured = False
        self._pending_updates = 0
        self._last_update_time = datetime.now()
        
    def _setup_treeview(self):
        """Configura a Treeview com scrollbars e ordenação"""
        # Frame container com grid
        self.tree_frame = ttk.Frame(self)
        self.tree_frame.grid(row=0, column=0, sticky="nsew", padx=2, pady=2)
        self.tree_frame.grid_rowconfigure(0, weight=1)
        self.tree_frame.grid_columnconfigure(0, weight=1)
        
        # Treeview com performance optimizada
        self.tree = ttk.Treeview(
            self.tree_frame,
            show="headings",
            selectmode="extended",
            height=25  # Limite inicial para performance
        )
        
        # Scrollbars
        y_scroll = ttk.Scrollbar(
            self.tree_frame, 
            orient="vertical", 
            command=self.tree.yview
        )
        x_scroll = ttk.Scrollbar(
            self.tree_frame, 
            orient="horizontal", 
            command=self.tree.xview
        )
        
        self.tree.configure(
            yscrollcommand=y_scroll.set,
            xscrollcommand=x_scroll.set
        )
        
        # Layout usando grid
        self.tree.grid(row=0, column=0, sticky="nsew")
        y_scroll.grid(row=0, column=1, sticky="ns")
        x_scroll.grid(row=1, column=0, sticky="ew")
        
        # Configuração das colunas
        self.columns = [
            'GTIN', 'Codigo', 'Descricao', 'Preco', 'Custo', 
            'Estoque', 'QNT_CONTADA', 'COD_BARRAS', 'Flag', 'DIFERENCA'
        ]
        
        # Definir colunas com tipos para ordenação
        self.column_types = {
            'GTIN': 'str',
            'Codigo': 'str',
            'Descricao': 'str',
            'Preco': 'float',
            'Custo': 'float',
            'Estoque': 'float',
            'QNT_CONTADA': 'float',
            'COD_BARRAS': 'str',
            'Flag': 'str',
            'DIFERENCA': 'float'
        }
        
        self.tree["columns"] = self.columns
        
        # Configura cabeçalhos com suporte a ordenação
        for col in self.columns:
            self.tree.heading(
                col, 
                text=col,
                command=lambda c=col: self.sort_by_column(c)
            )
            self.tree.column(col, width=120, anchor='center', stretch=True)
    
    def _setup_statusbar(self):
        """Configura a barra de status com mais informações"""
        self.status_frame = ttk.Frame(self)
        self.status_frame.grid(row=1, column=0, sticky="ew")
        
        self.status_var = tk.StringVar(value="Pronto")
        self.rows_var = tk.StringVar()
        self.sort_var = tk.StringVar()
        
        ttk.Label(
            self.status_frame,
            textvariable=self.status_var,
            relief=tk.SUNKEN,
            anchor=tk.W,
            width=40
        ).pack(side=tk.LEFT, fill=tk.X, expand=True)
        
        ttk.Label(
            self.status_frame,
            textvariable=self.rows_var,
            relief=tk.SUNKEN,
            anchor=tk.CENTER,
            width=15
        ).pack(side=tk.LEFT, padx=2)
        
        ttk.Label(
            self.status_frame,
            textvariable=self.sort_var,
            relief=tk.SUNKEN,
            anchor=tk.E,
            width=25
        ).pack(side=tk.LEFT)
    
    def display_data(self, data: pd.DataFrame, max_rows: int = 5000):
        """Exibe os dados na Treeview com tratamento robusto"""
        try:
            if not isinstance(data, pd.DataFrame) or data.empty:
                self._update_status("Nenhum dado para exibir", rows=0)
                return
                
            start_time = datetime.now()
            self._pending_updates += 1
            
            # Limitar tamanho para performance
            if len(data) > max_rows:
                data = data.head(max_rows)
                self.logger.warning(f"Dados truncados para {max_rows} registros por performance")
            
            self.current_data = data.copy()
            
            # Configura tags de estilo apenas uma vez
            if not self._style_tags_configured:
                self._configure_style_tags()
                self._style_tags_configured = True
            
            # Limpa a treeview de forma eficiente
            self.tree.delete(*self.tree.get_children())
            
            # Converte para lista de tuplas para melhor performance
            display_data = self._prepare_display_data(data)
            
            # Insere os dados em blocos para melhor performance
            self._insert_data(display_data)
            
            # Ordena pelos critérios atuais
            self.sort_by_column(self._sort_column, self._sort_reverse)
            
            # Atualiza status
            elapsed = (datetime.now() - start_time).total_seconds()
            self._update_status(
                f"Carregado em {elapsed:.2f}s",
                rows=len(data),
                sort_col=self._sort_column
            )
            
        except Exception as e:
            self.logger.error(f"Erro ao exibir dados: {str(e)}", exc_info=True)
            self._update_status("Erro ao carregar dados", error=True)
        finally:
            self._pending_updates = max(0, self._pending_updates - 1)
    
    def _prepare_display_data(self, data: pd.DataFrame) -> List[tuple]:
        """Prepara os dados para exibição de forma otimizada"""
        display_data = []
        for _, row in data.iterrows():
            try:
                values = []
                for col in self.columns:
                    if col in data.columns:
                        val = row[col]
                        # Formatação especial para valores numéricos
                        if pd.api.types.is_float(val) and not pd.isna(val):
                            if col in ['Preco', 'Custo']:
                                values.append(f"R$ {val:,.2f}")
                            elif col == 'DIFERENCA':
                                values.append(f"{val:+,.0f}")
                            else:
                                values.append(f"{val:,.0f}")
                        else:
                            values.append(str(val) if not pd.isna(val) else "")
                    else:
                        values.append("")
                
                # Tags para formatação condicional
                tags = []
                if 'Flag' in row and str(row['Flag']).upper() == 'ALERTA!':
                    tags.append('alerta')
                if 'DIFERENCA' in row and pd.notna(row['DIFERENCA']):
                    if row['DIFERENCA'] > 0:
                        tags.append('excesso')
                    elif row['DIFERENCA'] < 0:
                        tags.append('faltante')
                
                display_data.append((values, tags))
            except Exception as e:
                self.logger.warning(f"Erro ao processar linha: {str(e)}")
                continue
                
        return display_data
    
    def _insert_data(self, data: List[tuple]):
        """Insere dados na treeview de forma otimizada"""
        # Limitar atualizações visuais durante inserção
        self.tree.config(displaycolumns=self.columns)
        self.tree.configure(displaycolumns=self.columns)
        
        # Desabilitar redesenho durante inserção
        self.tree.bind('<<TreeviewOpen>>', lambda e: 'break')
        
        try:
            # Inserir em blocos para melhor performance
            block_size = 100
            for i in range(0, len(data), block_size):
                block = data[i:i+block_size]
                for values, tags in block:
                    self.tree.insert("", "end", values=values, tags=tags)
        finally:
            # Restaurar comportamento normal
            self.tree.unbind('<<TreeviewOpen>>')
    
    def _configure_style_tags(self):
        """Configura todos os tags de estilo uma única vez"""
        self.tree.tag_configure('alerta', background='#ffcccc')
        self.tree.tag_configure('excesso', background='#ff0000', foreground='white')
        self.tree.tag_configure('faltante', background='#fffacd')
        self.tree.tag_configure('neutral', background='#f0f0f0')
    
    def sort_by_column(self, column: str, reverse: bool = None):
        """Ordena a treeview por uma coluna específica"""
        try:
            if column not in self.columns:
                self.logger.warning(f"Tentativa de ordenar por coluna inválida: {column}")
                return
                
            if reverse is None:
                # Alternar ordem se não for especificada
                reverse = not self._sort_reverse if column == self._sort_column else False
                
            self._sort_column = column
            self._sort_reverse = reverse
            
            # Obter todos os itens da treeview
            items = [(self.tree.set(child, column), child) 
                    for child in self.tree.get_children('')]
            
            # Converter para o tipo apropriado
            if self.column_types.get(column) == 'float':
                items = [(float(val) if val and val.replace('.','',1).isdigit() else 0, child) 
                        for val, child in items]
            elif self.column_types.get(column) == 'int':
                items = [(int(val) if val and val.isdigit() else 0, child) 
                        for val, child in items]
            
            # Ordenar os itens
            items.sort(reverse=reverse)
            
            # Reorganizar os itens na treeview
            for index, (val, child) in enumerate(items):
                self.tree.move(child, '', index)
                
            # Atualizar status
            self.sort_var.set(f"Ordenado por: {column} {'↓' if reverse else '↑'}")
            
        except Exception as e:
            self.logger.error(f"Erro ao ordenar por {column}: {str(e)}")
            self._update_status(f"Erro ao ordenar: {column}", error=True)
    
    def _update_status(self, message: str, rows: int = None, 
                      sort_col: str = None, error: bool = False):
        """Atualiza a barra de status com informações detalhadas"""
        self.status_var.set(message)
        if rows is not None:
            self.rows_var.set(f"Registros: {rows:,}")
        if sort_col is not None:
            self.sort_var.set(f"Ordenado por: {sort_col} {'↓' if self._sort_reverse else '↑'}")
        
        if error:
            self.status_frame.configure(style='Error.TFrame')
            self.after(3000, lambda: self.status_frame.configure(style='TFrame'))
    
    def get_selected_rows(self) -> List[Dict[str, Any]]:
        """Retorna todas as linhas selecionadas como dicionários"""
        try:
            selected_items = self.tree.selection()
            if not selected_items:
                return []
                
            return [self._item_to_dict(item) for item in selected_items]
        except Exception as e:
            self.logger.error(f"Erro ao obter seleção: {str(e)}")
            return []
    
    def _item_to_dict(self, item_id: str) -> Dict[str, Any]:
        """Converte um item da treeview para dicionário"""
        item = self.tree.item(item_id)
        return {
            'id': item_id,
            'values': dict(zip(self.columns, item['values'])),
            'tags': item['tags']
        }
    
    def clear(self):
        """Limpa todos os dados da visualização"""
        self.tree.delete(*self.tree.get_children())
        self.current_data = None
        self._update_status("Pronto", rows=0)
        self.sort_var.set("")
    
    def refresh_style(self):
        """Atualiza os estilos da treeview"""
        self._style_tags_configured = False
        if self.current_data is not None:
            self.display_data(self.current_data)
    
    def is_updating(self) -> bool:
        """Verifica se há atualizações em andamento"""
        return self._pending_updates > 0