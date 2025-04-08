#inventory_view.py
import tkinter as tk
from tkinter import ttk
import pandas as pd
from typing import Optional, Dict, List, Any, Tuple
import logging
import numpy as np
from datetime import datetime
import threading
from queue import Queue

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
        
        # Frame principal
        main_frame = ttk.Frame(self)
        main_frame.grid(row=0, column=0, sticky="nsew")
        main_frame.grid_rowconfigure(0, weight=1)
        main_frame.grid_columnconfigure(0, weight=1)
        
        # Treeview com scrollbars
        self._setup_treeview(main_frame)
        
        # Controles de paginação
        self._setup_pagination_controls(main_frame)
        
        # Barra de status
        self._setup_statusbar()
        
    def _setup_state(self):
        """Configura variáveis de estado"""
        self.current_data = None
        self._sort_column = 'DIFERENCA'
        self._sort_reverse = True
        self._style_tags_configured = False
        self._pending_updates = 0
        self._last_update_time = datetime.now()
        
        # Paginação
        self.page_size = 1000  # Itens por página
        self.current_page = 0
        self.total_pages = 0
        
        # Threading
        self.data_queue = Queue()
        self.loading_thread = None
        self.stop_loading = threading.Event()
        
    def _setup_treeview(self, parent):
        """Configura a Treeview com scrollbars e ordenação"""
        # Treeview
        self.tree = ttk.Treeview(
            parent,
            show="headings",
            selectmode="extended",
            height=25
        )
        
        # Scrollbars
        y_scroll = ttk.Scrollbar(parent, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=y_scroll.set)
        
        x_scroll = ttk.Scrollbar(parent, orient="horizontal", command=self.tree.xview)
        self.tree.configure(xscrollcommand=x_scroll.set)
        
        # Layout
        self.tree.grid(row=0, column=0, sticky="nsew")
        y_scroll.grid(row=0, column=1, sticky="ns")
        x_scroll.grid(row=1, column=0, sticky="ew")
        
        # Configuração das colunas
        self.columns = [
            'GTIN', 'Codigo', 'Descricao', 'Preco', 'Custo', 
            'Estoque', 'QNT_CONTADA', 'COD_BARRAS', 'Flag', 'DIFERENCA'
        ]
        
        # Tipos de coluna para ordenação
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
        
        # Configura cabeçalhos
        for col in self.columns:
            self.tree.heading(
                col, 
                text=col,
                command=lambda c=col: self.sort_by_column(c)
            )
            self.tree.column(col, width=120, anchor='center', stretch=True)
    
    def _setup_pagination_controls(self, parent):
        """Configura os controles de paginação"""
        pagination_frame = ttk.Frame(parent)
        pagination_frame.grid(row=2, column=0, columnspan=2, sticky="ew", pady=5)
        
        self.prev_btn = ttk.Button(
            pagination_frame,
            text="< Anterior",
            command=self.prev_page,
            state=tk.DISABLED
        )
        self.prev_btn.pack(side=tk.LEFT, padx=5)
        
        self.page_info = ttk.Label(pagination_frame, text="Página 0/0")
        self.page_info.pack(side=tk.LEFT, expand=True)
        
        self.next_btn = ttk.Button(
            pagination_frame,
            text="Próxima >",
            command=self.next_page,
            state=tk.DISABLED
        )
        self.next_btn.pack(side=tk.RIGHT, padx=5)
    
    def _setup_statusbar(self):
        """Configura a barra de status"""
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
    
    def display_data(self, data: pd.DataFrame):
        """Exibe os dados na Treeview com tratamento robusto"""
        try:
            if not isinstance(data, pd.DataFrame) or data.empty:
                self._update_status("Nenhum dado para exibir", rows=0)
                return
                
            start_time = datetime.now()
            self._pending_updates += 1
            
            # Configura tags de estilo apenas uma vez
            if not self._style_tags_configured:
                self._configure_style_tags()
                self._style_tags_configured = True
            
            # Carrega os dados em uma thread separada
            self._load_data_async(data, start_time)
            
        except Exception as e:
            self.logger.error(f"Erro ao exibir dados: {str(e)}", exc_info=True)
            self._update_status("Erro ao carregar dados", error=True)
            self._pending_updates = max(0, self._pending_updates - 1)
    
    def _load_data_async(self, data: pd.DataFrame, start_time: datetime):
        """Carrega os dados em uma thread separada"""
        # Para qualquer carregamento anterior
        self.stop_loading.set()
        if self.loading_thread and self.loading_thread.is_alive():
            self.loading_thread.join(timeout=1)
        
        self.stop_loading.clear()
        
        # Inicia nova thread para carregamento
        self.loading_thread = threading.Thread(
            target=self._load_data_background,
            args=(data.copy(), start_time),
            daemon=True
        )
        self.loading_thread.start()
    
    def _load_data_background(self, data: pd.DataFrame, start_time: datetime):
        """Carrega e processa os dados em background"""
        try:
            # Processa os dados (pode ser demorado)
            processed_data = self._process_data(data)
            
            # Atualiza a interface na thread principal
            self.after(0, lambda: self._update_display(processed_data, start_time))
            
        except Exception as e:
            self.logger.error(f"Erro no carregamento em background: {e}", exc_info=True)
            self.after(0, lambda: self._update_status(f"Erro: {str(e)}", error=True))
    
    def _process_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """Processa os dados para exibição"""
        # Garante tipos corretos para as colunas numéricas
        numeric_cols = ['Preco', 'Custo', 'Estoque', 'QNT_CONTADA', 'DIFERENCA']
        for col in numeric_cols:
            if col in data.columns:
                data[col] = pd.to_numeric(data[col], errors='coerce').fillna(0)
        
        # Calcula diferença se necessário
        if 'DIFERENCA' not in data.columns and 'Estoque' in data.columns and 'QNT_CONTADA' in data.columns:
            data['DIFERENCA'] = data['QNT_CONTADA'] - data['Estoque']
        
        # Ordena os dados
        if self._sort_column in data.columns:
            data = data.sort_values(
                self._sort_column, 
                ascending=not self._sort_reverse
            )
        
        return data
    
    def _update_display(self, data: pd.DataFrame, start_time: datetime):
        """Atualiza a exibição com os dados processados"""
        try:
            self.current_data = data
            self.total_pages = (len(data) // self.page_size) + (1 if len(data) % self.page_size else 0)
            self.current_page = 0
            
            # Atualiza controles de paginação
            self._update_pagination_controls()
            
            # Mostra a primeira página
            self._show_current_page()
            
            # Atualiza status
            elapsed = (datetime.now() - start_time).total_seconds()
            self._update_status(
                f"Carregado em {elapsed:.2f}s",
                rows=len(data),
                sort_col=self._sort_column
            )
            
        except Exception as e:
            self.logger.error(f"Erro ao atualizar exibição: {e}", exc_info=True)
            self._update_status("Erro ao exibir dados", error=True)
        finally:
            self._pending_updates = max(0, self._pending_updates - 1)
    
    def _show_current_page(self):
        """Exibe os itens da página atual"""
        if self.current_data is None:
            return
            
        # Limpa a treeview
        self.tree.delete(*self.tree.get_children())
        
        # Calcula os índices da página atual
        start_idx = self.current_page * self.page_size
        end_idx = min(start_idx + self.page_size, len(self.current_data))
        
        # Prepara os dados da página atual
        page_data = self.current_data.iloc[start_idx:end_idx]
        display_data = self._prepare_display_data(page_data)
        
        # Insere os dados na treeview
        self._insert_data(display_data)
        
        # Atualiza informações de paginação
        self._update_page_info()
    
    def _prepare_display_data(self, data: pd.DataFrame) -> List[Tuple[List[str], List[str]]]:
        """Prepara os dados para exibição"""
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
    
    def _insert_data(self, data: List[Tuple[List[str], List[str]]]):
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
    
    def _update_pagination_controls(self):
        """Atualiza o estado dos controles de paginação"""
        if self.total_pages <= 1:
            self.prev_btn.config(state=tk.DISABLED)
            self.next_btn.config(state=tk.DISABLED)
        else:
            self.prev_btn.config(state=tk.NORMAL if self.current_page > 0 else tk.DISABLED)
            self.next_btn.config(state=tk.NORMAL if self.current_page < self.total_pages - 1 else tk.DISABLED)
    
    def _update_page_info(self):
        """Atualiza as informações de paginação exibidas"""
        if self.current_data is None:
            self.page_info.config(text="Página 0/0")
            return
            
        start_item = self.current_page * self.page_size + 1
        end_item = min((self.current_page + 1) * self.page_size, len(self.current_data))
        
        self.page_info.config(
            text=f"Página {self.current_page + 1}/{self.total_pages} "
                 f"(Itens {start_item}-{end_item} de {len(self.current_data)})"
        )
    
    def prev_page(self):
        """Navega para a página anterior"""
        if self.current_page > 0:
            self.current_page -= 1
            self._show_current_page()
            self._update_pagination_controls()
    
    def next_page(self):
        """Navega para a próxima página"""
        if self.current_page < self.total_pages - 1:
            self.current_page += 1
            self._show_current_page()
            self._update_pagination_controls()
    
    def sort_by_column(self, column: str, reverse: bool = None):
        """Ordena os dados por uma coluna específica"""
        try:
            if column not in self.columns:
                self.logger.warning(f"Tentativa de ordenar por coluna inválida: {column}")
                return
                
            if reverse is None:
                # Alternar ordem se não for especificada
                reverse = not self._sort_reverse if column == self._sort_column else False
                
            self._sort_column = column
            self._sort_reverse = reverse
            
            # Se já temos dados carregados, reordenar
            if self.current_data is not None:
                self.display_data(self.current_data)
                
            # Atualizar status
            self.sort_var.set(f"Ordenado por: {column} {'↓' if reverse else '↑'}")
            
        except Exception as e:
            self.logger.error(f"Erro ao ordenar por {column}: {str(e)}")
            self._update_status(f"Erro ao ordenar: {column}", error=True)
    
    def _configure_style_tags(self):
        """Configura todos os tags de estilo uma única vez"""
        self.tree.tag_configure('alerta', background='#ffcccc')
        self.tree.tag_configure('excesso', background='#ff0000', foreground='white')
        self.tree.tag_configure('faltante', background='#fffacd')
        self.tree.tag_configure('neutral', background='#f0f0f0')
    
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
        self.current_page = 0
        self.total_pages = 0
        self._update_status("Pronto", rows=0)
        self.sort_var.set("")
        self._update_pagination_controls()
        self._update_page_info()
    
    def refresh_style(self):
        """Atualiza os estilos da treeview"""
        self._style_tags_configured = False
        if self.current_data is not None:
            self.display_data(self.current_data)
    
    def is_updating(self) -> bool:
        """Verifica se há atualizações em andamento"""
        return self._pending_updates > 0