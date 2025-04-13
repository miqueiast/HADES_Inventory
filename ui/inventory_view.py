import tkinter as tk
from tkinter import ttk
import pandas as pd
from typing import Optional, Dict, List, Any, Tuple, Callable
import logging
from datetime import datetime
import threading
from queue import Queue
from pathlib import Path
from dataclasses import dataclass
from enum import Enum, auto

# Tipos de dados para melhor organização
class SortDirection(Enum):
    ASCENDING = auto()
    DESCENDING = auto()

@dataclass
class DisplayColumn:
    name: str
    dtype: str
    width: int = 120
    anchor: str = 'center'
    stretch: bool = True

class InventoryView(ttk.Frame):
    """Visualização avançada de dados de inventário com paginação, ordenação e formatação condicional."""
    
    # Configurações padrão
    DEFAULT_PAGE_SIZE = 1000
    DEFAULT_SORT_COLUMN = 'DIFERENCA'
    DEFAULT_SORT_DIRECTION = SortDirection.DESCENDING
    
    def __init__(self, master: tk.Misc, **kwargs):
        """Inicializa a visualização de inventário.
        
        Args:
            master: Widget pai
            **kwargs: Argumentos adicionais para ttk.Frame
        """
        super().__init__(master, **kwargs)
        self._initialize_logging()
        self._setup_state()
        self._setup_ui()
        self.logger.info("InventoryView inicializado")

    def _initialize_logging(self):
        """Configura o sistema de logging para esta classe."""
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self.logger.setLevel(logging.DEBUG)
        
    def _setup_state(self):
        """Configura o estado inicial da visualização."""
        # Dados
        self.current_data: Optional[pd.DataFrame] = None
        self.filtered_data: Optional[pd.DataFrame] = None
        
        # Ordenação
        self._sort_column = self.DEFAULT_SORT_COLUMN
        self._sort_direction = self.DEFAULT_SORT_DIRECTION
        
        # Paginação
        self.page_size = self.DEFAULT_PAGE_SIZE
        self.current_page = 0
        self.total_pages = 0
        
        # Threading
        self._pending_operations = 0
        self._data_queue: Queue = Queue()
        self._loading_thread: Optional[threading.Thread] = None
        self._stop_loading = threading.Event()
        
        # UI State
        self._style_tags_configured = False
        self._last_update_time = datetime.now()
        
    def _setup_ui(self):
        """Configura todos os componentes da interface."""
        self.grid_rowconfigure(0, weight=1)
        self.grid_columnconfigure(0, weight=1)
        
        main_frame = ttk.Frame(self)
        main_frame.grid(row=0, column=0, sticky="nsew")
        main_frame.grid_rowconfigure(0, weight=1)
        main_frame.grid_columnconfigure(0, weight=1)
        
        self._setup_treeview(main_frame)
        self._setup_pagination_controls(main_frame)
        self._setup_statusbar()
        
    def _setup_treeview(self, parent: ttk.Frame):
        """Configura a Treeview com scrollbars e ordenação."""
        # Definição das colunas
        self.columns = [
            DisplayColumn('GTIN', 'str', 150),
            DisplayColumn('Codigo', 'str', 100),
            DisplayColumn('Descricao', 'str', 200, stretch=True),
            DisplayColumn('Preco', 'float', 80),
            DisplayColumn('Custo', 'float', 80),
            DisplayColumn('Estoque', 'int', 80),
            DisplayColumn('QNT_CONTADA', 'int', 100),
            DisplayColumn('DIFERENCA', 'int', 100),
            DisplayColumn('OPERADOR', 'str', 120),
            DisplayColumn('ENDERECO', 'str', 150),
            DisplayColumn('Flag', 'str', 80)
        ]
        
        # Treeview principal
        self.tree = ttk.Treeview(
            parent,
            show="headings",
            selectmode="extended",
            height=25
        )
        
        # Configuração das colunas
        column_names = [col.name for col in self.columns]
        self.tree["columns"] = column_names
        
        for col in self.columns:
            self.tree.heading(
                col.name, 
                text=col.name,
                command=lambda c=col.name: self.sort_by_column(c)
            )
            self.tree.column(
                col.name,
                width=col.width,
                anchor=col.anchor,
                stretch=col.stretch
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
        
    def _setup_pagination_controls(self, parent: ttk.Frame):
        """Configura os controles de paginação."""
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
        """Configura a barra de status."""
        self.status_frame = ttk.Frame(self)
        self.status_frame.grid(row=1, column=0, sticky="ew")
        
        self.status_var = tk.StringVar(value="Pronto")
        self.rows_var = tk.StringVar(value="Registros: 0")
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
        """Exibe os dados na Treeview de forma assíncrona e segura.
        
        Args:
            data: DataFrame com os dados a serem exibidos
        """
        if not isinstance(data, pd.DataFrame) or data.empty:
            self._update_status("Nenhum dado para exibir", rows=0)
            return
            
        self._pending_operations += 1
        start_time = datetime.now()
        
        try:
            if not self._style_tags_configured:
                self._configure_style_tags()
                self._style_tags_configured = True
            
            self._load_data_async(data, start_time)
        except Exception as e:
            self.logger.error(f"Erro ao iniciar carregamento: {e}", exc_info=True)
            self._update_status("Erro ao carregar dados", error=True)
            self._pending_operations = max(0, self._pending_operations - 1)
    
    def _load_data_async(self, data: pd.DataFrame, start_time: datetime):
        """Carrega os dados em uma thread separada."""
        self._stop_loading.set()
        if self._loading_thread and self._loading_thread.is_alive():
            self._loading_thread.join(timeout=1)
        
        self._stop_loading.clear()
        
        self._loading_thread = threading.Thread(
            target=self._load_data_background,
            args=(data.copy(), start_time),
            daemon=True
        )
        self._loading_thread.start()
    
    def _load_data_background(self, data: pd.DataFrame, start_time: datetime):
        """Processa os dados em background e atualiza a UI na thread principal."""
        try:
            processed_data = self._process_data(data)
            self._data_queue.put(processed_data)
            
            self.after(0, lambda: self._update_display(processed_data, start_time))
        except Exception as e:
            self.logger.error(f"Erro no carregamento em background: {e}", exc_info=True)
            self.after(0, lambda: self._update_status(f"Erro: {str(e)}", error=True))
    
    def _process_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """Processa os dados para exibição com tratamento robusto."""
        try:
            # Cópia segura para manipulação
            processed = data.copy()
            
            # Conversão de tipos
            numeric_cols = ['Preco', 'Custo', 'Estoque', 'QNT_CONTADA']
            for col in numeric_cols:
                if col in processed.columns:
                    processed[col] = pd.to_numeric(processed[col], errors='coerce').fillna(0)
            
            # Calcula diferença se necessário
            if 'DIFERENCA' not in processed.columns and all(c in processed.columns for c in ['Estoque', 'QNT_CONTADA']):
                processed['DIFERENCA'] = processed['QNT_CONTADA'] - processed['Estoque']
            
            # Ordenação
            if self._sort_column in processed.columns:
                processed = processed.sort_values(
                    self._sort_column,
                    ascending=self._sort_direction == SortDirection.ASCENDING
                )
            
            return processed
            
        except Exception as e:
            self.logger.error(f"Erro no processamento de dados: {e}", exc_info=True)
            raise RuntimeError(f"Falha ao processar dados: {e}") from e
    
    def _update_display(self, data: pd.DataFrame, start_time: datetime):
        """Atualiza a exibição com os dados processados."""
        try:
            self.current_data = data
            self.total_pages = max(1, (len(data) // self.page_size) + (1 if len(data) % self.page_size else 0))
            self.current_page = 0
            
            self._update_pagination_controls()
            self._show_current_page()
            
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
            self._pending_operations = max(0, self._pending_operations - 1)
    
    def _show_current_page(self):
        """Exibe os itens da página atual de forma otimizada."""
        if self.current_data is None:
            return
            
        self.tree.delete(*self.tree.get_children())
        
        start_idx = self.current_page * self.page_size
        end_idx = min(start_idx + self.page_size, len(self.current_data))
        page_data = self.current_data.iloc[start_idx:end_idx]
        
        # Desativa redesenho durante inserção
        self.tree.bind('<<TreeviewOpen>>', lambda e: 'break')
        self.tree.config(displaycolumns=[col.name for col in self.columns])
        
        try:
            # Insere em blocos para melhor performance
            for _, row in page_data.iterrows():
                values, tags = self._prepare_row_data(row)
                self.tree.insert("", "end", values=values, tags=tags)
        finally:
            self.tree.unbind('<<TreeviewOpen>>')
            self._update_page_info()
    
    def _prepare_row_data(self, row: pd.Series) -> Tuple[List[str], List[str]]:
        """Prepara os dados de uma linha para exibição."""
        values = []
        tags = []
        
        for col in self.columns:
            val = row.get(col.name, "")
            
            # Formatação especial
            if pd.api.types.is_numeric_dtype(type(val)) and not pd.isna(val):
                if col.name in ['Preco', 'Custo']:
                    values.append(f"R$ {val:,.2f}")
                elif col.name == 'DIFERENCA':
                    values.append(f"{val:+,.0f}")
                else:
                    values.append(f"{val:,.0f}")
            else:
                values.append(str(val) if not pd.isna(val) else "")
            
            # Tags de formatação condicional
            if col.name == 'Flag' and str(val).upper() == 'ALERTA!':
                tags.append('alerta')
            elif col.name == 'DIFERENCA' and pd.notna(val):
                if val > 0:
                    tags.append('excesso')
                elif val < 0:
                    tags.append('faltante')
        
        return values, tags
    
    def _configure_style_tags(self):
        """Configura os estilos condicionais para a Treeview."""
        self.tree.tag_configure('alerta', background='#FFF3CD', foreground='#856404')  # Amarelo suave
        self.tree.tag_configure('excesso', background='#D4EDDA', foreground='#155724')  # Verde suave
        self.tree.tag_configure('faltante', background='#F8D7DA', foreground='#721C24')  # Vermelho suave
        self.tree.tag_configure('oddrow', background='#f9f9f9')
        self.tree.tag_configure('evenrow', background='#ffffff')
    
    def sort_by_column(self, column: str):
        """Ordena os dados pela coluna especificada."""
        if column not in [col.name for col in self.columns]:
            self.logger.warning(f"Tentativa de ordenar por coluna inválida: {column}")
            return
            
        # Alterna a direção se for a mesma coluna
        if column == self._sort_column:
            self._sort_direction = (
                SortDirection.ASCENDING if self._sort_direction == SortDirection.DESCENDING 
                else SortDirection.DESCENDING
            )
        else:
            self._sort_column = column
            self._sort_direction = SortDirection.DESCENDING
        
        if self.current_data is not None:
            self.display_data(self.current_data)
            
        direction_symbol = "↓" if self._sort_direction == SortDirection.DESCENDING else "↑"
        self.sort_var.set(f"Ordenado por: {column} {direction_symbol}")
    
    def prev_page(self):
        """Navega para a página anterior."""
        if self.current_page > 0:
            self.current_page -= 1
            self._show_current_page()
            self._update_pagination_controls()
    
    def next_page(self):
        """Navega para a próxima página."""
        if self.current_page < self.total_pages - 1:
            self.current_page += 1
            self._show_current_page()
            self._update_pagination_controls()
    
    def _update_pagination_controls(self):
        """Atualiza o estado dos controles de paginação."""
        if self.total_pages <= 1:
            self.prev_btn.config(state=tk.DISABLED)
            self.next_btn.config(state=tk.DISABLED)
        else:
            self.prev_btn.config(state=tk.NORMAL if self.current_page > 0 else tk.DISABLED)
            self.next_btn.config(state=tk.NORMAL if self.current_page < self.total_pages - 1 else tk.DISABLED)
    
    def _update_page_info(self):
        """Atualiza as informações de paginação."""
        if self.current_data is None:
            self.page_info.config(text="Página 0/0")
            return
            
        start_item = self.current_page * self.page_size + 1
        end_item = min((self.current_page + 1) * self.page_size, len(self.current_data))
        
        self.page_info.config(
            text=f"Página {self.current_page + 1}/{self.total_pages} "
                 f"(Itens {start_item}-{end_item} de {len(self.current_data)})"
        )
    
    def _update_status(self, message: str, rows: Optional[int] = None, 
                      sort_col: Optional[str] = None, error: bool = False):
        """Atualiza a barra de status."""
        self.status_var.set(message)
        
        if rows is not None:
            self.rows_var.set(f"Registros: {rows:,}")
            
        if sort_col is not None:
            direction_symbol = "↓" if self._sort_direction == SortDirection.DESCENDING else "↑"
            self.sort_var.set(f"Ordenado por: {sort_col} {direction_symbol}")
        
        if error:
            self.status_frame.configure(style='Error.TFrame')
            self.after(3000, lambda: self.status_frame.configure(style='TFrame'))
    
    def clear(self):
        """Limpa todos os dados da visualização."""
        self.tree.delete(*self.tree.get_children())
        self.current_data = None
        self.current_page = 0
        self.total_pages = 0
        self._update_status("Pronto", rows=0)
        self.sort_var.set("")
        self._update_pagination_controls()
        self._update_page_info()
    
    def get_selected_rows(self) -> List[Dict[str, Any]]:
        """Retorna as linhas selecionadas como dicionários."""
        try:
            return [
                {
                    'id': item,
                    'values': dict(zip(
                        [col.name for col in self.columns], 
                        self.tree.item(item)['values']
                    )),
                    'tags': self.tree.item(item)['tags']
                }
                for item in self.tree.selection()
            ]
        except Exception as e:
            self.logger.error(f"Erro ao obter seleção: {e}", exc_info=True)
            return []
    
    def is_updating(self) -> bool:
        """Verifica se há operações em andamento."""
        return self._pending_operations > 0

    def refresh_from_file(self, file_path: Path):
        """Atualiza a visualização diretamente de um arquivo parquet."""
        try:
            if not file_path.exists():
                self.clear()
                return False
                
            # Carrega apenas as colunas necessárias
            cols = [col.name for col in self.columns]
            df = pd.read_parquet(file_path, columns=cols)
            
            # Garante todas as colunas esperadas
            for col in self.columns:
                if col.name not in df.columns:
                    df[col.name] = '' if col.dtype == 'str' else 0
            
            self.display_data(df)
            return True
            
        except Exception as e:
            self.logger.error(f"Erro ao carregar arquivo: {e}", exc_info=True)
            self._update_status(f"Erro ao carregar {file_path.name}", error=True)
            return False