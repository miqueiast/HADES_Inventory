#virtual_treeview.py
import tkinter as tk
from tkinter import ttk
import pandas as pd
from typing import Optional, List, Dict, Any
import logging
import threading
from queue import Queue

class VirtualTreeview(ttk.Treeview):
    def __init__(self, master, page_size=1000, **kwargs):
        super().__init__(master, **kwargs)
        self.logger = logging.getLogger(f"{__name__}.VirtualTreeview")
        
        # Configuração de paginação
        self.page_size = page_size
        self.current_page = 0
        self.total_pages = 0
        self.total_items = 0
        self._data = None
        
        # Configuração de threading
        self.data_queue = Queue()
        self.loading_thread = None
        self.stop_loading = threading.Event()
        
        # Variáveis de estado
        self._sort_column = None
        self._sort_reverse = False
        
        # Configura eventos
        self.bind("<MouseWheel>", self._on_scroll)
        self.bind("<Configure>", self._update_display)
        
    def set_data(self, data: pd.DataFrame):
        """Define os dados a serem exibidos"""
        self._data = data
        self.total_items = len(data)
        self.total_pages = (self.total_items // self.page_size) + (1 if self.total_items % self.page_size else 0)
        self.current_page = 0
        self._update_display()
        
    def _update_display(self, event=None):
        """Atualiza a exibição com os dados da página atual"""
        if self._data is None:
            return
            
        # Limpa a treeview
        self.delete(*self.get_children())
        
        # Calcula os índices da página atual
        start_idx = self.current_page * self.page_size
        end_idx = min(start_idx + self.page_size, self.total_items)
        
        # Adiciona os itens da página atual
        page_data = self._data.iloc[start_idx:end_idx]
        for _, row in page_data.iterrows():
            values = [str(row[col]) if col in row else "" for col in self['columns']]
            self.insert("", "end", values=values)
            
    def _on_scroll(self, event):
        """Lida com o evento de scroll para mudar de página"""
        if event.delta > 0 and self.current_page > 0:
            self.current_page -= 1
            self._update_display()
        elif event.delta < 0 and self.current_page < self.total_pages - 1:
            self.current_page += 1
            self._update_display()
            
    def load_data_async(self, data_source, callback=None):
        """Carrega os dados em uma thread separada"""
        self.stop_loading.set()  # Para qualquer carregamento anterior
        
        if self.loading_thread and self.loading_thread.is_alive():
            self.loading_thread.join(timeout=1)
            
        self.stop_loading.clear()
        self.loading_thread = threading.Thread(
            target=self._load_data_background,
            args=(data_source, callback),
            daemon=True
        )
        self.loading_thread.start()
        
    def _load_data_background(self, data_source, callback):
        """Carrega os dados em background"""
        try:
            data = data_source()  # Deve retornar um DataFrame
            self.data_queue.put(data)
            
            # Atualiza a interface na thread principal
            if callback:
                self.after(0, lambda: callback(data))
                
        except Exception as e:
            self.logger.error(f"Erro ao carregar dados: {e}", exc_info=True)
            self.data_queue.put(None)
            
    def get_page_info(self):
        """Retorna informações sobre a paginação atual"""
        return {
            "current_page": self.current_page + 1,
            "total_pages": self.total_pages,
            "start_item": self.current_page * self.page_size + 1,
            "end_item": min((self.current_page + 1) * self.page_size, self.total_items),
            "total_items": self.total_items
        }