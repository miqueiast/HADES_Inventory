#inventory_manager_dialog.py
import tkinter as tk
from tkinter import ttk
from typing import Optional, Dict, Any
from pathlib import Path

class InventoryManagerDialog(tk.Toplevel):
    def __init__(self, parent, inventory_manager):
        super().__init__(parent)
        self.title("Gerenciar Inventários")
        self.geometry("800x500")  # Aumentei o tamanho para melhor visualização
        
        self.inventory_manager = inventory_manager
        self.result = None
        
        self.create_widgets()
        self.center_on_parent()
        self.load_inventories()
        
        # Configura duplo clique para selecionar
        self.tree.bind("<Double-1>", lambda e: self.select_inventory())
        
    def create_widgets(self):
        """Cria os componentes da interface"""
        main_frame = ttk.Frame(self, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Frame para criar novo inventário
        create_frame = ttk.LabelFrame(main_frame, text="Criar Novo Inventário", padding="10")
        create_frame.pack(fill=tk.X, pady=5)
        
        ttk.Label(create_frame, text="Nome do Inventário:").grid(row=0, column=0, sticky="w")
        self.name_entry = ttk.Entry(create_frame)
        self.name_entry.grid(row=0, column=1, sticky="ew", padx=5, pady=2)
        
        ttk.Label(create_frame, text="Nome da Loja:").grid(row=1, column=0, sticky="w")
        self.store_entry = ttk.Entry(create_frame)
        self.store_entry.grid(row=1, column=1, sticky="ew", padx=5, pady=2)
        
        ttk.Button(
            create_frame,
            text="Criar",
            command=self.create_inventory
        ).grid(row=2, column=1, sticky="e", pady=5)
        
        # Frame para inventários existentes
        list_frame = ttk.LabelFrame(main_frame, text="Inventários Existentes", padding="10")
        list_frame.pack(fill=tk.BOTH, expand=True, pady=5)
        
        # Treeview para listar inventários
        columns = ("name", "store", "created", "path")
        self.tree = ttk.Treeview(
            list_frame,
            columns=columns,
            show="headings",
            selectmode="browse"
        )
        
        # Configura colunas
        self.tree.heading("name", text="Nome")
        self.tree.column("name", width=150)
        self.tree.heading("store", text="Loja")
        self.tree.column("store", width=100)
        self.tree.heading("created", text="Criado em")
        self.tree.column("created", width=120)
        self.tree.heading("path", text="Caminho")
        self.tree.column("path", width=200, stretch=False)
        
        # Scrollbar
        scrollbar = ttk.Scrollbar(list_frame, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=scrollbar.set)
        
        # Layout
        self.tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        # Botões de ação
        button_frame = ttk.Frame(main_frame)
        button_frame.pack(fill=tk.X, pady=5)
        
        ttk.Button(
            button_frame,
            text="Selecionar",
            command=self.select_inventory
        ).pack(side=tk.RIGHT, padx=5)
        
        ttk.Button(
            button_frame,
            text="Cancelar",
            command=self.cancel
        ).pack(side=tk.RIGHT)
        
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
    
    def load_inventories(self):
        """Carrega a lista de inventários existentes"""
        for item in self.tree.get_children():
            self.tree.delete(item)
            
        inventories = self.inventory_manager.get_inventory_list()
        for inv in inventories:
            self.tree.insert("", "end", values=(
                inv["name"],
                inv["store"],
                inv["created_at"],
                inv["path"]
            ))
    
    def create_inventory(self):
        """Cria um novo inventário"""
        name = self.name_entry.get().strip()
        store = self.store_entry.get().strip()
        
        if not name or not store:
            tk.messagebox.showerror("Erro", "Por favor, preencha todos os campos")
            return
            
        success = self.inventory_manager.create_inventory(name, store)
        if success:
            tk.messagebox.showinfo("Sucesso", "Inventário criado com sucesso!")
            self.load_inventories()
            self.name_entry.delete(0, tk.END)
            self.store_entry.delete(0, tk.END)
        else:
            tk.messagebox.showerror("Erro", "Não foi possível criar o inventário")
    
    def select_inventory(self):
        """Seleciona o inventário escolhido"""
        selected = self.tree.focus()
        if not selected:
            tk.messagebox.showwarning("Aviso", "Nenhum inventário selecionado")
            return
            
        item = self.tree.item(selected)
        path = item["values"][3]  # O caminho está na 4ª coluna
        
        if self.inventory_manager.set_active_inventory(path):
            self.result = path
            self.destroy()
        else:
            tk.messagebox.showerror("Erro", "Não foi possível selecionar o inventário")
    
    def cancel(self):
        """Fecha o diálogo sem selecionar"""
        self.result = None
        self.destroy()
    
    def show(self):
        """Exibe o diálogo e espera pela resposta"""
        self.grab_set()
        self.wait_window(self)
        return self.result