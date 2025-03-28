import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from typing import Optional, Dict, Any
from pathlib import Path
import pandas as pd
from core.inventory_manager import InventoryManager
from core.file_processor import FileProcessor
from core.data_combiner import DataCombiner
from .progress_dialog import ProgressDialog
from .import_dialog import ImportDialog
from .inventory_view import InventoryView
from .inventory_manager_dialog import InventoryManagerDialog
import threading
import time

class MainWindow(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Inventário HADES")
        self.geometry("1200x800")
        
        self.inventory_manager = InventoryManager()
        self.data_combiner = None
        self.current_data = None
        
        self.create_widgets()
        self.protocol("WM_DELETE_WINDOW", self.on_close)
        
    def create_widgets(self):
        # Frame superior com controles
        control_frame = ttk.Frame(self)
        control_frame.pack(fill=tk.X, padx=5, pady=5)
        
        # Botão para criar/gerenciar inventários
        ttk.Button(control_frame, text="Gerenciar Inventários", 
                  command=self.manage_inventories).pack(side=tk.LEFT, padx=5)
        
        # Botão para importar dados iniciais
        ttk.Button(control_frame, text="Importar Dados Iniciais (TXT)", 
                  command=self.import_initial_data).pack(side=tk.LEFT, padx=5)
        
        # Botão para incluir novos dados
        ttk.Button(control_frame, text="Incluir Novos Dados (Excel)", 
                  command=self.import_new_data).pack(side=tk.LEFT, padx=5)
        
        # Botão para ativar/desativar combinação automática
        self.auto_combine_btn = ttk.Button(control_frame, text="Ativar Combinação Automática", 
                                         command=self.toggle_auto_combine)
        self.auto_combine_btn.pack(side=tk.LEFT, padx=5)
        
        # Botão para salvar
        ttk.Button(control_frame, text="Salvar", command=self.save_data).pack(side=tk.LEFT, padx=5)
        
        # Label do inventário ativo
        self.active_inv_label = ttk.Label(control_frame, text="Nenhum inventário ativo")
        self.active_inv_label.pack(side=tk.RIGHT, padx=10)
        
        # Visualização dos dados
        self.inventory_view = InventoryView(self)
        self.inventory_view.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
    def manage_inventories(self):
        """Abre diálogo para criar/selecionar inventários"""
        if not hasattr(self, 'active_inv_label'):
            self.active_inv_label = ttk.Label(self.control_frame, text="Nenhum inventário ativo")
            self.active_inv_label.pack(side=tk.RIGHT, padx=10)
        
        dialog = InventoryManagerDialog(self, self.inventory_manager)
        self.wait_window(dialog)
        
        if dialog.result:  # Se um inventário foi selecionado
            data_path = self.inventory_manager.get_active_inventory_data_path()
            if data_path:
                self.data_combiner = DataCombiner(data_path)
                self.active_inv_label.config(
                    text=f"Inventário Ativo: {self.inventory_manager.active_inventory}"
                )
                self.load_and_display_data()
    
    def import_initial_data(self):
        """Importa arquivo TXT inicial"""
        if not self.inventory_manager.active_inventory_path:
            messagebox.showerror("Erro", "Nenhum inventário ativo selecionado")
            return
            
        file_path = filedialog.askopenfilename(
            title="Selecione o arquivo TXT inicial",
            filetypes=[("Arquivos TXT", "*.txt"), ("Todos os arquivos", "*.*")]
        )
        
        if not file_path:
            return
            
        # Mostra diálogo de progresso
        progress = ProgressDialog(
            self, 
            title="Processando Arquivo Inicial", 
            message="Processando arquivo TXT, por favor aguarde..."
        )
        
        def process_file():
            data_path = self.inventory_manager.get_active_inventory_data_path()
            success, msg = FileProcessor.process_initial_txt(file_path, data_path)
            
            self.after(0, progress.close)
            
            if success:
                messagebox.showinfo("Sucesso", "Arquivo processado com sucesso!")
                self.after(0, self.load_and_display_data)
            else:
                messagebox.showerror("Erro", f"Falha ao processar arquivo:\n{msg}")
        
        threading.Thread(target=process_file, daemon=True).start()
        progress.show()
    
    def import_new_data(self):
        """Importa novo arquivo Excel de contagem"""
        if not self.inventory_manager.active_inventory_path:
            messagebox.showerror("Erro", "Nenhum inventário ativo selecionado")
            return
            
        dialog = ImportDialog(self)
        self.wait_window(dialog)
        
        if dialog.file_path:
            # Mostra diálogo de progresso
            progress = ProgressDialog(
                self, 
                title="Processando Novo Arquivo", 
                message="Processando arquivo Excel, por favor aguarde..."
            )
            
            def process_file():
                data_path = self.inventory_manager.get_active_inventory_data_path()
                success, msg = FileProcessor.process_excel_file(dialog.file_path, data_path)
                
                self.after(0, progress.close)
                
                if success:
                    messagebox.showinfo("Sucesso", "Arquivo processado com sucesso!")
                    # Aguarda 2 segundos e combina dados
                    time.sleep(2)
                    self.after(0, self.load_and_display_data)
                else:
                    messagebox.showerror("Erro", f"Falha ao processar arquivo:\n{msg}")
            
            threading.Thread(target=process_file, daemon=True).start()
            progress.show()
    
    def toggle_auto_combine(self):
        """Ativa/desativa combinação automática"""
        if not self.data_combiner:
            messagebox.showerror("Erro", "Nenhum inventário ativo ou pasta de dados configurada")
            return
            
        if self.data_combiner.watching:
            self.data_combiner.stop_watching()
            self.auto_combine_btn.config(text="Ativar Combinação Automática")
            messagebox.showinfo("Info", "Combinação automática desativada")
        else:
            self.data_combiner.start_watching()
            self.auto_combine_btn.config(text="Desativar Combinação Automática")
            messagebox.showinfo("Info", "Combinação automática ativada")
    
    def load_and_display_data(self):
        """Carrega e exibe os dados combinados"""
        if not self.data_combiner:
            return
            
        # Combina dados
        self.data_combiner.combine_data()
        
        # Carrega dados combinados
        combined_path = Path(self.inventory_manager.get_active_inventory_data_path()) / "combined_data.parquet"
        if combined_path.exists():
            try:
                df = pd.read_parquet(combined_path)
                self.current_data = df
                self.inventory_view.display_data(df)
            except Exception as e:
                messagebox.showerror("Erro", f"Falha ao carregar dados:\n{e}")
    
    def save_data(self):
        """Salva alterações nos dados"""
        if self.current_data is not None and self.data_combiner:
            combined_path = Path(self.inventory_manager.get_active_inventory_data_path()) / "combined_data.parquet"
            try:
                self.current_data.to_parquet(combined_path, index=False)
                messagebox.showinfo("Sucesso", "Dados salvos com sucesso!")
            except Exception as e:
                messagebox.showerror("Erro", f"Falha ao salvar dados:\n{e}")
    
    def on_close(self):
        """Lidar com fechamento da janela"""
        if self.data_combiner:
            self.data_combiner.stop_watching()
        self.destroy()