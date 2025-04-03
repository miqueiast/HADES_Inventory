import tkinter as tk
from tkinter import ttk, messagebox, filedialog
from pathlib import Path
from typing import Optional, Dict
import logging
import threading
import pandas as pd
from core.inventory_manager import InventoryManager
from core.file_processor import FileProcessor
from core.data_combiner import DataCombiner
from core.config_manager import ConfigManager
from ui.inventory_view import InventoryView
from ui.import_dialog import ImportDialog
from ui.progress_dialog import ProgressDialog
from utils.logger import setup_logger

class MainWindow(tk.Tk):
    def __init__(self):
        super().__init__()
        
        # Configuração inicial
        self.title("Inventário Hades")
        self.geometry("1000x700")
        self.protocol("WM_DELETE_WINDOW", self.on_close)
        
        # Inicializa primeiro os atributos de estado
        self.current_data = None
        self.watcher_active = True  # Valor padrão antes de carregar a configuração
        
        # Configuração de logging
        self.logger = setup_logger(
            "hades",
            log_file="inventario_hades.log",
            level=logging.INFO
        )
        
        # Gerenciadores
        self.config = ConfigManager()
        self.watcher_active = self.config.get("watcher.enabled", True)
        self.inventory_manager = InventoryManager()
        self.file_processor = FileProcessor(self.inventory_manager)
        self.data_combiner: Optional[DataCombiner] = None
        
        # UI
        self.setup_ui()
        self.apply_theme()
        
        # Inicialização
        self.update_inventory_list()
    
    def setup_ui(self):
        """Configura a interface do usuário"""
        # Frame principal
        self.main_frame = ttk.Frame(self)
        self.main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Barra de ferramentas
        self.setup_toolbar()
        
        # Painel de inventário
        self.setup_inventory_panel()
        
        # Visualização de dados
        self.setup_data_view()
        
        # Barra de status
        self.setup_statusbar()
    
    def setup_toolbar(self):
        """Configura a barra de ferramentas"""
        toolbar = ttk.Frame(self.main_frame)
        toolbar.pack(fill=tk.X, pady=(0, 10))
        
        # Botões principais
        ttk.Button(
            toolbar,
            text="Novo Inventário",
            command=self.create_inventory
        ).pack(side=tk.LEFT, padx=2)
        
        ttk.Button(
            toolbar,
            text="Carregar Dados Iniciais",
            command=self.load_initial_data,
            state=tk.DISABLED
        ).pack(side=tk.LEFT, padx=2)
        
        ttk.Button(
            toolbar,
            text="Incluir Novos Dados",
            command=self.import_new_data,
            state=tk.DISABLED
        ).pack(side=tk.LEFT, padx=2)
        
        ttk.Button(
            toolbar,
            text="Atualizar Dados",
            command=self.refresh_data,
            state=tk.DISABLED
        ).pack(side=tk.LEFT, padx=2)
        
        # Botão de monitoramento
        self.watcher_btn = ttk.Button(
            toolbar,
            text="Desativar Monitoramento" if self.watcher_active else "Ativar Monitoramento",
            command=self.toggle_watcher
        )
        self.watcher_btn.pack(side=tk.RIGHT, padx=2)
    
    def setup_inventory_panel(self):
        """Configura o painel de seleção de inventário"""
        inv_frame = ttk.LabelFrame(self.main_frame, text="Inventário Ativo", padding=10)
        inv_frame.pack(fill=tk.X, pady=(0, 10))
        
        # Combobox para seleção de inventário
        self.inventory_var = tk.StringVar()
        self.inventory_cb = ttk.Combobox(
            inv_frame,
            textvariable=self.inventory_var,
            state="readonly",
            width=50
        )
        self.inventory_cb.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 10))
        self.inventory_cb.bind("<<ComboboxSelected>>", self.on_inventory_selected)
        
        # Info do inventário
        self.inventory_info = ttk.Label(
            inv_frame,
            text="Nenhum inventário selecionado",
            foreground="gray"
        )
        self.inventory_info.pack(side=tk.LEFT, fill=tk.X, expand=True)
    
    def setup_data_view(self):
        """Configura a visualização dos dados"""
        self.inventory_view = InventoryView(self.main_frame)
        self.inventory_view.pack(fill=tk.BOTH, expand=True)
    
    def setup_statusbar(self):
        """Configura a barra de status"""
        self.status_var = tk.StringVar(value="Pronto")
        status_bar = ttk.Label(
            self,
            textvariable=self.status_var,
            relief=tk.SUNKEN,
            anchor=tk.W
        )
        status_bar.pack(side=tk.BOTTOM, fill=tk.X)
    
    def apply_theme(self):
        """Aplica o tema configurado"""
        try:
            style = ttk.Style()
            theme = self.config.get("ui.theme", "clam")
            style.theme_use(theme)
            
            # Configurações adicionais de estilo
            style.configure("TButton", padding=6)
            style.configure("TLabel", padding=2)
            style.configure("TFrame", background="#808080")
        except Exception as e:
            self.logger.error(f"Erro ao aplicar tema: {e}")
       
    def update_status(self, message: str):
        """Atualiza a barra de status"""
        self.status_var.set(message)
        self.logger.info(message)
        self.update_idletasks()
    
    def update_inventory_list(self):
        """Atualiza a lista de inventários disponíveis"""
        inventories = self.inventory_manager.get_inventory_list()
        display_items = [
            f"{inv['name']} - {inv['store']} ({inv['created_at'][:10]})"
            for inv in inventories
        ]
        
        self.inventory_cb['values'] = display_items
        
        if self.inventory_manager.active_inventory:
            active_info = self.inventory_manager.get_active_inventory_info()
            if active_info:
                active_display = (
                    f"{active_info['nome']} - {active_info['loja']} "
                    f"({active_info['criado_em'][:10]})"
                )
                if active_display in display_items:
                    self.inventory_cb.set(active_display)
                    self.update_inventory_info()
    
    def update_inventory_info(self):
        """Atualiza as informações do inventário selecionado"""
        if self.inventory_manager.active_inventory_path:
            info = self.inventory_manager.get_active_inventory_info()
            if info:
                self.inventory_info.config(
                    text=f"Loja: {info['loja']} | Criado em: {info['criado_em'][:16]}",
                    foreground="black"
                )
                self.enable_inventory_buttons()
            else:
                self.inventory_info.config(
                    text="Informações não disponíveis",
                    foreground="gray"
                )
        else:
            self.inventory_info.config(
                text="Nenhum inventário selecionado",
                foreground="gray"
            )
            self.disable_inventory_buttons()
    
    def enable_inventory_buttons(self):
        """Ativa botões que dependem de ter um inventário selecionado"""
        for btn in [
            "Carregar Dados Iniciais",
            "Incluir Novos Dados",
            "Atualizar Dados"
        ]:
            for child in self.main_frame.winfo_children():
                if isinstance(child, ttk.Frame):
                    for widget in child.winfo_children():
                        if isinstance(widget, ttk.Button) and widget['text'] == btn:
                            widget['state'] = tk.NORMAL
    
    def disable_inventory_buttons(self):
        """Desativa botões que dependem de ter um inventário selecionado"""
        for btn in [
            "Carregar Dados Iniciais",
            "Incluir Novos Dados",
            "Atualizar Dados"
        ]:
            for child in self.main_frame.winfo_children():
                if isinstance(child, ttk.Frame):
                    for widget in child.winfo_children():
                        if isinstance(widget, ttk.Button) and widget['text'] == btn:
                            widget['state'] = tk.DISABLED
    
    def create_inventory(self):
        """Cria um novo inventário"""
        dialog = tk.Toplevel(self)
        dialog.title("Novo Inventário")
        dialog.resizable(False, False)
        
        # Campos do formulário
        ttk.Label(dialog, text="Nome do Inventário:").grid(row=0, column=0, padx=5, pady=5, sticky=tk.E)
        name_entry = ttk.Entry(dialog, width=30)
        name_entry.grid(row=0, column=1, padx=5, pady=5)
        
        ttk.Label(dialog, text="Nome da Loja:").grid(row=1, column=0, padx=5, pady=5, sticky=tk.E)
        store_entry = ttk.Entry(dialog, width=30)
        store_entry.grid(row=1, column=1, padx=5, pady=5)
        
        # Botões
        btn_frame = ttk.Frame(dialog)
        btn_frame.grid(row=2, column=0, columnspan=2, pady=10)
        
        ttk.Button(
            btn_frame,
            text="Cancelar",
            command=dialog.destroy
        ).pack(side=tk.RIGHT, padx=5)
        
        ttk.Button(
            btn_frame,
            text="Criar",
            command=lambda: self.do_create_inventory(
                name_entry.get(),
                store_entry.get(),
                dialog
            )
        ).pack(side=tk.RIGHT)
        
        self.center_window(dialog)
    
    def do_create_inventory(self, name: str, store: str, dialog: tk.Toplevel):
        """Executa a criação do inventário"""
        from utils.validators import validate_inventory_name, validate_store_name
        
        name_error = validate_inventory_name(name)
        store_error = validate_store_name(store)
        
        if name_error or store_error:
            error_msg = ""
            if name_error:
                error_msg += f"Nome: {name_error}\n"
            if store_error:
                error_msg += f"Loja: {store_error}"
            messagebox.showerror("Erro de Validação", error_msg.strip())
            return
        
        result = self.inventory_manager.create_inventory(name, store)
        
        if result:
            self.update_inventory_list()
            self.update_inventory_info()
            dialog.destroy()
            messagebox.showinfo(
                "Sucesso",
                f"Inventário '{name}' criado com sucesso para a loja '{store}'"
            )
        else:
            messagebox.showerror(
                "Erro",
                "Não foi possível criar o inventário. Verifique os logs para mais detalhes."
            )
    
    def on_inventory_selected(self, event=None):
        """Callback quando um inventário é selecionado"""
        selection = self.inventory_var.get()
        if not selection:
            return
            
        parts = selection.split(" - ")
        if len(parts) >= 2:
            name = parts[0]
            store = parts[1].split(" (")[0]
            
            inventories = self.inventory_manager.get_inventory_list()
            for inv in inventories:
                if inv['name'] == name and inv['store'] == store:
                    self.inventory_manager.set_active_inventory(inv['path'])
                    self.update_inventory_info()
                    
                    data_path = self.inventory_manager.get_active_inventory_data_path()
                    if data_path:
                        self.data_combiner = DataCombiner(data_path)
                        if self.watcher_active:
                            self.data_combiner.start_watching(
                                self.config.get("watcher.interval", 60))
                
                    self.refresh_data()
                    break
    
    def load_initial_data(self):
        """Carrega o arquivo TXT inicial"""
        file_types = [("Arquivos de texto", "*.txt"), ("Todos os arquivos", "*.*")]
        file_path = filedialog.askopenfilename(
            title="Selecione o arquivo TXT com os dados iniciais",
            filetypes=file_types
        )
        
        if not file_path:
            return
            
        progress = ProgressDialog(
            self,
            title="Processando Arquivo",
            message="Processando arquivo inicial, por favor aguarde..."
        )
        progress.show()
        
        try:
            def process_file():
                success, message = self.file_processor.process_initial_txt(file_path)
                self.after(0, lambda: progress.close())
                
                if success:
                    self.after(0, lambda: messagebox.showinfo(
                        "Sucesso",
                        "Arquivo processado com sucesso!\n" + message
                    ))
                    self.after(0, self.refresh_data)
                else:
                    self.after(0, lambda: messagebox.showerror(
                        "Erro",
                        "Falha ao processar arquivo:\n" + message
                    ))
            
            threading.Thread(target=process_file, daemon=True).start()
            
        except Exception as e:
            progress.close()
            messagebox.showerror(
                "Erro",
                f"Ocorreu um erro ao processar o arquivo:\n{str(e)}"
            )
            self.logger.error(f"Erro ao processar arquivo inicial: {e}", exc_info=True)
    
    def import_new_data(self):
        """Importa novos dados de contagem"""
        dialog = ImportDialog(self)
        file_path = dialog.show()
        
        if not file_path:
            return
            
        progress = ProgressDialog(
            self,
            title="Processando Excel",
            message="Processando arquivo de contagem, por favor aguarde..."
        )
        progress.show()
        
        try:
            def process_file():
                success, message = self.file_processor.process_excel_file(file_path)
                self.after(0, lambda: progress.close())
                
                if success:
                    self.after(0, lambda: messagebox.showinfo(
                        "Sucesso",
                        "Arquivo processado com sucesso!\n" + message
                    ))
                    self.after(0, self.refresh_data)
                else:
                    self.after(0, lambda: messagebox.showerror(
                        "Erro",
                        "Falha ao processar arquivo:\n" + message
                    ))
            
            threading.Thread(target=process_file, daemon=True).start()
            
        except Exception as e:
            progress.close()
            messagebox.showerror(
                "Erro",
                f"Ocorreu um erro ao processar o arquivo:\n{str(e)}"
            )
            self.logger.error(f"Erro ao processar arquivo Excel: {e}", exc_info=True)
    
    def refresh_data(self):
        """Atualiza a visualização dos dados"""
        if not self.inventory_manager.active_inventory_path:
            return
            
        data_path = self.inventory_manager.get_active_inventory_data_path()
        if not data_path:
            return
            
        combined_file = Path(data_path) / "combined_data.parquet"
        
        if combined_file.exists():
            try:
                df = pd.read_parquet(combined_file)
                self.current_data = df
                
                if hasattr(self, 'inventory_view'):
                    self.inventory_view.display_data(df)
                
                if 'DIFERENCA' in df.columns:
                    pos_diff = (df['DIFERENCA'] > 0).sum()
                    neg_diff = (df['DIFERENCA'] < 0).sum()
                    self.update_status(
                        f"Dados carregados | Itens com excesso: {pos_diff} | "
                        f"Itens faltando: {neg_diff} | Total: {len(df)}"
                    )
                else:
                    self.update_status(f"Dados carregados | Total: {len(df)}")
            except Exception as e:
                self.logger.error(f"Erro ao carregar dados combinados: {e}", exc_info=True)
                messagebox.showerror(
                    "Erro",
                    f"Não foi possível carregar os dados:\n{str(e)}"
                )
        else:
            self.update_status("Nenhum arquivo de dados combinados encontrado")
    
    def toggle_watcher(self):
        """Ativa/desativa o monitoramento automático"""
        if not hasattr(self, 'data_combiner') or not self.data_combiner:
            return
            
        self.watcher_active = not self.watcher_active
        self.config.set("watcher.enabled", self.watcher_active)
        
        if self.watcher_active:
            self.data_combiner.start_watching(self.config.get("watcher.interval", 60))
        else:
            self.data_combiner.stop_watching()
        
        self.update_watcher_button()
        messagebox.showinfo(
            "Monitoramento",
            f"Monitoramento automático {'ativado' if self.watcher_active else 'desativado'}"
        )
    
    def update_watcher_button(self):
        """Atualiza o texto do botão de monitoramento"""
        if hasattr(self, 'watcher_btn') and hasattr(self, 'watcher_active'):
            self.watcher_btn.config(
                text="Desativar Monitoramento" if self.watcher_active else "Ativar Monitoramento"
            )
    
    def center_window(self, window):
        """Centraliza uma janela em relação à janela principal"""
        window.update_idletasks()
        width = window.winfo_width()
        height = window.winfo_height()
        x = (self.winfo_screenwidth() // 2) - (width // 2)
        y = (self.winfo_screenheight() // 2) - (height // 2)
        window.geometry(f"{width}x{height}+{x}+{y}")
    
    def on_close(self):
        """Callback para fechar a janela principal"""
        try:
            if hasattr(self, 'data_combiner') and self.data_combiner:
                self.data_combiner.stop_watching()
        except Exception as e:
            self.logger.error(f"Erro ao parar watcher: {e}")
        finally:
            self.destroy()

if __name__ == "__main__":
    app = MainWindow()
    app.mainloop()