#main_window.py
import tkinter as tk
from tkinter import ttk, messagebox, filedialog
from pathlib import Path
from typing import Optional, Dict, Tuple
import logging
import threading
import os
import pandas as pd
from core.inventory_manager import InventoryManager
from core.file_processor import FileProcessor
from core.data_combiner import DataCombiner
from core.config_manager import ConfigManager
from ui.inventory_view import InventoryView
from ui.import_dialog import ImportDialog
from ui.progress_dialog import ProgressDialog
from utils.logger import setup_logger
from utils.validators import validate_inventory_name, validate_store_name

class MainWindow(tk.Tk):
    def __init__(self):
        super().__init__()
        self._set_window_icon()
        
        # Configuração inicial
        self.title("Inventário Grupo Mini Preço")
        self.geometry("1000x700")
        self.minsize(800, 600)
        self.protocol("WM_DELETE_WINDOW", self.on_close)
        
        # Variáveis de estado
        self.current_data = None
        self.processing_lock = threading.Lock()
        self.watcher_active = True
        
        # Configuração de logging
        self.logger = setup_logger(
            "hades",
            log_file="inventario_hades.log",
            level=logging.INFO
        )
        
        # Inicializa serviços
        self._initialize_services()
        
        # Interface do usuário
        self._setup_ui()
        self._apply_theme()
        
        # Carrega estado inicial
        self._load_initial_state()

    def _initialize_services(self):
        """Inicializa todos os serviços e gerenciadores"""
        try:
            self.config_manager = ConfigManager()  # Renomeado para evitar conflito
            self.watcher_active = self.config_manager.get("watcher.enabled", True)
            self.inventory_manager = InventoryManager()
            self.file_processor = FileProcessor(self.inventory_manager)
            self.data_combiner = None
        except Exception as e:
            self.logger.critical(f"Falha na inicialização dos serviços: {e}", exc_info=True)
            raise
        
    def _set_window_icon(self):
        """Configura o ícone da janela principal com tamanho aprimorado"""
        try:
            # Caminho para a pasta assets
            assets_dir = os.path.join(os.path.dirname(__file__), 'assets')
            icon_path = os.path.join(assets_dir, 'logo_mini.ico')
            
            if os.path.exists(icon_path):
                # Para Windows (usando .ico com múltiplos tamanhos embutidos)
                if os.name == 'nt':
                    try:
                        # Força o uso do ícone em tamanho maior
                        self.iconbitmap(default=icon_path)
                        # Adiciona também como ícone da aplicação
                        self.tk.call('wm', 'iconphoto', self._w, 
                                    tk.PhotoImage(file=icon_path))
                    except Exception as e:
                        self.logger.warning(f"Erro ao carregar ícone no Windows: {e}")
                
                # Para Linux/macOS ou fallback
                else:
                    try:
                        # Carrega a imagem com tamanho específico
                        img = tk.PhotoImage(file=icon_path)
                        # Redimensiona se necessário (opcional)
                        img = img.zoom(2, 2)  # Aumenta 2x
                        self.tk.call('wm', 'iconphoto', self._w, img)
                    except:
                        # Fallback simples se o redimensionamento falhar
                        self.tk.call('wm', 'iconphoto', self._w, 
                                    tk.PhotoImage(file=icon_path))
            else:
                self.logger.warning("Arquivo de ícone não encontrado")
        except Exception as e:
            self.logger.error(f"Erro ao configurar ícone: {e}")

    def _setup_ui(self):
        """Configura todos os componentes da interface"""
        # Frame principal
        self.main_frame = ttk.Frame(self)
        self.main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Componentes da UI
        self._setup_toolbar()
        self._setup_inventory_panel()
        self._setup_data_view()
        self._setup_statusbar()

    def _setup_toolbar(self):
        """Configura a barra de ferramentas superior"""
        self.toolbar = ttk.Frame(self.main_frame)
        self.toolbar.pack(fill=tk.X, pady=(0, 10))
        
        # Botões principais
        self.buttons = {
            "new": self._create_toolbutton("Novo Inventário", self.create_inventory),
            "load": self._create_toolbutton("Carregar Dados Iniciais", self.load_initial_data, False),
            "import": self._create_toolbutton("Incluir Novos Dados", self.import_new_data, False),
            "refresh": self._create_toolbutton("Atualizar Dados", self.refresh_data, False)
        }
        
        # Botão de monitoramento
        self.watcher_btn = ttk.Button(
            self.toolbar,
            text="Desativar Monitoramento" if self.watcher_active else "Ativar Monitoramento",
            command=self.toggle_watcher
        )
        self.watcher_btn.pack(side=tk.RIGHT, padx=2)

    def _create_toolbutton(self, text: str, command, enabled: bool = True) -> ttk.Button:
        """Cria um botão padronizado para a toolbar"""
        btn = ttk.Button(
            self.toolbar,
            text=text,
            command=command,
            state=tk.NORMAL if enabled else tk.DISABLED
        )
        btn.pack(side=tk.LEFT, padx=2)
        return btn

    def _setup_inventory_panel(self):
        """Configura o painel de seleção de inventário"""
        self.inv_frame = ttk.LabelFrame(self.main_frame, text="Inventário Ativo", padding=10)
        self.inv_frame.pack(fill=tk.X, pady=(0, 10))
        
        # Combobox para seleção
        self.inventory_var = tk.StringVar()
        self.inventory_cb = ttk.Combobox(
            self.inv_frame,
            textvariable=self.inventory_var,
            state="readonly",
            width=50
        )
        self.inventory_cb.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 10))
        self.inventory_cb.bind("<<ComboboxSelected>>", self.on_inventory_selected)
        
        # Informações do inventário
        self.inventory_info = ttk.Label(
            self.inv_frame,
            text="Nenhum inventário selecionado",
            foreground="gray"
        )
        self.inventory_info.pack(side=tk.LEFT, fill=tk.X, expand=True)

    def _setup_data_view(self):
        """Configura a área de visualização de dados"""
        self.inventory_view = InventoryView(self.main_frame)
        self.inventory_view.pack(fill=tk.BOTH, expand=True)

    def _setup_statusbar(self):
        """Configura a barra de status inferior"""
        self.status_var = tk.StringVar(value="Pronto")
        self.status_bar = ttk.Label(
            self,
            textvariable=self.status_var,
            relief=tk.SUNKEN,
            anchor=tk.W,
            padding=(5, 2)
        )
        self.status_bar.pack(side=tk.BOTTOM, fill=tk.X)

    def _apply_theme(self):
        """Aplica o tema visual configurado"""
        try:
            style = ttk.Style()
            theme = self.config_manager.get("ui.theme", "clam")
            style.theme_use(theme)
            
            # Configurações personalizadas
            style.configure("TButton", padding=6)
            style.configure("TLabel", padding=2)
            style.configure("TFrame", background="#f0f0f0")
            style.configure("TCombobox", padding=3)
        except Exception as e:
            self.logger.error(f"Erro ao aplicar tema: {e}")

    def _load_initial_state(self):
        """Carrega o estado inicial da aplicação"""
        self.update_inventory_list()
        
        # Verifica se há um inventário ativo na sessão anterior
        active_path = self.config_manager.get("inventory.active_path")
        if active_path and Path(active_path).exists():
            self.inventory_manager.set_active_inventory(active_path)
            self._update_ui_for_active_inventory()

    def _update_ui_for_active_inventory(self):
        """Atualiza a UI quando um inventário está ativo"""
        info = self.inventory_manager.get_active_inventory_info()
        if info:
            display_text = f"{info['nome']} - {info['loja']} ({info['criado_em'][:10]})"
            self.inventory_var.set(display_text)
            self.inventory_info.config(
                text=f"Loja: {info['loja']} | Criado em: {info['criado_em'][:16]}",
                foreground="black"
            )
            
            # Ativa botões dependentes
            for btn in ["load", "import", "refresh"]:
                self.buttons[btn].config(state=tk.NORMAL)
            
            # Inicializa o data_combiner
            data_path = self.inventory_manager.get_active_inventory_data_path()
            if data_path:
                self.data_combiner = DataCombiner(data_path)
                if self.watcher_active:
                    interval = self.config_manager.get("watcher.interval", 60)
                    self.data_combiner.start_watching(interval)
            
            self.refresh_data()
        else:
            self._update_ui_for_no_inventory()

    def _update_ui_for_no_inventory(self):
        """Atualiza a UI quando não há inventário ativo"""
        self.inventory_info.config(
            text="Nenhum inventário selecionado",
            foreground="gray"
        )
        for btn in ["load", "import", "refresh"]:
            self.buttons[btn].config(state=tk.DISABLED)

    def create_inventory(self):
        """Cria um novo inventário"""
        dialog = tk.Toplevel(self)
        dialog.title("Novo Inventário")
        dialog.resizable(False, False)
        
        # Formulário
        ttk.Label(dialog, text="Nome do Inventário:").grid(row=0, column=0, padx=5, pady=5, sticky=tk.E)
        name_entry = ttk.Entry(dialog, width=30)
        name_entry.grid(row=0, column=1, padx=5, pady=5)
        name_entry.focus_set()
        
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
            command=lambda: self._handle_create_inventory(
                name_entry.get().strip(),
                store_entry.get().strip(),
                dialog
            )
        ).pack(side=tk.RIGHT)
        
        self.center_window(dialog)
        dialog.bind("<Return>", lambda e: self._handle_create_inventory(
            name_entry.get().strip(),
            store_entry.get().strip(),
            dialog
        ))

    def _handle_create_inventory(self, name: str, store: str, dialog: tk.Toplevel):
        """Valida e processa a criação de novo inventário"""
        # Validação
        errors = []
        name_error = validate_inventory_name(name)
        if name_error:
            errors.append(f"Nome: {name_error}")
        
        store_error = validate_store_name(store)
        if store_error:
            errors.append(f"Loja: {store_error}")
        
        if errors:
            messagebox.showerror("Erro de Validação", "\n".join(errors))
            return
        
        # Criação do inventário
        try:
            with self._operation_in_progress():
                result = self.inventory_manager.create_inventory(name, store)
                
                if result:
                    self.config_manager.set("inventory.active_path", result['path'])
                    self.update_inventory_list()
                    self._update_ui_for_active_inventory()
                    dialog.destroy()
                    messagebox.showinfo(
                        "Sucesso",
                        f"Inventário '{name}' criado para a loja '{store}'"
                    )
                else:
                    messagebox.showerror(
                        "Erro",
                        "Falha ao criar inventário. Verifique os logs."
                    )
        except Exception as e:
            self.logger.error(f"Erro ao criar inventário: {e}", exc_info=True)
            messagebox.showerror(
                "Erro",
                f"Falha inesperada ao criar inventário:\n{str(e)}"
            )

    def load_initial_data(self):
        """Carrega arquivo TXT com dados iniciais"""
        file_types = [("Arquivos de texto", "*.txt"), ("Todos os arquivos", "*.*")]
        file_path = filedialog.askopenfilename(
            title="Selecione o arquivo TXT com os dados iniciais",
            filetypes=file_types
        )
        
        if not file_path:
            return
        
        self._run_operation_with_progress(
            operation=lambda: self.file_processor.process_initial_txt(file_path),
            title="Processando Arquivo",
            message="Processando arquivo inicial, por favor aguarde...",
            success_msg="Arquivo processado com sucesso!",
            error_msg="Falha ao processar arquivo inicial"
        )

    def import_new_data(self):
        """Importa arquivo Excel com novos dados"""
        dialog = ImportDialog(self)
        file_path = dialog.show()
        
        if not file_path:
            return
        
        self._run_operation_with_progress(
            operation=lambda: self.file_processor.process_excel_file(file_path),
            title="Processando Excel",
            message="Processando arquivo de contagem...",
            success_msg="Dados importados com sucesso!",
            error_msg="Falha ao importar dados"
        )

    def refresh_data(self):
        """Atualiza a visualização dos dados de forma otimizada"""
        if not self.inventory_manager.active_inventory_path:
            self.update_status("Nenhum inventário ativo selecionado")
            return
        
        def load_and_display():
            try:
                with self._operation_in_progress():
                    data_path = self.inventory_manager.get_active_inventory_data_path()
                    if not data_path:
                        self.after(0, lambda: self.update_status("Caminho de dados inválido"))
                        return
                        
                    combined_file = Path(data_path) / "combined_data.parquet"
                    
                    if not combined_file.exists():
                        self.after(0, lambda: self.update_status("Arquivo combinado não encontrado"))
                        return
                        
                    # Colunas necessárias
                    required_cols = [
                        'GTIN', 'Codigo', 'Descricao', 'Preco', 'Custo',
                        'Estoque', 'QNT_CONTADA', 'COD_BARRAS', 'Flag'
                    ]
                    
                    # Lê apenas as colunas necessárias
                    try:
                        df = pd.read_parquet(combined_file, columns=required_cols)
                    except Exception as e:
                        self.logger.error(f"Erro ao ler parquet: {e}")
                        # Fallback - lê todas as colunas e filtra depois
                        df = pd.read_parquet(combined_file)
                        df = df[required_cols] if all(col in df.columns for col in required_cols) else pd.DataFrame(columns=required_cols)
                    
                    # Garante que as colunas numéricas tenham o tipo correto
                    numeric_cols = ['Estoque', 'QNT_CONTADA']
                    for col in numeric_cols:
                        if col in df.columns:
                            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                    
                    # Calcula diferença
                    if 'Estoque' in df.columns and 'QNT_CONTADA' in df.columns:
                        df['DIFERENCA'] = df['QNT_CONTADA'] - df['Estoque']
                    
                    # Ordena por diferença (maiores no topo)
                    if 'DIFERENCA' in df.columns:
                        df = df.sort_values('DIFERENCA', ascending=False)
                    
                    # Atualiza UI na thread principal
                    self.after(0, lambda: self._update_display(df))
                    
            except Exception as e:
                self.logger.error(f"Erro ao carregar dados: {e}", exc_info=True)
                self.after(0, lambda: self._show_error(f"Erro ao carregar dados: {str(e)}"))
        
        # Executa em thread separada
        threading.Thread(target=load_and_display, daemon=True).start()

    def toggle_watcher(self):
        """Ativa/desativa o monitoramento automático"""
        if not self.data_combiner:
            return
        
        self.watcher_active = not self.watcher_active
        self.config_manager.set("watcher.enabled", self.watcher_active)
        
        try:
            if self.watcher_active:
                interval = self.config_manager.get("watcher.interval", 60)
                self.data_combiner.start_watching(interval)
                message = "Monitoramento automático ativado"
            else:
                self.data_combiner.stop_watching()
                message = "Monitoramento automático desativado"
            
            self._update_watcher_button()
            self.update_status(message)
            messagebox.showinfo("Monitoramento", message)
        except Exception as e:
            self.logger.error(f"Erro ao alternar monitoramento: {e}")
            messagebox.showerror(
                "Erro",
                f"Falha ao alternar monitoramento:\n{str(e)}"
            )

    def on_inventory_selected(self, event=None):
        """Callback para seleção de inventário"""
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
                    if self.inventory_manager.set_active_inventory(inv['path']):
                        self.config_manager.set("inventory.active_path", inv['path'])
                        self._update_ui_for_active_inventory()
                    break

    def _run_operation_with_progress(self, operation, title: str, message: str,
                                   success_msg: str, error_msg: str):
        """Executa uma operação com diálogo de progresso"""
        progress = ProgressDialog(self, title=title, message=message)
        progress.show()
        
        def execute():
            try:
                success, result = operation()
                
                self.after(0, lambda: progress.close())
                
                if success:
                    self.after(0, lambda: messagebox.showinfo(
                        "Sucesso",
                        f"{success_msg}\n{result}"
                    ))
                    self.after(0, self.refresh_data)
                else:
                    self.after(0, lambda: messagebox.showerror(
                        "Erro",
                        f"{error_msg}\n{result}"
                    ))
            except Exception as e:
                self.after(0, lambda: progress.close())
                self.after(0, lambda: messagebox.showerror(
                    "Erro",
                    f"{error_msg}\n{str(e)}"
                ))
                self.logger.error(f"Erro na operação: {e}", exc_info=True)
        
        threading.Thread(target=execute, daemon=True).start()

    def _update_display(self, df: pd.DataFrame):
        """Atualiza a exibição dos dados na UI"""
        self.current_data = df
        self.inventory_view.display_data(df)
        
        # Atualiza status
        if 'DIFERENCA' in df.columns:
            pos_diff = (df['DIFERENCA'] > 0).sum()
            neg_diff = (df['DIFERENCA'] < 0).sum()
            status = (f"Dados carregados | Itens com excesso: {pos_diff} | "
                     f"Itens faltando: {neg_diff} | Total: {len(df)}")
        else:
            status = f"Dados carregados | Total: {len(df)}"
        
        self.update_status(status)

    def _show_error(self, message: str):
        """Exibe uma mensagem de erro na UI"""
        self.update_status(message)
        messagebox.showerror("Erro", message)

    def _update_watcher_button(self):
        """Atualiza o texto do botão de monitoramento"""
        self.watcher_btn.config(
            text="Desativar Monitoramento" if self.watcher_active else "Ativar Monitoramento"
        )

    def _operation_in_progress(self):
        """Context manager para operações em andamento"""
        class OperationContext:
            def __init__(self, window):
                self.window = window
            
            def __enter__(self):
                self.window.set_cursor("watch")
                self.window.update_status("Processando...")
                self.window.update_idletasks()
                return self
            
            def __exit__(self, exc_type, exc_val, exc_tb):
                self.window.set_cursor("")
                if exc_type is not None:
                    self.window.update_status("Erro durante o processamento")
                else:
                    self.window.update_status("Pronto")
                self.window.update_idletasks()
        
        return OperationContext(self)

    def update_inventory_list(self):
        """Atualiza a lista de inventários no combobox"""
        inventories = self.inventory_manager.get_inventory_list()
        display_items = [
            f"{inv['name']} - {inv['store']} ({inv['created_at'][:10]})"
            for inv in inventories
        ]
        self.inventory_cb['values'] = display_items

    def update_status(self, message: str):
        """Atualiza a mensagem na barra de status"""
        self.status_var.set(message)
        self.logger.info(message)

    def set_cursor(self, cursor_type: str):
        """Define o cursor da janela"""
        self.configure(cursor=cursor_type)

    def center_window(self, window):
        """Centraliza uma janela em relação à principal"""
        window.update_idletasks()
        width = window.winfo_width()
        height = window.winfo_height()
        x = (self.winfo_screenwidth() // 2) - (width // 2)
        y = (self.winfo_screenheight() // 2) - (height // 2)
        window.geometry(f"+{x}+{y}")

    def on_close(self):
        """Callback para fechar a aplicação"""
        try:
            if self.data_combiner:
                self.data_combiner.stop_watching()
            self.config_manager.save_config()
        except Exception as e:
            self.logger.error(f"Erro ao encerrar: {e}")
        finally:
            self.destroy()

if __name__ == "__main__":
    app = MainWindow()
    app.mainloop()