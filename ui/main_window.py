# Suas importações originais
from compileall import compile_file
import tkinter as tk
from tkinter import ttk, messagebox, filedialog
from pathlib import Path
from typing import Optional, Dict, Callable
import logging
import threading
import os
try:
    from venv import logger # Este import pode causar conflito, mantido como no original
except ImportError:
    logger = logging # Fallback
import pandas as pd
from datetime import datetime
import time

# Suas importações de classes
from core.inventory_manager import InventoryManager
from core.file_processor import FileProcessor
from core.data_combiner import DataCombiner
from core.config_manager import ConfigManager
from core.api_collector import ApiCollector
from ui.inventory_view import InventoryView
from ui.import_dialog import ImportDialog
from ui.progress_dialog import ProgressDialog
from utils.logger import setup_logger
from utils.validators import validate_inventory_name, validate_store_name


class MainWindow(tk.Tk):
    def __init__(self):
        super().__init__()
        
        # Configuração inicial
        self.title("Inventário Grupo Mini Preço")
        self.geometry("1000x700")
        self.minsize(800, 600)
        self.protocol("WM_DELETE_WINDOW", self._on_close)

        # Variáveis de estado
        self.current_data = None
        self.processing_lock = threading.Lock()
        self.watcher_active = True
        self.last_backup_time = None
        
        # <<< MUDANÇA 1 DE 2: INSERIR O TOKEN REAL AQUI >>>
        self.api_collector = None
        # IMPORTANTE: Substitua a string abaixo pelo seu token real que você pegou do arquivo settings.py da API.
        self.api_token = "FDAGHFH$@&@#$&$#%YFHGBSZDGHBSDFHADFHSGHSDFJFJSDFJXCVBQDFG$@&¨¨&#*(&GET12345!" 
        
        # Configuração de backup
        self.backup_running = False
        self.backup_thread = None
        self.backup_config = {
            'auto_backup': True,
            'backup_interval': 3600,
            'max_backups': 5
        }
        
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
        self._create_enhanced_context_menu()

    def _initialize_services(self):
        """Inicializa todos os serviços e gerenciadores"""
        try:
            self.config_manager = ConfigManager()
            self.watcher_active = self.config_manager.get("watcher.enabled", True)
            self.inventory_manager = InventoryManager()
            self.file_processor = FileProcessor(self.inventory_manager)
            self.data_combiner = None
        except Exception as e:
            self.logger.critical(f"Falha na inicialização dos serviços: {e}", exc_info=True)
            raise

    def _setup_ui(self):
        """Configura todos os componentes da interface"""
        self.main_frame = ttk.Frame(self)
        self.main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        self._setup_toolbar()
        self._setup_inventory_panel()
        self._setup_data_view()
        self._setup_statusbar()

    def _setup_toolbar(self):
        """Configura a barra de ferramentas superior"""
        self.toolbar = ttk.Frame(self.main_frame)
        self.toolbar.pack(fill=tk.X, pady=(0, 10))
        
        self.buttons = {
            "new": self._create_toolbutton("Novo Inventário", self.create_inventory),
            "load": self._create_toolbutton("Carregar Dados Iniciais", self.load_initial_data, False),
            "import": self._create_toolbutton("Incluir Novos Dados", self.import_new_data, False),
            "refresh": self._create_toolbutton("Atualizar Dados", self.refresh_data, False),
            "backup": self._create_toolbutton("Salvar", self.manual_backup, False)
        }
        
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
        
        self.inventory_var = tk.StringVar()
        self.inventory_cb = ttk.Combobox(
            self.inv_frame,
            textvariable=self.inventory_var,
            state="readonly",
            width=50
        )
        self.inventory_cb.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 10))
        self.inventory_cb.bind("<<ComboboxSelected>>", self.on_inventory_selected)
        
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
        status_frame = ttk.Frame(self)
        status_frame.pack(side=tk.BOTTOM, fill=tk.X)
        
        self.status_var = tk.StringVar(value="Pronto")
        ttk.Label(
            status_frame,
            textvariable=self.status_var,
            relief=tk.SUNKEN,
            anchor=tk.W,
            padding=(5, 2)
        ).pack(side=tk.LEFT, fill=tk.X, expand=True)
        
        self.last_backup_var = tk.StringVar(value="")
        ttk.Label(
            status_frame,
            textvariable=self.last_backup_var,
            relief=tk.SUNKEN,
            anchor=tk.E,
            padding=(5, 2),
            width=25
        ).pack(side=tk.RIGHT)

    def _apply_theme(self):
        """Aplica o tema visual configurado"""
        try:
            style = ttk.Style()
            theme = self.config_manager.get("ui.theme", "clam")
            style.theme_use(theme)
            style.configure("TButton", padding=6)
            style.configure("TLabel", padding=2)
            style.configure("TFrame", background="#f0f0f0")
            style.configure("TCombobox", padding=3)
        except Exception as e:
            self.logger.error(f"Erro ao aplicar tema: {e}")

    def _load_initial_state(self):
        """Carrega o estado inicial da aplicação"""
        self.update_inventory_list()
        active_path = self.config_manager.get("inventory.active_path")
        if active_path and Path(active_path).exists():
            self.inventory_manager.set_active_inventory(active_path)
            self._update_ui_for_active_inventory()

    def _update_ui_for_active_inventory(self):
        """Atualiza a UI e os serviços quando um inventário se torna ativo."""
        info = self.inventory_manager.get_active_inventory_info()
        if not info:
            return
            
        display_text = f"{info['nome']} - {info['loja']} ({info['criado_em'][:10]})"
        self.inventory_var.set(display_text)
        self.inventory_info.config(
            text=f"Loja: {info['loja']} | Criado em: {info['criado_em'][:16]}",
            foreground="black"
        )
        
        for btn in ["load", "import", "refresh", "backup"]:
            self.buttons[btn].config(state=tk.NORMAL)
        
        data_path = self.inventory_manager.get_active_inventory_data_path()
        if data_path:
            self.data_combiner = DataCombiner(data_path)
            
            # Define o callback para quando o DataCombiner (via watcher) detectar mudanças
            def on_data_combined():
                # A UI precisa ser atualizada na thread principal
                self.after(0, self.refresh_data)

            self.data_combiner.set_update_callback(on_data_combined)
            
            if self.watcher_active:
                interval = self.config_manager.get("watcher.interval", 60) # Aumentado para 60s
                self.data_combiner.start_watching(interval)
        
        # Inicia todos os serviços em background para o inventário ativo
        self._start_background_services()
        # Faz uma atualização inicial dos dados
        self.refresh_data()

    def _update_ui_for_no_inventory(self):
        """Atualiza a UI quando não há inventário ativo e para os serviços."""
        self.inventory_var.set("")
        self.inventory_info.config(
            text="Nenhum inventário selecionado",
            foreground="gray"
        )
        for btn in ["load", "import", "refresh", "backup"]:
            self.buttons[btn].config(state=tk.DISABLED)
        
        # Para todos os serviços em background se não há inventário
        self._stop_background_services()

    def create_inventory(self):
        """Cria um novo inventário"""
        dialog = tk.Toplevel(self)
        dialog.title("Novo Inventário")
        dialog.resizable(False, False)
        
        ttk.Label(dialog, text="Nome do Inventário:").grid(row=0, column=0, padx=5, pady=5, sticky=tk.W)
        name_entry = ttk.Entry(dialog, width=30)
        name_entry.grid(row=0, column=1, padx=5, pady=5)
        name_entry.focus_set()
        
        ttk.Label(dialog, text="ID da Loja (numérico, obrigatório para API):").grid(row=1, column=0, padx=5, pady=5, sticky=tk.W) # <-- MUDANÇA AQUI
        store_entry = ttk.Entry(dialog, width=30)
        store_entry.grid(row=1, column=1, padx=5, pady=5)
        
        btn_frame = ttk.Frame(dialog)
        btn_frame.grid(row=2, column=0, columnspan=2, pady=10)
        
        ttk.Button(
            btn_frame,
            text="Criar",
            command=lambda: self._handle_create_inventory(
                name_entry.get().strip(),
                store_entry.get().strip(),
                dialog
            )
        ).pack(side=tk.LEFT, padx=5)
        
        ttk.Button(
            btn_frame,
            text="Cancelar",
            command=dialog.destroy
        ).pack(side=tk.LEFT, padx=5)
        
        self.center_window(dialog)
        dialog.transient(self)
        dialog.grab_set()
        dialog.bind("<Return>", lambda e: self._handle_create_inventory(
            name_entry.get().strip(), store_entry.get().strip(), dialog
        ))

    def _handle_create_inventory(self, name: str, store: str, dialog: tk.Toplevel):
        """Valida e processa a criação de novo inventário"""
        errors = []
        name_error = validate_inventory_name(name)
        if name_error:
            errors.append(f"Nome: {name_error}")
        
        # Validação aprimorada para o loja_key:
        store_error = None
        if not store: # Campo vazio é erro
            store_error = "O ID da Loja é obrigatório."
        elif not store.isdigit(): # Não é numérico é erro
            store_error = "O ID da Loja deve ser um número inteiro."
        
        if store_error: # Se houver erro, adiciona à lista
            errors.append(f"Loja: {store_error}")
        
        if errors:
            messagebox.showerror("Erro de Validação", "\n".join(errors), parent=dialog)
            return
        
        try:
            with self._operation_in_progress("Criando inventário..."):
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
                        "Erro", "Falha ao criar inventário. Verifique os logs.", parent=dialog
                    )
        except Exception as e:
            self.logger.error(f"Erro ao criar inventário: {e}", exc_info=True)
            messagebox.showerror(
                "Erro", f"Falha inesperada:\n{str(e)}", parent=dialog
            )

    def load_initial_data(self):
        """Carrega arquivo TXT com dados iniciais"""
        file_types = [("Arquivos de texto", "*.txt"), ("Todos os arquivos", "*.*")]
        file_path = filedialog.askopenfilename(
            title="Selecione o arquivo TXT com os dados iniciais",
            filetypes=file_types
        )
        if not file_path: return
        
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
        if not file_path: return
        
        self._run_operation_with_progress(
            operation=lambda: self.file_processor.process_excel(file_path),
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
        
        data_path = self.inventory_manager.get_active_inventory_data_path()
        if not data_path:
            self.update_status("Caminho de dados inválido")
            return
            
        combined_file = Path(data_path) / "combined_data.parquet"
        
        if not combined_file.exists():
            self.update_status("Inventário criado - Adicione os dados iniciais")
            self.inventory_view.clear()
            return
        
        threading.Thread(
            target=lambda: self._refresh_data_task(combined_file),
            daemon=True
        ).start()

    def _refresh_data_task(self, file_path: Path):
        """Tarefa de carregamento de dados em background"""
        try:
            success = self.inventory_view.refresh_from_file(file_path)
            self.after(0, self._update_display_stats if success else 
                           lambda: self.update_status("Falha ao carregar dados"))
        except Exception as e:
            self.logger.error(f"Erro ao atualizar dados: {e}", exc_info=True)
            self.after(0, lambda: self.update_status(f"Erro: {str(e)}"))

    def _update_display_stats(self):
        """Atualiza as estatísticas de exibição na barra de status"""
        ### CORREÇÃO 2: Acessar a propriedade .current_data diretamente ###
        current_data = self.inventory_view.current_data
        
        if current_data is None:
            self.update_status("Nenhum dado carregado")
            return
        elif not isinstance(current_data, pd.DataFrame):
            self.update_status(f"Formato de dados inválido: {type(current_data)}")
            return
        elif current_data.empty:
            self.update_status("Planilha vazia - nada para exibir")
            return
            
        df = current_data
        if 'DIFERENCA' in df.columns:
            pos_diff = (df['DIFERENCA'] > 0).sum()
            neg_diff = (df['DIFERENCA'] < 0).sum()
            status = (f"Dados carregados | Itens com excesso: {pos_diff} | "
                      f"Itens faltando: {neg_diff} | Total: {len(df)}")
        else:
            status = f"Dados carregados | Total: {len(df)}"
        
        self.update_status(status)

    def _operation_in_progress(self, message="Processando..."):
        """Context manager para operações em andamento"""
        class OperationContext:
            def __init__(self, window, msg):
                self.window = window
                self.msg = msg
            
            def __enter__(self):
                self.window.config(cursor="watch")
                self.window.update_status(self.msg)
                return self
            
            def __exit__(self, exc_type, exc_val, exc_tb):
                self.window.config(cursor="")
                self.window.update_status("Pronto" if exc_type is None else "Erro")
        
        return OperationContext(self, message)

    def manual_backup(self):
        """Executa um backup manual do inventário ativo"""
        if not self.inventory_manager.active_inventory_path:
            messagebox.showwarning("Aviso", "Nenhum inventário ativo para backup")
            return
        
        try:
            with self._operation_in_progress("Criando backup..."):
                backup_path = self.inventory_manager.backup_inventory(
                    self.inventory_manager.active_inventory_path
                )
                if backup_path:
                    self.last_backup_time = datetime.now()
                    self.last_backup_var.set(
                        f"Último backup: {self.last_backup_time.strftime('%d/%m/%Y %H:%M')}"
                    )
                    messagebox.showinfo(
                        "Backup Concluído",
                        f"Backup criado com sucesso em:\n{backup_path}"
                    )
                else:
                    messagebox.showerror(
                        "Erro", "Falha ao criar backup. Verifique os logs."
                    )
        except Exception as e:
            self.logger.error(f"Erro no backup manual: {e}", exc_info=True)
            messagebox.showerror(
                "Erro", f"Falha inesperada no backup:\n{str(e)}"
            )

    def on_inventory_selected(self, event=None):
        """Callback para seleção de inventário"""
        selection = self.inventory_var.get()
        if not selection: return
        
        # Parse do nome e loja para encontrar o caminho correto
        name = selection.split(" - ")[0]
        store = selection.split(" - ")[1].split(" (")[0]
        
        inventories = self.inventory_manager.get_inventory_list()
        for inv in inventories:
            if inv['name'] == name and inv['store'] == store:
                if self.inventory_manager.set_active_inventory(inv['path']):
                    self.config_manager.set("inventory.active_path", inv['path'])
                    self._update_ui_for_active_inventory()
                break

    def _run_operation_with_progress(self, operation: Callable, title: str, message: str,
                                     success_msg: str, error_msg: str):
        """Executa uma operação com diálogo de progresso"""
        progress = ProgressDialog(self, title=title, message=message)
        
        def execute():
            try:
                success, result = operation()
                self.after(0, progress.close)
                if success:
                    self.after(0, lambda: messagebox.showinfo("Sucesso", f"{success_msg}\n{result}"))
                    # O callback do DataCombiner já chama o refresh_data.
                    # Apenas chamamos manualmente se a operação não for o combiner.
                    if "combiner" not in str(operation):
                         self.after(0, self.refresh_data)
                else:
                    self.after(0, lambda: messagebox.showerror("Erro", f"{error_msg}\n{result}"))
            except Exception as e:
                self.after(0, progress.close)
                self.after(0, lambda: messagebox.showerror("Erro", f"{error_msg}\n{str(e)}"))
                self.logger.error(f"Erro na operação '{title}': {e}", exc_info=True)
        
        progress.show()
        threading.Thread(target=execute, daemon=True).start()

    def update_inventory_list(self):
        """Atualiza a lista de inventários no combobox"""
        inventories = self.inventory_manager.get_inventory_list()
        if inventories is None: return
        
        display_items = [
            f"{inv['name']} - {inv['store']} ({inv['created_at'][:10]})"
            for inv in inventories
        ]
        self.inventory_cb['values'] = display_items

    def update_status(self, message: str, error: bool = False):
        """Atualiza a mensagem na barra de status"""
        self.status_var.set(message)
        if not error: self.logger.info(message)
        else: self.logger.error(message)

    def center_window(self, window):
        """Centraliza uma janela em relação à principal"""
        window.update_idletasks()
        width = window.winfo_width()
        height = window.winfo_height()
        x = (self.winfo_screenwidth() // 2) - (width // 2)
        y = (self.winfo_screenheight() // 2) - (height // 2)
        window.geometry(f"+{x}+{y}")

    def _create_enhanced_context_menu(self):
        """Adiciona um menu de contexto avançado à visualização de dados"""
        self.enhanced_context_menu = tk.Menu(self, tearoff=0)
        self.enhanced_context_menu.add_command(label="Validar Dados", command=self._validate_selected_data)
        self.enhanced_context_menu.add_command(label="Estatísticas", command=self._show_data_stats)
        self.enhanced_context_menu.add_separator()
        self.enhanced_context_menu.add_command(label="Atualizar", command=self.refresh_data)

        if hasattr(self.inventory_view, 'bind_context_menu'):
            self.inventory_view.bind_context_menu(self.enhanced_context_menu)

    def _validate_selected_data(self):
        """Valida os dados selecionados com tratamento robusto"""
        # (Sua lógica original, mantida como está)
        pass

    def _show_data_stats(self):
        """Exibe estatísticas avançadas dos dados selecionados"""
        # (Sua lógica original, mantida como está)
        pass
        
    def _start_background_services(self):
        """Inicia todos os serviços que rodam em background (API, Backup, etc.)."""
        self._start_api_collector()
        self._start_auto_backup()

    def _stop_background_services(self):
        """Para todos os serviços que rodam em background."""
        self._stop_api_collector()
        self._stop_auto_backup()

    def _start_api_collector(self):
        """Inicia o coletor de dados da API para o inventário ativo."""
        self._stop_api_collector()

        data_path = self.inventory_manager.get_active_inventory_data_path()
        inv_info = self.inventory_manager.get_active_inventory_info()

        if data_path and inv_info:
            if "COLOQUE_AQUI" in self.api_token or not self.api_token:
                self.logger.warning("Token da API não configurado. O coletor não será iniciado.")
                self.update_status("Aviso: Token da API não configurado.", error=True)
                return
            
            ### CORREÇÃO 1: Validação da loja_key ###
            try:
                loja_key = inv_info.get('loja')
                if not loja_key or not str(loja_key).isdigit():
                     self.logger.warning(f"ID da Loja ('{loja_key}') é inválido. Coletor da API não será iniciado.")
                     self.update_status(f"Aviso: ID da Loja '{loja_key}' inválido para a API.", error=True)
                     return

                # Esta função será passada para o ApiCollector.
                # Ela será chamada de uma thread secundária.
                def api_update_trigger():
                    self.logger.info("ApiCollector detectou novos dados. Acionando DataCombiner.")
                    # Agendamos a combinação para rodar na thread principal da UI,
                    # para evitar conflitos de acesso a arquivos.
                    if self.data_combiner:
                        self.after(0, self.data_combiner.combine_data)
                
                # Instanciamos o coletor com o token real e o callback.
                self.api_collector = ApiCollector(
                    inventory_data_path=Path(data_path),
                    api_token=self.api_token,
                    loja_key=loja_key,
                    update_callback=api_update_trigger # Passando a função de gatilho
                )
                self.api_collector.start()
                self.update_status(f"Coletor da API ativado para a loja: {loja_key}")
            
            except (ValueError, TypeError) as e:
                self.logger.error(f"Falha ao iniciar ApiCollector devido a loja_key inválida: {e}")
                self.update_status(f"Erro: ID da Loja deve ser um número.", error=True)
        else:
            self.logger.warning("Não foi possível iniciar o coletor da API: caminho ou informações do inventário inválidos.")

    def _stop_api_collector(self):
        """Para o coletor de dados da API, se estiver em execução."""
        if self.api_collector:
            self.api_collector.stop()
            self.api_collector = None
            self.logger.info("Coletor da API parado.")
    
    def _stop_auto_backup(self):
        """Para o serviço de backup automático."""
        if self.backup_thread and self.backup_thread.is_alive():
            self.backup_running = False
            self.backup_thread.join(timeout=2)
        self.logger.info("Serviço de backup automático parado.")

    def _start_auto_backup(self):
        """Inicia o serviço de backup automático"""
        # (Sua lógica original, mantida como está)
        pass

    def _cleanup_old_backups(self):
        """Remove backups antigos conforme configuração"""
        # (Sua lógica original, mantida como está)
        pass

    def _on_close(self):
        """Método para lidar com o fechamento da janela principal"""
        self.logger.info("Iniciando processo de encerramento da aplicação...")
        self._stop_background_services()
        
        if hasattr(self, 'data_combiner') and self.data_combiner:
            self.data_combiner.stop_watching()
            
        if hasattr(self, 'config_manager') and self.config_manager:
            try:
                self.config_manager.save_config()
            except Exception as e:
                self.logger.error(f"Erro ao salvar configuração ao fechar: {e}")
        
        self.destroy()
        self.logger.info("Aplicação encerrada.")

    def toggle_watcher(self):
        """Ativa/desativa o monitoramento automático"""
        if not hasattr(self, 'data_combiner') or not self.data_combiner:
            messagebox.showwarning("Aviso", "Selecione um inventário para ativar o monitoramento.")
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
            
            self.watcher_btn.config(
                text="Desativar Monitoramento" if self.watcher_active else "Ativar Monitoramento"
            )
            self.update_status(message)
            messagebox.showinfo("Monitoramento", message)
        except Exception as e:
            self.logger.error(f"Erro ao alternar monitoramento: {e}", exc_info=True)
            messagebox.showerror(
                "Erro",
                f"Falha ao alternar monitoramento:\n{str(e)}"
            )

# Ponto de entrada da aplicação
if __name__ == "__main__":
    app = MainWindow()
    app.mainloop()