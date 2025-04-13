from compileall import compile_file
import tkinter as tk
from tkinter import ttk, messagebox, filedialog
from pathlib import Path
from typing import Optional, Dict, Self, Tuple
import logging
import threading
import os
from venv import logger
import pandas as pd
from datetime import datetime
import time
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
            "refresh": self._create_toolbutton("Atualizar Dados", self.refresh_data, False),
            "backup": self._create_toolbutton("Backup", self.manual_backup, False)
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
        """Atualiza a UI quando um inventário está ativo"""
        info = self.inventory_manager.get_active_inventory_info()
        if not info:
            return
            
        display_text = f"{info['nome']} - {info['loja']} ({info['criado_em'][:10]})"
        self.inventory_var.set(display_text)
        self.inventory_info.config(
            text=f"Loja: {info['loja']} | Criado em: {info['criado_em'][:16]}",
            foreground="black"
        )
        
        # Ativa botões
        for btn in ["load", "import", "refresh", "backup"]:
            self.buttons[btn].config(state=tk.NORMAL)
        
        # Configura DataCombiner
        data_path = self.inventory_manager.get_active_inventory_data_path()
        if data_path:
            self.data_combiner = DataCombiner(data_path)
            
            # Verifica se o método existe antes de chamar
            if hasattr(self.data_combiner, 'set_update_callback'):
                def update_callback():
                    combined_path = Path(data_path) / "combined_data.parquet"
                    if combined_path.exists():
                        self.inventory_view.refresh_from_file(combined_path)
                        self._update_display_stats()
                
                self.data_combiner.set_update_callback(update_callback)
            
            if self.watcher_active:
                interval = self.config_manager.get("watcher.interval", 2)
                self.data_combiner.start_watching(interval)
        
        self._start_auto_backup()

    def _update_ui_for_no_inventory(self):
        """Atualiza a UI quando não há inventário ativo"""
        self.inventory_info.config(
            text="Nenhum inventário selecionado",
            foreground="gray"
        )
        for btn in ["load", "import", "refresh", "backup"]:
            self.buttons[btn].config(state=tk.DISABLED)
            
    def _setup_data_combiner(self):
        """Configura o DataCombiner com callbacks para atualizações automáticas"""
        if not hasattr(self, 'data_combiner') or not self.data_combiner:
            return
        
        def on_data_updated():
            data_path = self.inventory_manager.get_active_inventory_data_path()
            if data_path:
                combined_file = Path(data_path) / "combined_data.parquet"
                self.inventory_view.refresh_from_file(combined_file)
                self._update_display_stats()
        
        self.data_combiner.set_update_callback(on_data_updated)

    def create_inventory(self):
        """Cria um novo inventário"""
        dialog = tk.Toplevel(self)
        dialog.title("Novo Inventário")
        dialog.resizable(False, False)
        
        ttk.Label(dialog, text="Nome do Inventário:").grid(row=0, column=0, padx=5, pady=5, sticky=tk.E)
        name_entry = ttk.Entry(dialog, width=30)
        name_entry.grid(row=0, column=1, padx=5, pady=5)
        name_entry.focus_set()
        
        ttk.Label(dialog, text="Nome da Loja:").grid(row=1, column=0, padx=5, pady=5, sticky=tk.E)
        store_entry = ttk.Entry(dialog, width=30)
        store_entry.grid(row=1, column=1, padx=5, pady=5)
        
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
        
        # Usando lambda para capturar self automaticamente
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
            self.after(0, lambda: self.update_status(f"Erro: {str(e)}", error=True))

    def _update_display_stats(self):
        """Atualiza as estatísticas de exibição na barra de status"""
        try:
            current_data = self.inventory_view.current_data
            
            # Verificação segura dos dados
            if current_data is None:
                self.update_status("Nenhum dado carregado")
                return
            elif not isinstance(current_data, pd.DataFrame):
                self.update_status(f"Formato de dados inválido: {type(current_data)}")
                return
            elif current_data.empty:
                self.update_status("Planilha vazia - nada para exibir")
                return
                
            # Processamento dos dados (sua lógica original)
            df = current_data
            if 'DIFERENCA' in df.columns:
                pos_diff = (df['DIFERENCA'] > 0).sum()
                neg_diff = (df['DIFERENCA'] < 0).sum()
                status = (f"Dados carregados | Itens com excesso: {pos_diff} | "
                        f"Itens faltando: {neg_diff} | Total: {len(df)}")
            else:
                status = f"Dados carregados | Total: {len(df)}"
            
            self.update_status(status)

        except AttributeError as e:
            self.update_status("Erro: estrutura de dados inválida")
            logger.error(f"AttributeError in _update_display_stats: {str(e)}")
        except Exception as e:
            self.update_status("Erro ao calcular estatísticas")
            logger.error(f"Unexpected error in _update_display_stats: {str(e)}", exc_info=True)
        
        def load_data():
            try:
                data_path = self.inventory_manager.get_active_inventory_data_path()
                if not data_path:
                    return None, "Caminho de dados inválido"
                    
                combined_file = Path(data_path) / "combined_data.parquet"
                
                if not combined_file.exists():
                    return None, "Arquivo combinado não encontrado"
                    
                # Carrega apenas as colunas necessárias para melhor performance
                required_cols = [
                    'GTIN', 'Codigo', 'Descricao', 'Preco', 'Custo',
                    'Estoque', 'QNT_CONTADA', 'COD_BARRAS', 'Flag'
                ]
                
                try:
                    df = pd.read_parquet(combined_file, columns=required_cols)
                except Exception:
                    # Fallback: carrega tudo e filtra as colunas
                    df = pd.read_parquet(combined_file)
                    df = df[required_cols] if all(col in df.columns for col in required_cols) else pd.DataFrame(columns=required_cols)
                
                # Converte colunas numéricas
                numeric_cols = ['Estoque', 'QNT_CONTADA']
                for col in numeric_cols:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                
                if 'Estoque' in df.columns and 'QNT_CONTADA' in df.columns:
                    df['DIFERENCA'] = df['QNT_CONTADA'] - df['Estoque']
                
                if 'DIFERENCA' in df.columns:
                    df = df.sort_values('DIFERENCA', ascending=False)
                
                return df, None
                    
            except Exception as e:
                self.logger.error(f"Erro ao carregar dados: {e}", exc_info=True)
                return None, f"Erro ao carregar dados: {str(e)}"
        
        def on_data_loaded():
            with self._operation_in_progress():
                df, error = load_data()
                if error:
                    self.update_status(error)
                    return
                
                self._update_display(df)
        
        # Executa o carregamento em thread separada
        threading.Thread(
            target=on_data_loaded,
            daemon=True
        ).start()

    def _update_display(self, df: pd.DataFrame):
        """Atualiza a exibição dos dados na UI"""
        self.current_data = df
        self.inventory_view.display_data(df)
        
        if 'DIFERENCA' in df.columns:
            pos_diff = (df['DIFERENCA'] > 0).sum()
            neg_diff = (df['DIFERENCA'] < 0).sum()
            status = (f"Dados carregados | Itens com excesso: {pos_diff} | "
                     f"Itens faltando: {neg_diff} | Total: {len(df)}")
        else:
            status = f"Dados carregados | Total: {len(df)}"
        
        self.update_status(status)

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

    def manual_backup(self):
        """Executa um backup manual do inventário ativo"""
        if not self.inventory_manager.active_inventory_path:
            messagebox.showwarning("Aviso", "Nenhum inventário ativo para backup")
            return
        
        try:
            with self._operation_in_progress():
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
                        "Erro",
                        "Falha ao criar backup. Verifique os logs."
                    )
        except Exception as e:
            self.logger.error(f"Erro no backup manual: {e}", exc_info=True)
            messagebox.showerror(
                "Erro",
                f"Falha inesperada ao criar backup:\n{str(e)}"
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

    def _show_error(self, message: str):
        """Exibe uma mensagem de erro na UI"""
        self.update_status(message)
        messagebox.showerror("Erro", message)

    def _update_watcher_button(self):
        """Atualiza o texto do botão de monitoramento"""
        self.watcher_btn.config(
            text="Desativar Monitoramento" if self.watcher_active else "Ativar Monitoramento"
        )

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

    def _create_enhanced_context_menu(self):
        """Adiciona um menu de contexto avançado à visualização de dados"""
        self.enhanced_context_menu = tk.Menu(self, tearoff=0)
        #self.enhanced_context_menu.add_command(label="Copiar", command=self._copy_selected)
        #self.enhanced_context_menu.add_command(label="Exportar Seleção", command=self.export_selection)
        self.enhanced_context_menu.add_separator()
        self.enhanced_context_menu.add_command(label="Validar Dados", command=self._validate_selected_data)
        self.enhanced_context_menu.add_command(label="Estatísticas", command=self._show_data_stats)
        self.enhanced_context_menu.add_separator()
        self.enhanced_context_menu.add_command(label="Atualizar", command=self.refresh_data)

        if hasattr(self.inventory_view, 'bind_context_menu'):
            self.inventory_view.bind_context_menu(self.enhanced_context_menu)

    def _validate_selected_data(self):
        """Valida os dados selecionados com tratamento robusto"""
        try:
            selected = self.inventory_view.get_selected_rows()
            if not selected:
                messagebox.showwarning("Aviso", "Nenhum item selecionado para validação")
                return

            validation_errors = []
            gtin_errors = 0
            count_errors = 0
            
            for item in selected:
                values = item['values']
                
                # Valida GTIN
                if 'GTIN' in values and len(str(values['GTIN'])) != 13:
                    gtin_errors += 1
                    
                # Valida quantidade
                if 'QNT_CONTADA' in values and float(values['QNT_CONTADA']) < 0:
                    count_errors += 1
            
            errors = []
            if gtin_errors > 0:
                errors.append(f"{gtin_errors} GTINs inválidos")
            if count_errors > 0:
                errors.append(f"{count_errors} quantidades negativas")
            
            if errors:
                messagebox.showwarning("Problemas Encontrados", "\n".join(errors))
            else:
                messagebox.showinfo("Validação", "Todos os dados selecionados são válidos!")

        except Exception as e:
            self.logger.error(f"Erro na validação: {e}", exc_info=True)
            messagebox.showerror("Erro", f"Falha na validação:\n{str(e)}")

    def _show_data_stats(self):
        """Exibe estatísticas avançadas dos dados selecionados"""
        try:
            selected = self.inventory_view.get_selected_rows()
            if not selected:
                messagebox.showwarning("Aviso", "Nenhum item selecionado para análise")
                return

            stats_window = tk.Toplevel(self)
            stats_window.title("Estatísticas dos Dados Selecionados")
            stats_window.geometry("400x300")
            stats_window.resizable(False, False)

            main_frame = ttk.Frame(stats_window, padding="10")
            main_frame.pack(fill=tk.BOTH, expand=True)

            stats_text = tk.Text(main_frame, wrap=tk.WORD)
            stats_text.pack(fill=tk.BOTH, expand=True)

            # Converte para DataFrame para facilitar análise
            df = pd.DataFrame([item['values'] for item in selected])
            
            stats = [
                f"Total de itens selecionados: {len(selected)}",
                ""
            ]

            if 'QNT_CONTADA' in df.columns:
                try:
                    df['QNT_CONTADA'] = pd.to_numeric(df['QNT_CONTADA'])
                    stats.extend([
                        "Quantidades Contadas:",
                        f"- Média: {df['QNT_CONTADA'].mean():.2f}",
                        f"- Máxima: {df['QNT_CONTADA'].max()}",
                        f"- Mínima: {df['QNT_CONTADA'].min()}",
                        f"- Total: {df['QNT_CONTADA'].sum()}",
                        ""
                    ])
                except:
                    pass

            if 'DIFERENCA' in df.columns:
                try:
                    df['DIFERENCA'] = pd.to_numeric(df['DIFERENCA'])
                    positives = df[df['DIFERENCA'] > 0]
                    negatives = df[df['DIFERENCA'] < 0]
                    
                    stats.extend([
                        "Diferenças:",
                        f"- Itens com excesso: {len(positives)}",
                        f"- Itens faltando: {len(negatives)}",
                        f"- Maior divergência: {df['DIFERENCA'].max()}",
                        f"- Menor divergência: {df['DIFERENCA'].min()}",
                        f"- Diferença total: {df['DIFERENCA'].sum():+}"
                    ])
                except:
                    pass

            stats_text.insert(tk.END, "\n".join(stats))
            stats_text.config(state=tk.DISABLED)

            ttk.Button(
                main_frame,
                text="Fechar",
                command=stats_window.destroy
            ).pack(pady=10)

            self.center_window(stats_window)

        except Exception as e:
            self.logger.error(f"Erro ao gerar estatísticas: {e}", exc_info=True)
            messagebox.showerror("Erro", f"Falha ao calcular estatísticas:\n{str(e)}")

    def _start_auto_backup(self):
        """Inicia o serviço de backup automático"""
        if self.backup_running or not self.backup_config['auto_backup']:
            return
            
        self.backup_running = True
        
        def backup_loop():
            while self.backup_running:
                try:
                    if self.inventory_manager.active_inventory_path:
                        backup_path = self.inventory_manager.backup_inventory(
                            self.inventory_manager.active_inventory_path,
                            backup_dir="auto_backups"
                        )
                        if backup_path:
                            self.last_backup_time = datetime.now()
                            self.after(0, lambda: self.last_backup_var.set(
                                f"Último backup: {self.last_backup_time.strftime('%d/%m/%Y %H:%M')}"
                            ))
                            self._cleanup_old_backups()
                except Exception as e:
                    self.logger.error(f"Erro no backup automático: {e}", exc_info=True)
                
                for _ in range(self.backup_config['backup_interval']):
                    if not self.backup_running:
                        break
                    time.sleep(1)
        
        self.backup_thread = threading.Thread(target=backup_loop, daemon=True)
        self.backup_thread.start()

    def _cleanup_old_backups(self):
        """Remove backups antigos conforme configuração"""
        try:
            backup_dir = Path(self.inventory_manager.data_folder) / "auto_backups"
            if not backup_dir.exists():
                return
                
            backups = sorted(backup_dir.glob("*.zip"), key=os.path.getmtime, reverse=True)
            
            if len(backups) > self.backup_config['max_backups']:
                for old_backup in backups[self.backup_config['max_backups']:]:
                    try:
                        os.remove(old_backup)
                        self.logger.info(f"Backup antigo removido: {old_backup}")
                    except Exception as e:
                        self.logger.warning(f"Falha ao remover backup {old_backup}: {e}")
                        
        except Exception as e:
            self.logger.error(f"Erro na limpeza de backups: {e}", exc_info=True)

    def _on_close(self):
        """Método para lidar com o fechamento da janela principal"""
        self.backup_running = False
        try:
            if hasattr(self, 'data_combiner') and self.data_combiner:
                self.data_combiner.stop_watching()
            if hasattr(self, 'config_manager') and self.config_manager:
                self.config_manager.save_config()
        except Exception as e:
            self.logger.error(f"Erro ao encerrar: {e}")
        finally:
            self.destroy()

    def toggle_watcher(self):
        """Ativa/desativa o monitoramento automático"""
        if not hasattr(self, 'data_combiner') or not self.data_combiner:
            return
            
        self.watcher_active = not self.watcher_active
        self.config_manager.set("watcher.enabled", self.watcher_active)
        
        try:
            if self.watcher_active:
                interval = self.config_manager.get("watcher.interval", 2)
                self.data_combiner.start_watching(interval)
                message = "Monitoramento automático ativado"
            else:
                self.data_combiner.stop_watching()
                message = "Monitoramento automático desativado"
            
            self._update_watcher_button()
            self.update_status(message)
            messagebox.showinfo("Monitoramento", message)
        except Exception as e:
            self.logger.error(f"Erro ao alternar monitoramento: {e}", exc_info=True)
            messagebox.showerror(
                "Erro",
                f"Falha ao alternar monitoramento:\n{str(e)}"
            )

if __name__ == "__main__":
    app = MainWindow()
    app.mainloop()