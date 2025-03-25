import os
import threading
import time
import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import pandas as pd
from tkinter.scrolledtext import ScrolledText
import sys
from io import StringIO
import logging
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import queue
import traceback

class FileMonitor(FileSystemEventHandler):
    def __init__(self, app, inventory_name):
        self.app = app
        self.inventory_name = inventory_name
        self.logger = logging.getLogger(__name__)
        
    def on_created(self, event):
        if not event.is_directory and event.src_path.lower().endswith('.xlsx'):
            self.logger.info(f"Novo arquivo detectado: {event.src_path}")
            self.app.process_new_file(event.src_path, self.inventory_name)

class InventarioApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Sistema de Gerenciamento de Inventário - Versão Final")
        self.root.geometry("1100x800")
        
        # Configuração de estilo
        self.setup_styles()
        
        # Variáveis para armazenar dados
        self.nome_inventario = tk.StringVar()
        self.nome_loja = tk.StringVar()
        self.nome_pessoa = tk.StringVar()
        self.txt_file_path = tk.StringVar()
        self.selected_inventory = tk.StringVar()
        
        # Controle de monitoramento
        self.monitor_thread = None
        self.observer = None
        self.monitor_active = False
        self.file_queue = queue.Queue()
        
        # Configurar o notebook (abas)
        self.notebook = ttk.Notebook(root)
        self.notebook.pack(fill='both', expand=True, padx=10, pady=10)
        
        # Abas
        self.create_tab1()  # Criar Inventário
        self.create_tab2()  # Processar TXT
        self.create_tab3()  # Consolidar Dados
        self.create_tab4()  # Visualizar Dados
        self.create_tab5()  # Monitoramento
        
        # Barra de status
        self.status_var = tk.StringVar()
        self.status_var.set("Pronto")
        self.status_bar = ttk.Label(root, textvariable=self.status_var, relief='sunken')
        self.status_bar.pack(side='bottom', fill='x')
        
        # Configurar logging
        self.setup_logging()
        
        # Atualizar lista de inventários disponíveis
        self.update_inventory_list()
        
        # Iniciar thread para processar arquivos na fila
        self.start_file_processor()
    
    def setup_styles(self):
        """Configura estilos para a interface"""
        style = ttk.Style()
        style.theme_use('clam')
        style.configure('TFrame', background='#f0f0f0')
        style.configure('TLabel', background='#f0f0f0', font=('Arial', 10))
        style.configure('TButton', font=('Arial', 10))
        style.configure('TNotebook.Tab', font=('Arial', 10, 'bold'))
        style.configure('Header.TLabel', font=('Arial', 12, 'bold'))
        style.configure('Accent.TButton', foreground='white', background='#4a6ea9', font=('Arial', 10, 'bold'))
        style.configure('Alert.TLabel', foreground='red', font=('Arial', 10, 'bold'))
        style.configure('Success.TLabel', foreground='green', font=('Arial', 10, 'bold'))
    
    def setup_logging(self):
        """Configura o sistema de logging"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('inventario.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def create_tab1(self):
        """Cria a aba para criar novo inventário"""
        tab1 = ttk.Frame(self.notebook)
        self.notebook.add(tab1, text="1. Criar Inventário")
        
        # Frame principal
        main_frame = ttk.Frame(tab1)
        main_frame.pack(fill='both', expand=True, padx=20, pady=20)
        
        # Frame do formulário
        form_frame = ttk.LabelFrame(main_frame, text="Dados do Inventário")
        form_frame.pack(fill='x', pady=10)
        
        # Campos do formulário
        ttk.Label(form_frame, text="Nome do Inventário:").grid(row=0, column=0, padx=10, pady=5, sticky='e')
        ttk.Entry(form_frame, textvariable=self.nome_inventario, width=40).grid(row=0, column=1, padx=10, pady=5, sticky='w')
        
        ttk.Label(form_frame, text="Nome da Loja:").grid(row=1, column=0, padx=10, pady=5, sticky='e')
        ttk.Entry(form_frame, textvariable=self.nome_loja, width=40).grid(row=1, column=1, padx=10, pady=5, sticky='w')
        
        ttk.Label(form_frame, text="Responsável:").grid(row=2, column=0, padx=10, pady=5, sticky='e')
        ttk.Entry(form_frame, textvariable=self.nome_pessoa, width=40).grid(row=2, column=1, padx=10, pady=5, sticky='w')
        
        # Botão para criar inventário
        btn_frame = ttk.Frame(main_frame)
        btn_frame.pack(fill='x', pady=10)
        self.create_button = ttk.Button(btn_frame, text="Criar Inventário", command=self.criar_inventario, 
                                      style='Accent.TButton')
        self.create_button.pack(pady=10, ipadx=20, ipady=5)
        
        # Área de log
        log_frame = ttk.LabelFrame(main_frame, text="Log de Execução")
        log_frame.pack(fill='both', expand=True, pady=10)
        
        self.log_area = ScrolledText(log_frame, height=10, width=100, wrap=tk.WORD)
        self.log_area.pack(fill='both', expand=True, padx=5, pady=5)
        self.log_area.config(state='disabled')
    
    def create_tab2(self):
        """Cria a aba para processar arquivo TXT"""
        tab2 = ttk.Frame(self.notebook)
        self.notebook.add(tab2, text="2. Processar TXT")
        
        # Frame principal
        main_frame = ttk.Frame(tab2)
        main_frame.pack(fill='both', expand=True, padx=20, pady=20)
        
        # Frame de seleção de arquivo
        file_frame = ttk.LabelFrame(main_frame, text="Selecionar Arquivo TXT")
        file_frame.pack(fill='x', pady=10)
        
        ttk.Label(file_frame, text="Arquivo TXT:").grid(row=0, column=0, padx=10, pady=5, sticky='e')
        ttk.Entry(file_frame, textvariable=self.txt_file_path, width=60).grid(row=0, column=1, padx=10, pady=5)
        ttk.Button(file_frame, text="Procurar", command=self.browse_txt_file).grid(row=0, column=2, padx=10, pady=5)
        
        # Frame de seleção de inventário
        inv_frame = ttk.LabelFrame(main_frame, text="Selecionar Inventário Destino")
        inv_frame.pack(fill='x', pady=10)
        
        ttk.Label(inv_frame, text="Inventário:").grid(row=0, column=0, padx=10, pady=5, sticky='e')
        self.inventory_combobox = ttk.Combobox(inv_frame, textvariable=self.selected_inventory, width=58)
        self.inventory_combobox.grid(row=0, column=1, padx=10, pady=5)
        ttk.Button(inv_frame, text="Atualizar Lista", command=self.update_inventory_list).grid(row=0, column=2, padx=10, pady=5)
        
        # Botão para processar
        btn_frame = ttk.Frame(main_frame)
        btn_frame.pack(fill='x', pady=10)
        self.process_button = ttk.Button(btn_frame, text="Processar TXT", command=self.processar_txt, 
                                       style='Accent.TButton')
        self.process_button.pack(pady=10, ipadx=20, ipady=5)
        
        # Área de log
        log_frame = ttk.LabelFrame(main_frame, text="Log de Processamento")
        log_frame.pack(fill='both', expand=True, pady=10)
        
        self.process_log = ScrolledText(log_frame, height=10, width=100, wrap=tk.WORD)
        self.process_log.pack(fill='both', expand=True, padx=5, pady=5)
        self.process_log.config(state='disabled')
    
    def create_tab3(self):
        """Cria a aba para consolidar dados"""
        tab3 = ttk.Frame(self.notebook)
        self.notebook.add(tab3, text="3. Consolidar Dados")
        
        # Frame principal
        main_frame = ttk.Frame(tab3)
        main_frame.pack(fill='both', expand=True, padx=20, pady=20)
        
        # Frame de seleção
        select_frame = ttk.LabelFrame(main_frame, text="Selecionar Inventário")
        select_frame.pack(fill='x', pady=10)
        
        ttk.Label(select_frame, text="Inventário:").grid(row=0, column=0, padx=10, pady=5, sticky='e')
        self.consolidate_combobox = ttk.Combobox(select_frame, textvariable=self.selected_inventory, width=60)
        self.consolidate_combobox.grid(row=0, column=1, padx=10, pady=5)
        ttk.Button(select_frame, text="Atualizar Lista", command=self.update_inventory_list).grid(row=0, column=2, padx=10, pady=5)
        
        # Botão para consolidar
        btn_frame = ttk.Frame(main_frame)
        btn_frame.pack(fill='x', pady=10)
        self.consolidate_button = ttk.Button(btn_frame, text="Iniciar Consolidação", command=self.iniciar_consolidacao, 
                                           style='Accent.TButton')
        self.consolidate_button.pack(pady=10, ipadx=20, ipady=5)
        
        # Progress bar
        progress_frame = ttk.Frame(main_frame)
        progress_frame.pack(fill='x', pady=10)
        ttk.Label(progress_frame, text="Progresso:").pack(side='left', padx=10)
        self.progress = ttk.Progressbar(progress_frame, orient='horizontal', length=600, mode='determinate')
        self.progress.pack(fill='x', expand=True, padx=10)
        
        # Área de log
        log_frame = ttk.LabelFrame(main_frame, text="Log de Consolidação")
        log_frame.pack(fill='both', expand=True, pady=10)
        
        self.consolidate_log = ScrolledText(log_frame, height=10, width=100, wrap=tk.WORD)
        self.consolidate_log.pack(fill='both', expand=True, padx=5, pady=5)
        self.consolidate_log.config(state='disabled')
    
    def create_tab4(self):
        """Cria a aba para visualizar dados consolidados"""
        tab4 = ttk.Frame(self.notebook)
        self.notebook.add(tab4, text="4. Visualizar Dados")
        
        # Frame principal
        main_frame = ttk.Frame(tab4)
        main_frame.pack(fill='both', expand=True, padx=20, pady=20)
        
        # Frame de controle
        control_frame = ttk.Frame(main_frame)
        control_frame.pack(fill='x', pady=10)
        
        ttk.Label(control_frame, text="Selecione o Inventário:").pack(side='left', padx=10)
        self.view_combobox = ttk.Combobox(control_frame, textvariable=self.selected_inventory, width=40)
        self.view_combobox.pack(side='left', padx=10)
        ttk.Button(control_frame, text="Atualizar Lista", command=self.update_inventory_list).pack(side='left', padx=10)
        self.load_button = ttk.Button(control_frame, text="Carregar Dados", command=self.carregar_dados, 
                                    style='Accent.TButton')
        self.load_button.pack(side='right', padx=10)
        
        # Treeview para exibir os dados
        tree_frame = ttk.Frame(main_frame)
        tree_frame.pack(fill='both', expand=True)
        
        self.tree = ttk.Treeview(tree_frame)
        self.tree.pack(side='left', fill='both', expand=True)
        
        # Scrollbars
        ysb = ttk.Scrollbar(tree_frame, orient='vertical', command=self.tree.yview)
        ysb.pack(side='right', fill='y')
        self.tree.configure(yscroll=ysb.set)
        
        xsb = ttk.Scrollbar(main_frame, orient='horizontal', command=self.tree.xview)
        xsb.pack(fill='x')
        self.tree.configure(xscroll=xsb.set)
        
        # Frame de informações
        info_frame = ttk.Frame(main_frame)
        info_frame.pack(fill='x', pady=10)
        self.info_label = ttk.Label(info_frame, text="Selecione um inventário e clique em 'Carregar Dados'")
        self.info_label.pack()
    
    def create_tab5(self):
        """Cria a aba para monitoramento"""
        tab5 = ttk.Frame(self.notebook)
        self.notebook.add(tab5, text="5. Monitoramento")
        
        # Frame principal
        main_frame = ttk.Frame(tab5)
        main_frame.pack(fill='both', expand=True, padx=20, pady=20)
        
        # Frame de status
        status_frame = ttk.LabelFrame(main_frame, text="Status do Monitoramento")
        status_frame.pack(fill='x', pady=10)
        
        self.monitor_status = ttk.Label(status_frame, text="Monitoramento INATIVO", style='Alert.TLabel')
        self.monitor_status.pack(pady=5)
        
        self.current_inventory_label = ttk.Label(status_frame, text="Inventário monitorado: Nenhum")
        self.current_inventory_label.pack(pady=5)
        
        self.watch_folder_label = ttk.Label(status_frame, text="Pasta monitorada: Nenhuma")
        self.watch_folder_label.pack(pady=5)
        
        # Frame de controle
        control_frame = ttk.Frame(main_frame)
        control_frame.pack(fill='x', pady=10)
        
        self.stop_monitor_button = ttk.Button(control_frame, text="Parar Monitoramento", command=self.parar_monitoramento,
                                            style='Accent.TButton')
        self.stop_monitor_button.pack(side='right', padx=5)
        
        # Área de log
        log_frame = ttk.LabelFrame(main_frame, text="Log de Monitoramento")
        log_frame.pack(fill='both', expand=True, pady=10)
        
        self.monitor_log = ScrolledText(log_frame, height=15, width=100, wrap=tk.WORD)
        self.monitor_log.pack(fill='both', expand=True, padx=5, pady=5)
        self.monitor_log.config(state='disabled')
    
    def update_inventory_list(self):
        """Atualiza a lista de inventários disponíveis"""
        inventory_dir = os.path.join(os.getcwd(), "Inventário")
        if os.path.exists(inventory_dir):
            inventories = [d for d in os.listdir(inventory_dir) 
                          if os.path.isdir(os.path.join(inventory_dir, d))]
            inventories.sort()
            
            self.inventory_combobox['values'] = inventories
            self.consolidate_combobox['values'] = inventories
            self.view_combobox['values'] = inventories
            
            if inventories:
                self.selected_inventory.set(inventories[0])
                self.log_message(self.log_area, f"Inventários disponíveis: {', '.join(inventories)}")
            else:
                self.log_message(self.log_area, "Nenhum inventário encontrado.")
        else:
            self.log_message(self.log_area, "Diretório 'Inventário' não encontrado.")
    
    def browse_txt_file(self):
        """Abre diálogo para selecionar arquivo TXT"""
        file_path = filedialog.askopenfilename(
            title="Selecione o arquivo TXT",
            filetypes=[("Arquivos TXT", "*.txt"), ("Todos os arquivos", "*.*")]
        )
        if file_path:
            self.txt_file_path.set(file_path)
            self.log_message(self.process_log, f"Arquivo selecionado: {file_path}")
    
    def log_message(self, widget, message):
        """Adiciona mensagem ao widget de log especificado"""
        widget.config(state='normal')
        widget.insert('end', message + '\n')
        widget.see('end')
        widget.config(state='disabled')
        self.logger.info(message)
    
    def criar_inventario(self):
        """Cria um novo inventário e inicia o monitoramento"""
        nome = self.nome_inventario.get().strip()
        loja = self.nome_loja.get().strip()
        pessoa = self.nome_pessoa.get().strip()
        
        if not nome or not loja or not pessoa:
            messagebox.showerror("Erro", "Todos os campos são obrigatórios!")
            return
        
        # Desabilita o botão durante a criação
        self.create_button.config(state='disabled')
        self.status_var.set(f"Criando inventário: {nome}...")
        self.log_message(self.log_area, f"\n=== INICIANDO CRIAÇÃO DE INVENTÁRIO ===")
        self.log_message(self.log_area, f"Nome: {nome}")
        self.log_message(self.log_area, f"Loja: {loja}")
        self.log_message(self.log_area, f"Responsável: {pessoa}")
        
        def run():
            try:
                # Cria a estrutura de pastas
                self.criar_estrutura_pastas(nome, loja, pessoa)
                
                # Caminho da pasta de contagem na área de trabalho
                desktop = os.path.join(os.path.expanduser("~"), "Desktop")
                pasta_contagem = os.path.join(desktop, f"{nome} - Inserir Dados de Contagem")
                
                # Inicia o monitoramento
                self.iniciar_monitoramento(pasta_contagem, nome)
                
                # Atualiza a interface
                self.root.after(0, lambda: [
                    messagebox.showinfo(
                        "Sucesso", 
                        f"Inventário '{nome}' criado com sucesso!\n"
                        f"Pasta de contagem criada em:\n{pasta_contagem}"
                    ),
                    self.update_inventory_list(),
                    self.create_button.config(state='normal'),
                    self.status_var.set("Inventário criado com sucesso!")
                ])
                
            except Exception as e:
                error_msg = f"ERRO: {str(e)}\n{traceback.format_exc()}"
                self.logger.error(error_msg)
                self.root.after(0, lambda: [
                    messagebox.showerror(
                        "Erro", 
                        f"Falha ao criar inventário:\n{str(e)}"
                    ),
                    self.create_button.config(state='normal'),
                    self.status_var.set("Erro ao criar inventário"),
                    self.log_message(self.log_area, "=== ERRO NA CRIAÇÃO ==="),
                    self.log_message(self.log_area, error_msg)
                ])
                
            finally:
                self.root.after(3000, lambda: self.status_var.set("Pronto"))
        
        threading.Thread(target=run, daemon=True).start()
    
    def criar_estrutura_pastas(self, nome_inventario, nome_loja, nome_pessoa):
        """Cria a estrutura de pastas para o inventário"""
        pasta_principal = "Inventário"
        caminho_inventario = os.path.join(pasta_principal, nome_inventario)
        
        # Verifica se já existe
        if os.path.exists(caminho_inventario):
            raise Exception(f"O inventário '{nome_inventario}' já existe!")
        
        # Cria a estrutura de pastas
        os.makedirs(caminho_inventario)
        os.makedirs(os.path.join(caminho_inventario, "txt"))
        os.makedirs(os.path.join(caminho_inventario, "dados_processados"))
        
        # Cria arquivo de informações
        with open(os.path.join(caminho_inventario, "informacoes.txt"), 'w', encoding='utf-8') as f:
            f.write(f"Nome do Inventário: {nome_inventario}\n")
            f.write(f"Nome da Loja: {nome_loja}\n")
            f.write(f"Responsável: {nome_pessoa}\n")
        
        # Cria pasta na área de trabalho
        desktop = os.path.join(os.path.expanduser("~"), "Desktop")
        pasta_contagem = os.path.join(desktop, f"{nome_inventario} - Inserir Dados de Contagem")
        os.makedirs(pasta_contagem, exist_ok=True)
        
        self.logger.info(f"Estrutura criada com sucesso em: {caminho_inventario}")
        self.logger.info(f"Pasta de contagem criada na área de trabalho: {pasta_contagem}")
    
    def iniciar_monitoramento(self, pasta_contagem, nome_inventario):
        """Inicia o monitoramento com tratamento de erros melhorado"""
        try:
            if not os.path.exists(pasta_contagem):
                raise FileNotFoundError(f"A pasta de contagem não existe: {pasta_contagem}")
            
            # Para qualquer monitoramento existente
            if self.observer and self.observer.is_alive():
                self.observer.stop()
                self.observer.join()
            
            # Configura o novo monitoramento
            self.observer = Observer()
            event_handler = FileMonitor(self, nome_inventario)
            self.observer.schedule(event_handler, pasta_contagem, recursive=False)
            self.observer.start()
            self.monitor_active = True
            
            self.logger.info(f"Monitoramento iniciado para: {pasta_contagem}")
            self.log_message(self.monitor_log, f"\n=== MONITORAMENTO INICIADO ===")
            self.log_message(self.monitor_log, f"Data/Hora: {time.strftime('%d/%m/%Y %H:%M:%S')}")
            self.log_message(self.monitor_log, f"Pasta: {pasta_contagem}")
            self.log_message(self.monitor_log, f"Inventário: {nome_inventario}")
            
            # Atualiza status na interface
            self.root.after(0, lambda: [
                self.update_monitor_status(True, nome_inventario, pasta_contagem),
                messagebox.showinfo(
                    "Monitoramento Ativo",
                    f"Monitoramento iniciado com sucesso!\n"
                    f"Pasta: {pasta_contagem}\n"
                    f"Qualquer arquivo .xlsx adicionado será processado automaticamente."
                )
            ])
            
        except Exception as e:
            error_msg = f"Falha ao iniciar monitoramento: {str(e)}\n{traceback.format_exc()}"
            self.logger.error(error_msg)
            self.root.after(0, lambda: [
                messagebox.showerror(
                    "Erro no Monitoramento",
                    f"Não foi possível iniciar o monitoramento:\n{str(e)}"
                ),
                self.log_message(self.monitor_log, "\n=== ERRO NO MONITORAMENTO ==="),
                self.log_message(self.monitor_log, error_msg),
                self.update_monitor_status(False, "", "")
            ])
    
    def parar_monitoramento(self):
        """Para o monitoramento da pasta"""
        try:
            if self.observer and self.observer.is_alive():
                self.observer.stop()
                self.observer.join()
                self.monitor_active = False
                
                self.logger.info("Monitoramento parado com sucesso")
                self.log_message(self.monitor_log, "\n=== MONITORAMENTO PARADO ===")
                self.log_message(self.monitor_log, f"Data/Hora: {time.strftime('%d/%m/%Y %H:%M:%S')}")
                
                self.root.after(0, lambda: [
                    messagebox.showinfo(
                        "Monitoramento Parado",
                        "O monitoramento foi interrompido com sucesso."
                    ),
                    self.update_monitor_status(False, "", "")
                ])
            else:
                self.root.after(0, lambda: [
                    messagebox.showinfo(
                        "Monitoramento",
                        "Nenhum monitoramento ativo para parar."
                    )
                ])
                
        except Exception as e:
            error_msg = f"Erro ao parar monitoramento: {str(e)}\n{traceback.format_exc()}"
            self.logger.error(error_msg)
            self.root.after(0, lambda: [
                messagebox.showerror(
                    "Erro",
                    f"Falha ao parar monitoramento:\n{str(e)}"
                ),
                self.log_message(self.monitor_log, "\n=== ERRO AO PARAR MONITORAMENTO ==="),
                self.log_message(self.monitor_log, error_msg)
            ])
    
    def update_monitor_status(self, active, inventory_name, watch_folder):
        """Atualiza o status do monitoramento na interface"""
        if active:
            self.monitor_status.config(text="Monitoramento ATIVO", style='Success.TLabel')
            self.current_inventory_label.config(text=f"Inventário monitorado: {inventory_name}")
            self.watch_folder_label.config(text=f"Pasta monitorada: {watch_folder}")
            self.stop_monitor_button.config(state='normal')
        else:
            self.monitor_status.config(text="Monitoramento INATIVO", style='Alert.TLabel')
            self.current_inventory_label.config(text="Inventário monitorado: Nenhum")
            self.watch_folder_label.config(text="Pasta monitorada: Nenhuma")
            self.stop_monitor_button.config(state='disabled')
    
    def start_file_processor(self):
        """Inicia a thread que processa arquivos da fila"""
        def processor():
            while True:
                file_path, inventory_name = self.file_queue.get()
                try:
                    self.process_excel_file(file_path, inventory_name)
                except Exception as e:
                    error_msg = f"ERRO no processamento automático: {str(e)}\n{traceback.format_exc()}"
                    self.logger.error(error_msg)
                    self.root.after(0, lambda: [
                        self.log_message(self.monitor_log, "\n=== ERRO NO PROCESSAMENTO AUTOMÁTICO ==="),
                        self.log_message(self.monitor_log, error_msg),
                        self.status_var.set("Erro no processamento automático")
                    ])
                finally:
                    self.file_queue.task_done()
                    self.root.after(2000, lambda: self.status_var.set("Pronto"))
        
        thread = threading.Thread(target=processor, daemon=True)
        thread.start()
    
    def process_new_file(self, file_path, inventory_name):
        """Adiciona novo arquivo à fila de processamento"""
        self.file_queue.put((file_path, inventory_name))
        self.log_message(self.monitor_log, f"\nNovo arquivo detectado e adicionado à fila: {file_path}")
        self.status_var.set(f"Novo arquivo detectado: {os.path.basename(file_path)}")
    
    def process_excel_file(self, file_path, inventory_name):
        """Processa um arquivo Excel com tratamento de erros detalhado"""
        self.logger.info(f"Iniciando processamento do arquivo: {file_path}")
        self.root.after(0, lambda: [
            self.status_var.set(f"Processando {os.path.basename(file_path)}..."),
            self.log_message(self.monitor_log, f"\n=== INICIANDO PROCESSAMENTO ==="),
            self.log_message(self.monitor_log, f"Arquivo: {file_path}"),
            self.log_message(self.monitor_log, f"Inventário: {inventory_name}"),
            self.log_message(self.monitor_log, f"Data/Hora: {time.strftime('%d/%m/%Y %H:%M:%S')}")
        ])
        
        try:
            # Verifica se o arquivo existe e é válido
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"Arquivo não encontrado: {file_path}")
            
            if os.path.getsize(file_path) == 0:
                raise ValueError("O arquivo está vazio")
            
            # Caminhos dos arquivos
            dados_processados = os.path.join("Inventário", inventory_name, "dados_processados")
            os.makedirs(dados_processados, exist_ok=True)
            
            consolidados_file = os.path.join(dados_processados, "dados_consolidados.csv")
            output_file = os.path.join(dados_processados, "output.csv")
            
            # Lê o arquivo Excel
            try:
                df = pd.read_excel(file_path)
                self.log_message(self.monitor_log, "Arquivo Excel lido com sucesso")
            except Exception as e:
                raise ValueError(f"Erro ao ler arquivo Excel: {str(e)}")
            
            # Verifica colunas necessárias
            required_columns = ["LOJA KEY", "OPERADOR", "ENDEREÇO", "CÓD. BARRAS", "QNT. CONTADA"]
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                raise ValueError(f"Colunas obrigatórias faltando: {', '.join(missing_columns)}")
            
            # Processa os dados
            try:
                df_agrupado = (
                    df.groupby(["LOJA KEY", "ENDEREÇO", "CÓD. BARRAS"], as_index=False)
                    .agg({
                        "QNT. CONTADA": "sum",
                        "OPERADOR": lambda x: "/".join(sorted(set(x)))
                    })
                )
                
                # Converte tipos de dados
                df_agrupado["LOJA KEY"] = df_agrupado["LOJA KEY"].astype(int)
                df_agrupado["ENDEREÇO"] = df_agrupado["ENDEREÇO"].astype(int)
                df_agrupado["CÓD. BARRAS"] = df_agrupado["CÓD. BARRAS"].astype(int)
                df_agrupado["QNT. CONTADA"] = df_agrupado["QNT. CONTADA"].astype(float)
                
                self.log_message(self.monitor_log, "Dados processados e agrupados com sucesso")
            except Exception as e:
                raise ValueError(f"Erro ao processar dados: {str(e)}")
            
            # Atualiza ou cria o arquivo consolidado
            if os.path.exists(consolidados_file):
                try:
                    df_consolidado = pd.read_csv(consolidados_file, sep=";")
                    df_consolidado = pd.concat([df_consolidado, df_agrupado])
                    self.log_message(self.monitor_log, "Arquivo consolidado existente carregado para atualização")
                except Exception as e:
                    raise ValueError(f"Erro ao ler arquivo consolidado: {str(e)}")
            else:
                df_consolidado = df_agrupado
                self.log_message(self.monitor_log, "Criando novo arquivo consolidado")
            
            # Remove duplicatas
            try:
                df_consolidado = (
                    df_consolidado.groupby(["LOJA KEY", "ENDEREÇO", "CÓD. BARRAS"], as_index=False)
                    .agg({
                        "QNT. CONTADA": "sum",
                        "OPERADOR": lambda x: "/".join(sorted(set(x)))
                    })
                )
                self.log_message(self.monitor_log, "Duplicatas removidas com sucesso")
            except Exception as e:
                raise ValueError(f"Erro ao remover duplicatas: {str(e)}")
            
            # Salva o arquivo consolidado
            try:
                df_consolidado.to_csv(consolidados_file, index=False, sep=";")
                self.log_message(self.monitor_log, f"Arquivo consolidado salvo em: {consolidados_file}")
            except Exception as e:
                raise ValueError(f"Erro ao salvar arquivo consolidado: {str(e)}")
            
            # Se existir o arquivo output.csv, faz a consolidação final
            if os.path.exists(output_file):
                self.consolidar_dados_automatico(inventory_name)
            
            self.logger.info(f"Arquivo processado com sucesso: {file_path}")
            self.root.after(0, lambda: [
                self.log_message(self.monitor_log, "=== PROCESSAMENTO CONCLUÍDO ==="),
                self.log_message(self.monitor_log, "Arquivo processado com sucesso!"),
                self.status_var.set("Arquivo processado com sucesso!")
            ])
            
        except Exception as e:
            error_msg = f"ERRO ao processar {file_path}: {str(e)}\n{traceback.format_exc()}"
            self.logger.error(error_msg)
            self.root.after(0, lambda: [
                self.log_message(self.monitor_log, "=== ERRO NO PROCESSAMENTO ==="),
                self.log_message(self.monitor_log, error_msg),
                self.status_var.set("Erro no processamento automático")
            ])
            raise
    
    def consolidar_dados_automatico(self, inventory_name):
        """Consolida os dados com mensagens detalhadas"""
        self.logger.info(f"Iniciando consolidação automática para {inventory_name}")
        self.root.after(0, lambda: [
            self.status_var.set(f"Consolidando dados para {inventory_name}..."),
            self.log_message(self.monitor_log, f"\nIniciando consolidação automática para {inventory_name}")
        ])
        
        try:
            dados_dir = os.path.join("Inventário", inventory_name, "dados_processados")
            output_file = os.path.join(dados_dir, "output.csv")
            consolidados_file = os.path.join(dados_dir, "dados_consolidados.csv")
            dados_tela_file = os.path.join(dados_dir, "arquivo_exibir.csv")
            
            # Verifica se os arquivos existem
            if not os.path.exists(output_file):
                raise FileNotFoundError(f"Arquivo 'output.csv' não encontrado em {dados_dir}")
            if not os.path.exists(consolidados_file):
                raise FileNotFoundError(f"Arquivo 'dados_consolidados.csv' não encontrado em {dados_dir}")
            
            # Lê os arquivos
            try:
                df_output = pd.read_csv(output_file, sep=";")
                df_consolidados = pd.read_csv(consolidados_file, sep=";")
                
                # Limpa nomes de colunas
                df_output.columns = df_output.columns.str.strip()
                df_consolidados.columns = df_consolidados.columns.str.strip()
                
                self.log_message(self.monitor_log, "Arquivos lidos com sucesso")
            except Exception as e:
                raise ValueError(f"Erro ao ler arquivos para consolidação: {str(e)}")
            
            # Verifica colunas necessárias
            if "GTIN" not in df_output.columns:
                raise ValueError("Coluna 'GTIN' não encontrada no arquivo output.csv")
            if "CÓD. BARRAS" not in df_consolidados.columns:
                raise ValueError("Coluna 'CÓD. BARRAS' não encontrada no arquivo dados_consolidados.csv")
            
            # Converte tipos de dados
            try:
                df_output["GTIN"] = pd.to_numeric(df_output["GTIN"], errors="coerce")
                df_consolidados["CÓD. BARRAS"] = pd.to_numeric(df_consolidados["CÓD. BARRAS"], errors="coerce")
                self.log_message(self.monitor_log, "Dados convertidos para tipos numéricos")
            except Exception as e:
                raise ValueError(f"Erro ao converter tipos de dados: {str(e)}")
            
            # Combina os dados
            try:
                df_tela = pd.merge(
                    df_output,
                    df_consolidados,
                    left_on="GTIN",
                    right_on="CÓD. BARRAS",
                    how="outer"
                )
                self.log_message(self.monitor_log, "Dados combinados com sucesso")
            except Exception as e:
                raise ValueError(f"Erro ao combinar dados: {str(e)}")
            
            # Salva o resultado
            try:
                df_tela.to_csv(dados_tela_file, index=False, sep=";")
                self.logger.info(f"Dados consolidados salvos em: {dados_tela_file}")
                self.root.after(0, lambda: [
                    self.log_message(self.monitor_log, f"Consolidação concluída: {dados_tela_file}"),
                    self.status_var.set("Consolidação concluída!")
                ])
            except Exception as e:
                raise ValueError(f"Erro ao salvar arquivo consolidado: {str(e)}")
            
        except Exception as e:
            error_msg = f"ERRO na consolidação automática: {str(e)}\n{traceback.format_exc()}"
            self.logger.error(error_msg)
            self.root.after(0, lambda: [
                self.log_message(self.monitor_log, "=== ERRO NA CONSOLIDAÇÃO ==="),
                self.log_message(self.monitor_log, error_msg),
                self.status_var.set("Erro na consolidação automática")
            ])
            raise
    
    def processar_txt(self):
        """Processa o arquivo TXT selecionado com feedback visual melhorado"""
        txt_file = self.txt_file_path.get()
        inventory = self.selected_inventory.get()
        
        if not txt_file:
            messagebox.showerror("Erro", "Selecione um arquivo TXT!")
            return
        
        if not inventory:
            messagebox.showerror("Erro", "Selecione um inventário!")
            return
        
        # Desabilita o botão durante o processamento
        self.process_button.config(state='disabled')
        self.status_var.set(f"Processando {os.path.basename(txt_file)}...")
        self.log_message(self.process_log, f"\n=== INICIANDO PROCESSAMENTO ===")
        self.log_message(self.process_log, f"Arquivo: {txt_file}")
        self.log_message(self.process_log, f"Destino: {inventory}")
        self.log_message(self.process_log, f"Data/Hora: {time.strftime('%d/%m/%Y %H:%M:%S')}")
        
        def run():
            try:
                # Configura caminhos
                input_path = txt_file
                output_folder = os.path.join("Inventário", inventory, "dados_processados")
                os.makedirs(output_folder, exist_ok=True)
                output_path = os.path.join(output_folder, "output.csv")
                
                # Processa o arquivo
                start_time = time.time()
                df = self.processar_arquivo_txt(input_path, output_path)
                elapsed_time = time.time() - start_time
                
                # Atualiza a interface
                self.root.after(0, lambda: [
                    messagebox.showinfo(
                        "Sucesso",
                        f"Arquivo processado com sucesso!\n"
                        f"Registros: {len(df)}\n"
                        f"Tempo: {elapsed_time:.2f} segundos\n"
                        f"Salvo em: {output_path}"
                    ),
                    self.process_button.config(state='normal'),
                    self.status_var.set("Processamento concluído!"),
                    self.log_message(self.process_log, f"=== PROCESSAMENTO CONCLUÍDO ==="),
                    self.log_message(self.process_log, f"Tempo total: {elapsed_time:.2f} segundos"),
                    self.log_message(self.process_log, f"Registros processados: {len(df)}"),
                    self.log_message(self.process_log, f"Arquivo gerado: {output_path}")
                ])
                
            except Exception as e:
                error_msg = f"ERRO: {str(e)}\n{traceback.format_exc()}"
                self.logger.error(error_msg)
                self.root.after(0, lambda: [
                    messagebox.showerror("Erro no Processamento", f"Falha ao processar arquivo:\n{str(e)}"),
                    self.process_button.config(state='normal'),
                    self.status_var.set("Erro no processamento"),
                    self.log_message(self.process_log, "=== ERRO NO PROCESSAMENTO ==="),
                    self.log_message(self.process_log, error_msg)
                ])
                
            finally:
                self.root.after(3000, lambda: self.status_var.set("Pronto"))
        
        threading.Thread(target=run, daemon=True).start()
    
    def processar_arquivo_txt(self, input_path, output_path):
        """Processa o arquivo TXT e salva como CSV"""
        dados_tratados = []
        
        with open(input_path, 'r', encoding='latin-1') as file:
            for idx, linha in enumerate(file):
                try:
                    linha = linha.strip()
                    partes = linha.split()
                    
                    if len(partes) < 7:
                        self.logger.warning(f"Linha {idx+1} ignorada - menos de 7 colunas")
                        continue
                    
                    gtin = partes[0]
                    codigo_interno = partes[1]
                    secao = partes[-1]
                    custo = partes[-2]
                    estoque = partes[-3]
                    preco = partes[-4]
                    descricao = " ".join(partes[2:-4])
                    
                    dados_tratados.append([gtin, codigo_interno, descricao, preco, estoque, custo, secao])
                
                except Exception as e:
                    self.logger.error(f"Erro na linha {idx+1}: {str(e)}")
        
        # Cria DataFrame e salva como CSV
        df = pd.DataFrame(dados_tratados, columns=[
            "GTIN", "Código Interno", "Descrição", "Preço", "Estoque", "Custo", "Seção"
        ])
        
        # Converte colunas numéricas
        numeric_cols = ["GTIN", "Código Interno", "Preço", "Estoque", "Custo", "Seção"]
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        df.to_csv(output_path, index=False, sep=";", encoding='utf-8')
        self.logger.info(f"Arquivo CSV salvo em: {output_path}")
        
        return df
    
    def iniciar_consolidacao(self):
        """Inicia o processo de consolidação dos dados"""
        inventory = self.selected_inventory.get()
        
        if not inventory:
            messagebox.showerror("Erro", "Selecione um inventário!")
            return
        
        # Desabilita o botão durante a consolidação
        self.consolidate_button.config(state='disabled')
        self.status_var.set(f"Consolidando dados: {inventory}...")
        self.log_message(self.consolidate_log, f"\n=== INICIANDO CONSOLIDAÇÃO ===")
        self.log_message(self.consolidate_log, f"Inventário: {inventory}")
        self.log_message(self.consolidate_log, f"Data/Hora: {time.strftime('%d/%m/%Y %H:%M:%S')}")
        
        def run():
            try:
                # Configura caminhos
                dados_dir = os.path.join("Inventário", inventory, "dados_processados")
                output_file = os.path.join(dados_dir, "output.csv")
                consolidados_file = os.path.join(dados_dir, "dados_consolidados.csv")
                dados_tela_file = os.path.join(dados_dir, "arquivo_exibir.csv")
                
                # Verifica se os arquivos existem
                if not os.path.exists(output_file):
                    raise FileNotFoundError(f"Arquivo 'output.csv' não encontrado em {dados_dir}")
                if not os.path.exists(consolidados_file):
                    raise FileNotFoundError(f"Arquivo 'dados_consolidados.csv' não encontrado em {dados_dir}")
                
                # Atualiza progresso
                for i in range(1, 11):
                    time.sleep(0.3)  # Simula processamento
                    self.progress['value'] = i * 10
                    self.root.update_idletasks()
                
                # Processa os arquivos
                df_output = pd.read_csv(output_file, sep=";")
                df_consolidados = pd.read_csv(consolidados_file, sep=";")
                
                # Limpa nomes de colunas
                df_output.columns = df_output.columns.str.strip()
                df_consolidados.columns = df_consolidados.columns.str.strip()
                
                self.log_message(self.consolidate_log, "Arquivos carregados com sucesso")
                
                # Combina os dados
                df_tela = pd.merge(
                    df_output,
                    df_consolidados,
                    left_on="GTIN",
                    right_on="CÓD. BARRAS",
                    how="outer"
                )
                
                # Salva o resultado
                df_tela.to_csv(dados_tela_file, index=False, sep=";")
                
                # Atualiza a interface
                self.root.after(0, lambda: [
                    messagebox.showinfo(
                        "Sucesso",
                        f"Dados consolidados com sucesso!\n"
                        f"Arquivo gerado: {dados_tela_file}"
                    ),
                    self.consolidate_button.config(state='normal'),
                    self.status_var.set("Consolidação concluída!"),
                    self.log_message(self.consolidate_log, "=== CONSOLIDAÇÃO CONCLUÍDA ==="),
                    self.log_message(self.consolidate_log, f"Arquivo gerado: {dados_tela_file}")
                ])
                
            except Exception as e:
                error_msg = f"ERRO: {str(e)}\n{traceback.format_exc()}"
                self.logger.error(error_msg)
                self.root.after(0, lambda: [
                    messagebox.showerror(
                        "Erro",
                        f"Falha na consolidação:\n{str(e)}"
                    ),
                    self.consolidate_button.config(state='normal'),
                    self.status_var.set("Erro na consolidação"),
                    self.log_message(self.consolidate_log, "=== ERRO NA CONSOLIDAÇÃO ==="),
                    self.log_message(self.consolidate_log, error_msg)
                ])
                
            finally:
                self.progress['value'] = 0
                self.root.after(3000, lambda: self.status_var.set("Pronto"))
        
        threading.Thread(target=run, daemon=True).start()
    
    def carregar_dados(self):
        """Carrega os dados consolidados para visualização"""
        inventory = self.selected_inventory.get()
        
        if not inventory:
            messagebox.showerror("Erro", "Selecione um inventário!")
            return
        
        csv_path = os.path.join("Inventário", inventory, "dados_processados", "arquivo_exibir.csv")
        
        if not os.path.exists(csv_path):
            messagebox.showerror("Erro", f"Arquivo não encontrado:\n{csv_path}")
            return
        
        # Desabilita o botão durante o carregamento
        self.load_button.config(state='disabled')
        self.status_var.set(f"Carregando dados: {inventory}...")
        
        try:
            # Limpa a treeview
            for item in self.tree.get_children():
                self.tree.delete(item)
            self.tree["columns"] = []
            
            # Lê o arquivo CSV
            df = pd.read_csv(csv_path, sep=";")
            
            # Configura as colunas da treeview
            self.tree["columns"] = list(df.columns)
            for col in df.columns:
                self.tree.heading(col, text=col)
                self.tree.column(col, width=120, anchor='center', minwidth=50)
            
            # Adiciona os dados
            for _, row in df.iterrows():
                self.tree.insert("", "end", values=list(row))
            
            # Atualiza informações
            self.info_label.config(text=f"Inventário: {inventory} | Registros: {len(df)}")
            
            self.root.after(0, lambda: [
                messagebox.showinfo(
                    "Sucesso",
                    f"Dados carregados com sucesso!\n"
                    f"Registros: {len(df)}"
                ),
                self.load_button.config(state='normal'),
                self.status_var.set("Dados carregados com sucesso!")
            ])
            
        except Exception as e:
            error_msg = f"ERRO: {str(e)}\n{traceback.format_exc()}"
            self.logger.error(error_msg)
            self.root.after(0, lambda: [
                messagebox.showerror("Erro", f"Falha ao carregar dados:\n{str(e)}"),
                self.load_button.config(state='normal'),
                self.status_var.set("Erro ao carregar dados"),
                self.log_message(self.consolidate_log, "=== ERRO AO CARREGAR DADOS ==="),
                self.log_message(self.consolidate_log, error_msg)
            ])
            
        finally:
            self.root.after(3000, lambda: self.status_var.set("Pronto"))
    
    def on_closing(self):
        """Executa ao fechar a aplicação"""
        try:
            # Para o monitoramento se estiver ativo
            if self.observer and self.observer.is_alive():
                self.observer.stop()
                self.observer.join()
            
            # Fecha a aplicação
            self.root.destroy()
        except Exception as e:
            self.logger.error(f"Erro ao fechar aplicação: {str(e)}")
            self.root.destroy()

if __name__ == "__main__":
    root = tk.Tk()
    
    # Configura um estilo moderno
    style = ttk.Style()
    style.theme_use('clam')
    
    app = InventarioApp(root)
    root.protocol("WM_DELETE_WINDOW", app.on_closing)
    root.mainloop()