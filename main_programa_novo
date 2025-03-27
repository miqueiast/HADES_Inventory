import threading
import time
import pandas as pd
import tkinter as tk
from tkinter import ttk, messagebox, filedialog
from tkinter.scrolledtext import ScrolledText
import os
import json
import logging
import traceback
from datetime import datetime
import unicodedata
import re

class InventoryManager:
    def __init__(self):
        self.inventories_root = "Inventário"
        os.makedirs(self.inventories_root, exist_ok=True)
        
    def create_inventory(self, name, store, creator):
        """Cria um novo inventário"""
        inventory_path = os.path.join(self.inventories_root, name)
        if os.path.exists(inventory_path):
            raise Exception(f"Inventário '{name}' já existe!")
        
        os.makedirs(inventory_path)
        os.makedirs(os.path.join(inventory_path, "dados"))
        
        inventory_data = {
            "name": name,
            "store": store,
            "creator": creator,
            "path": inventory_path,
            "created_at": datetime.now().isoformat(),
            "last_modified": datetime.now().isoformat(),
            "data_file": None
        }
        
        with open(os.path.join(inventory_path, "metadata.json"), "w") as f:
            json.dump(inventory_data, f, indent=4)
            
        return inventory_data
    
    def load_inventory(self, inventory_path):
        """Carrega um inventário existente"""
        with open(os.path.join(inventory_path, "metadata.json"), "r") as f:
            return json.load(f)
    
    def save_inventory(self, inventory_data):
        """Salva os metadados do inventário"""
        with open(os.path.join(inventory_data["path"], "metadata.json"), "w") as f:
            json.dump(inventory_data, f, indent=4)
    
    def list_inventories(self):
        """Lista todos os inventários disponíveis"""
        return [d for d in os.listdir(self.inventories_root) 
               if os.path.isdir(os.path.join(self.inventories_root, d))]

class FileProcessor:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.produtos_atencao = None
        
        # Padrão regex para arquivos TXT
        self.txt_pattern = re.compile(
            r'^(?P<gtin>\d{13})\s+'          # GTIN (13 dígitos)
            r'(?P<codigo>\S+)\s+'            # Código interno
            r'(?P<descricao>.+?)(?=\s+000)|(?P<descricao2>.+?)(?=\s{2,})'  # Descrição
            r'(?:\s+000)?\s+'                # Consome o padrão de término
            r'(?P<preco>\d{8})\s+'           # Preço (8 dígitos)
            r'(?P<desconto>\d{8})\s+'        # Desconto (8 dígitos)
            r'(?P<custo>\d{8})\s+'           # Custo (8 dígitos)
            r'(?P<secao>\d{5})$'             # Seção (5 dígitos)
        )

    def process_inventory_txt(self, file_path: str):
        """Processa arquivo TXT com tratamento especial para descrição e zeros"""
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                lines = file.readlines()
            
            data = []
            line_count = 0
            errors = 0
            
            for line in lines:
                line_count += 1
                line = line.strip()
                if not line:
                    continue
                
                match = self.txt_pattern.match(line)
                if not match:
                    errors += 1
                    self.logger.warning(f"Linha {line_count} não corresponde ao padrão: {line[:50]}...")
                    continue
                
                descricao = match.group('descricao') or match.group('descricao2')
                
                item = {
                    'GTIN': match.group('gtin'),
                    'Código Interno': match.group('codigo').lstrip('0'),
                    'Descrição': descricao.strip(),
                    'Preço': float(match.group('preco')) / 100,
                    'Desconto': float(match.group('desconto')) / 100,
                    'Custo': float(match.group('custo')) / 100,
                    'Seção': match.group('secao').lstrip('0'),
                    'Linha Original': line
                }
                data.append(item)
            
            if not data:
                return False, None, "Nenhum dado válido encontrado no arquivo"
            
            df = pd.DataFrame(data)
            
            str_cols = df.select_dtypes(include='object').columns
            for col in str_cols:
                if col != 'Descrição':
                    df[col] = df[col].astype(str).str.lstrip('0')
            
            self.logger.info(f"Arquivo processado com {len(df)} itens válidos e {errors} erros")
            return True, df, None
            
        except Exception as e:
            error_msg = f"Erro ao processar TXT: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            return False, None, error_msg

    def save_to_parquet(self, df: pd.DataFrame, output_dir: str, file_name: str):
        """Salva DataFrame em Parquet removendo zeros à esquerda"""
        try:
            os.makedirs(output_dir, exist_ok=True)
            output_path = os.path.join(output_dir, f"{file_name}.parquet")
            
            str_cols = df.select_dtypes(include='object').columns
            for col in str_cols:
                if col != 'Descrição':
                    df[col] = df[col].astype(str).str.lstrip('0')
            
            dtype_mapping = {
                'GTIN': 'string',
                'Código Interno': 'string',
                'Descrição': 'string',
                'Seção': 'string',
                'Linha Original': 'string'
            }
            
            for col, dtype in dtype_mapping.items():
                if col in df.columns:
                    df[col] = df[col].astype(dtype)
            
            df.to_parquet(
                output_path,
                engine='pyarrow',
                compression='snappy',
                index=False
            )
            
            return True, output_path
            
        except Exception as e:
            error_msg = f"Erro ao salvar Parquet: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            return False, error_msg

    def carregar_produtos_atencao(self) -> bool:
        """Carrega lista de produtos que precisam de atenção especial"""
        try:
            produtos = {
                'GTIN': ['7891234567890', '7899876543210'],
                'flag': [1, 1]
            }
            self.produtos_atencao = pd.DataFrame(produtos)
            return True
        except Exception as e:
            self.logger.error(f"Erro ao carregar produtos de atenção: {str(e)}")
            return False

class InventoryApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Sistema de Gerenciamento de Inventário - HADES Ambiente Monte Olimpo (®)")
        self.root.geometry("1100x800")
        
        # Variáveis de controle
        self.current_page = 0
        self.rows_per_page = 500
        self.total_rows = 0
        self.current_filter = ""
        self.unsaved_changes = False
        
        # Inicializa componentes
        self.inventory_manager = InventoryManager()
        self.file_processor = FileProcessor()
        self.current_inventory = None
        
        # Configurações iniciais
        self.setup_styles()
        self.setup_logging()
        self.setup_variables()
        self.setup_ui()
        
        # Configura autosalvamento
        self.auto_save_interval = 300000  # 5 minutos em milissegundos
        self.setup_auto_save()
        
        # Configura o fechamento da janela
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)
    
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
        style.configure('Treeview', font=('Arial', 9), rowheight=25)
        style.configure('Treeview.Heading', font=('Arial', 9, 'bold'))
    
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
    
    def setup_variables(self):
        """Configura variáveis de controle"""
        self.nome_inventario = tk.StringVar()
        self.nome_loja = tk.StringVar()
        self.nome_pessoa = tk.StringVar()
        self.txt_file_path = tk.StringVar()
        self.selected_inventory = tk.StringVar()
        self.status_var = tk.StringVar()
        self.status_var.set("Pronto")
    
    def setup_ui(self):
        """Configura a interface do usuário"""
        # Barra de status
        self.status_bar = ttk.Label(self.root, textvariable=self.status_var, relief='sunken')
        self.status_bar.pack(side='bottom', fill='x')
        
        # Notebook (abas)
        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(fill='both', expand=True, padx=10, pady=10)
        
        # Cria as abas
        self.create_tab_gerenciar()
        self.create_tab_processar_arquivos()
        self.create_tab_visualizar()
    
    def create_tab_gerenciar(self):
        """Cria a aba para gerenciar inventários"""
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="1. Gerenciar Inventário")
        
        # Frame principal
        main_frame = ttk.Frame(tab)
        main_frame.pack(fill='both', expand=True, padx=20, pady=20)
        
        # Frame de seleção de inventário existente
        select_frame = ttk.LabelFrame(main_frame, text="Selecionar Inventário Existente")
        select_frame.pack(fill='x', pady=10)
        
        ttk.Label(select_frame, text="Inventário:").grid(row=0, column=0, padx=10, pady=5, sticky='e')
        self.inventory_combobox = ttk.Combobox(select_frame, textvariable=self.selected_inventory, width=40)
        self.inventory_combobox.grid(row=0, column=1, padx=10, pady=5, sticky='w')
        ttk.Button(select_frame, text="Carregar", command=self.carregar_inventario).grid(row=0, column=2, padx=10, pady=5)
        ttk.Button(select_frame, text="Atualizar Lista", command=self.update_inventory_list).grid(row=0, column=3, padx=10, pady=5)
        
        # Frame do formulário para novo inventário
        form_frame = ttk.LabelFrame(main_frame, text="Criar Novo Inventário")
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
        
        # Área de informações do inventário atual
        info_frame = ttk.LabelFrame(main_frame, text="Informações do Inventário Atual")
        info_frame.pack(fill='both', expand=True, pady=10)
        
        self.inventory_info = ScrolledText(info_frame, height=10, width=100, wrap=tk.WORD)
        self.inventory_info.pack(fill='both', expand=True, padx=5, pady=5)
        self.inventory_info.config(state='disabled')
        self.update_inventory_info()
    
    def create_tab_processar_arquivos(self):
        """Cria a aba para processar arquivos (TXT e Excel/CSV)"""
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="2. Processar Arquivos")
        
        # Frame principal
        main_frame = ttk.Frame(tab)
        main_frame.pack(fill='both', expand=True, padx=20, pady=20)
        
        # Frame para upload de arquivos da empresa
        upload_frame = ttk.LabelFrame(main_frame, text="Fazer upload de arquivos da empresa de inventário")
        upload_frame.pack(fill='x', pady=10)
        
        ttk.Button(upload_frame, text="Upload de Arquivo (Excel/CSV)", 
                  command=self.processar_arquivo_empresa, 
                  style='Accent.TButton').pack(pady=10, ipadx=20, ipady=5)
        
        # Frame de seleção de arquivo TXT
        file_frame = ttk.LabelFrame(main_frame, text="Processar Arquivo TXT")
        file_frame.pack(fill='x', pady=10)
        
        ttk.Label(file_frame, text="Arquivo TXT:").grid(row=0, column=0, padx=10, pady=5, sticky='e')
        ttk.Entry(file_frame, textvariable=self.txt_file_path, width=60).grid(row=0, column=1, padx=10, pady=5)
        ttk.Button(file_frame, text="Procurar", command=self.browse_txt_file).grid(row=0, column=2, padx=10, pady=5)
        
        # Frame de seleção de inventário
        inv_frame = ttk.LabelFrame(main_frame, text="Selecionar Inventário Destino")
        inv_frame.pack(fill='x', pady=10)
        
        ttk.Label(inv_frame, text="Inventário:").grid(row=0, column=0, padx=10, pady=5, sticky='e')
        self.inventory_combobox_process = ttk.Combobox(inv_frame, textvariable=self.selected_inventory, width=58)
        self.inventory_combobox_process.grid(row=0, column=1, padx=10, pady=5)
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
    
    def create_tab_visualizar(self):
        """Cria a aba para visualizar dados com paginação"""
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="3. Visualizar Dados")
        
        main_frame = ttk.Frame(tab)
        main_frame.pack(fill='both', expand=True, padx=20, pady=20)
        
        # Frame de controle
        control_frame = ttk.Frame(main_frame)
        control_frame.pack(fill='x', pady=10)
        
        ttk.Label(control_frame, text="Inventário Ativo:").pack(side='left', padx=10)
        self.current_inventory_label = ttk.Label(control_frame, text="Nenhum", font=('Arial', 10, 'bold'))
        self.current_inventory_label.pack(side='left', padx=10)
        
        # Controles de paginação
        pagination_frame = ttk.Frame(control_frame)
        pagination_frame.pack(side='right')
        
        ttk.Button(pagination_frame, text="<<", command=self.first_page).pack(side='left')
        ttk.Button(pagination_frame, text="<", command=self.prev_page).pack(side='left')
        ttk.Button(pagination_frame, text=">", command=self.next_page).pack(side='left')
        ttk.Button(pagination_frame, text=">>", command=self.last_page).pack(side='left')
        
        # Controle de filtro
        filter_frame = ttk.Frame(control_frame)
        filter_frame.pack(side='right', padx=10)
        
        ttk.Label(filter_frame, text="Filtrar:").pack(side='left')
        self.filter_entry = ttk.Entry(filter_frame, width=20)
        self.filter_entry.pack(side='left', padx=5)
        ttk.Button(filter_frame, text="Aplicar", command=self.apply_filter).pack(side='left')
        
        # Treeview
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
        self.info_label = ttk.Label(info_frame, text="Selecione um inventário na aba 'Gerenciar Inventário'")
        self.info_label.pack()
    
    def update_inventory_list(self):
        """Atualiza a lista de inventários disponíveis"""
        inventories = self.inventory_manager.list_inventories()
        inventories.sort()
        
        self.inventory_combobox['values'] = inventories
        self.inventory_combobox_process['values'] = inventories
        
        if inventories:
            self.selected_inventory.set(inventories[0])
            self.log_message(self.process_log, f"Inventários disponíveis: {', '.join(inventories)}")
        else:
            self.log_message(self.process_log, "Nenhum inventário encontrado.")
    
    def carregar_inventario(self):
        """Carrega um inventário existente"""
        inventory_name = self.selected_inventory.get()
        if not inventory_name:
            messagebox.showerror("Erro", "Selecione um inventário!")
            return
        
        try:
            inventory_path = os.path.join(self.inventory_manager.inventories_root, inventory_name)
            self.current_inventory = self.inventory_manager.load_inventory(inventory_path)
            
            self.update_inventory_info()
            self.current_inventory_label.config(text=inventory_name)
            
            if 'data_file' in self.current_inventory:
                if self.current_inventory['data_file'].endswith('.parquet'):
                    df = pd.read_parquet(self.current_inventory['data_file'])
                    self.display_tracking_data(df)
            
            messagebox.showinfo("Sucesso", f"Inventário '{inventory_name}' carregado com sucesso!")
            
        except Exception as e:
            messagebox.showerror("Erro", f"Falha ao carregar inventário:\n{str(e)}")
    
    def update_inventory_info(self):
        """Atualiza as informações do inventário na interface"""
        if self.current_inventory:
            info_text = f"=== {self.current_inventory['name']} ===\n\n"
            info_text += f"Loja: {self.current_inventory['store']}\n"
            info_text += f"Criado por: {self.current_inventory.get('creator', 'N/A')}\n"
            info_text += f"Criado em: {self.current_inventory['created_at']}\n"
            info_text += f"Última modificação: {self.current_inventory['last_modified']}\n"
            
            if 'data_file' in self.current_inventory:
                info_text += f"\nArquivo de dados: {os.path.basename(self.current_inventory['data_file'])}"
                if self.current_inventory['data_file'].endswith('.parquet'):
                    try:
                        df = pd.read_parquet(self.current_inventory['data_file'])
                        info_text += f"\nTotal de itens: {len(df)}"
                    except Exception as e:
                        info_text += f"\nErro ao ler dados: {str(e)}"
            
            self.inventory_info.config(state='normal')
            self.inventory_info.delete(1.0, tk.END)
            self.inventory_info.insert(tk.END, info_text)
            self.inventory_info.config(state='disabled')
        else:
            self.inventory_info.config(state='normal')
            self.inventory_info.delete(1.0, tk.END)
            self.inventory_info.insert(tk.END, "Nenhum inventário carregado. Selecione ou crie um inventário.")
            self.inventory_info.config(state='disabled')
    
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
        """Cria um novo inventário"""
        name = self.nome_inventario.get().strip()
        store = self.nome_loja.get().strip()
        creator = self.nome_pessoa.get().strip()
        
        if not name or not store or not creator:
            messagebox.showerror("Erro", "Preencha todos os campos!")
            return
        
        try:
            self.current_inventory = self.inventory_manager.create_inventory(name, store, creator)
            self.update_inventory_list()
            self.update_inventory_info()
            self.current_inventory_label.config(text=name)
            
            messagebox.showinfo("Sucesso", f"Inventário '{name}' criado com sucesso!")
            
        except Exception as e:
            self.log_message(self.process_log, f"Erro ao criar inventário: {str(e)}")
            messagebox.showerror("Erro", f"Não foi possível criar o inventário:\n{str(e)}")
    
    def processar_txt(self):
        """Processa o arquivo TXT selecionado de forma otimizada"""
        if not self.current_inventory:
            messagebox.showerror("Erro", "Crie ou carregue um inventário primeiro!")
            return
        
        file_path = self.txt_file_path.get()
        if not file_path:
            messagebox.showerror("Erro", "Selecione um arquivo TXT!")
            return
        
        self.process_button.config(state='disabled')
        self.status_var.set(f"Processando {os.path.basename(file_path)}...")
        self.log_message(self.process_log, f"\n=== INICIANDO PROCESSAMENTO ===")
        self.log_message(self.process_log, f"Arquivo: {file_path}")
        self.log_message(self.process_log, f"Data/Hora: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
        
        def run_processing():
            try:
                start_time = time.time()
                success, df, error = self.file_processor.process_inventory_txt(file_path)
                
                if not success:
                    raise Exception(error)
                
                self.log_message(self.process_log, f"Arquivo processado em {time.time() - start_time:.2f} segundos")
                self.log_message(self.process_log, f"Total de itens válidos: {len(df)}")
                
                if self.file_processor.carregar_produtos_atencao():
                    produtos_atencao = self.file_processor.produtos_atencao
                    df = df.merge(produtos_atencao, on='GTIN', how='left')
                    df['flag'] = df['flag'].fillna(0)
                    self.log_message(self.process_log, "Dados de atenção mesclados com sucesso")
                
                dados_dir = os.path.join(self.current_inventory['path'], 'dados')
                file_name = os.path.splitext(os.path.basename(file_path))[0]
                
                save_start = time.time()
                success, output_path = self.file_processor.save_to_parquet(df, dados_dir, file_name)
                
                if not success:
                    raise Exception(output_path)
                
                self.log_message(self.process_log, f"Arquivo salvo como Parquet em {time.time() - save_start:.2f} segundos")
                self.log_message(self.process_log, f"Tamanho do arquivo: {os.path.getsize(output_path)/1024/1024:.2f} MB")
                
                self.current_inventory['last_modified'] = datetime.now().isoformat()
                self.current_inventory['data_file'] = output_path
                self.inventory_manager.save_inventory(self.current_inventory)
                
                self.root.after(0, lambda: self.display_tracking_data(df))
                
                self.log_message(self.process_log, "=== PROCESSAMENTO CONCLUÍDO ===")
                self.log_message(self.process_log, f"Tempo total: {time.time() - start_time:.2f} segundos")
                
                self.root.after(0, lambda: messagebox.showinfo(
                    "Sucesso", 
                    f"Arquivo processado com sucesso!\n"
                    f"Registros: {len(df)}\n"
                    f"Tempo total: {time.time() - start_time:.2f} segundos"
                ))
                
            except Exception as e:
                error_msg = f"ERRO: {str(e)}\n{traceback.format_exc()}"
                self.log_message(self.process_log, "=== ERRO NO PROCESSAMENTO ===")
                self.log_message(self.process_log, error_msg)
                
                self.root.after(0, lambda: messagebox.showerror(
                    "Erro no Processamento", 
                    f"Falha ao processar arquivo:\n{str(e)}"
                ))
                
            finally:
                self.root.after(0, lambda: [
                    self.process_button.config(state='normal'),
                    self.status_var.set("Pronto")
                ])
        
        threading.Thread(target=run_processing, daemon=True).start()
    
    def processar_arquivo_empresa(self):
        """Processa arquivos Excel/CSV enviados pela empresa de inventário"""
        try:
            self.root.withdraw()
            
            arquivo = filedialog.askopenfilename(
                title="Selecione o arquivo Excel ou CSV",
                filetypes=[("Arquivos Excel", "*.xlsx;*.xls"), ("Arquivos CSV", "*.csv")]
            )
            
            if not arquivo:
                self.log_message(self.process_log, "Nenhum arquivo foi selecionado.")
                self.root.deiconify()
                return

            pasta_inventario = os.path.join(os.getcwd(), "Inventário")
            if not os.path.exists(pasta_inventario):
                self.log_message(self.process_log, f'A pasta "Inventário" não foi encontrada no diretório: {os.getcwd()}')
                self.root.deiconify()
                return

            subpastas = [f for f in os.listdir(pasta_inventario) if os.path.isdir(os.path.join(pasta_inventario, f))]
            if not subpastas:
                self.log_message(self.process_log, 'Não há subpastas na pasta "Inventário".')
                self.root.deiconify()
                return

            self.select_window = tk.Toplevel(self.root)
            self.select_window.title("Selecionar Inventário")
            self.select_window.geometry("400x300")
            
            ttk.Label(self.select_window, text="Selecione o inventário de destino:").pack(pady=10)
            
            self.subpasta_var = tk.StringVar()
            for subpasta in subpastas:
                ttk.Radiobutton(self.select_window, text=subpasta, variable=self.subpasta_var, value=subpasta).pack(anchor='w', padx=20)
            
            ttk.Button(self.select_window, text="Confirmar", command=lambda: self.finalizar_processamento_empresa(arquivo)).pack(pady=20)
            
        except Exception as e:
            self.log_message(self.process_log, f"Erro ao processar arquivo da empresa: {str(e)}")
            self.root.deiconify()
    
    def finalizar_processamento_empresa(self, arquivo):
        """Finaliza o processamento do arquivo da empresa"""
        try:
            subpasta_escolhida = self.subpasta_var.get()
            if not subpasta_escolhida:
                messagebox.showerror("Erro", "Selecione um inventário de destino!")
                return
            
            caminho_subpasta = os.path.join(os.getcwd(), "Inventário", subpasta_escolhida)
            
            if arquivo.endswith(".csv"):
                with open(arquivo, 'r', encoding='utf-8') as f:
                    primeiro_caractere = f.read(1024).splitlines()[0]
                    separador = ';' if ';' in primeiro_caractere else ','

                df = pd.read_csv(arquivo, sep=separador, encoding='utf-8')
            else:
                df = pd.read_excel(arquivo)

            df.columns = df.columns.str.strip()
            df.columns = df.columns.str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
            df.columns = df.columns.str.replace(' ', '_').str.lower()

            colunas_esperadas = ['loja_key', 'operador', 'endereco', 'cod._barras', 'qnt._contada']
            if not all(coluna in df.columns for coluna in colunas_esperadas):
                messagebox.showerror("Erro", "Colunas obrigatórias não encontradas no arquivo!")
                self.select_window.destroy()
                self.root.deiconify()
                return

            nome_arquivo_saida = os.path.basename(arquivo)
            caminho_saida = os.path.join(caminho_subpasta, nome_arquivo_saida)
            df.to_excel(caminho_saida, index=False)
            
            self.log_message(self.process_log, f"Arquivo processado e salvo em: {caminho_saida}")
            messagebox.showinfo("Sucesso", "Arquivo processado com sucesso!")
            
        except Exception as e:
            self.log_message(self.process_log, f"Erro ao processar arquivo: {str(e)}")
            messagebox.showerror("Erro", f"Falha ao processar arquivo:\n{str(e)}")
        finally:
            self.select_window.destroy()
            self.root.deiconify()
    
    def display_tracking_data(self, data):
        """Exibe os dados de acompanhamento na interface"""
        for item in self.tree.get_children():
            self.tree.delete(item)
        
        columns = ["GTIN", "Código Interno", "Descrição", "Preço", "Desconto", "Custo", "Seção"]
        if isinstance(data, pd.DataFrame):
            if 'flag' in data.columns:
                columns.append("Atenção")
        
        self.tree["columns"] = columns
        for col in columns:
            self.tree.heading(col, text=col)
            self.tree.column(col, width=100, anchor='center')
        
        if isinstance(data, pd.DataFrame):
            self.total_rows = len(data)
            end_idx = min(self.rows_per_page, self.total_rows)
            
            for _, row in data.iloc[:end_idx].iterrows():
                values = [
                    row.get('GTIN', ''),
                    row.get('Código Interno', ''),
                    row.get('Descrição', ''),
                    f"R$ {row.get('Preço', 0):.2f}",
                    f"R$ {row.get('Desconto', 0):.2f}",
                    f"R$ {row.get('Custo', 0):.2f}",
                    row.get('Seção', '')
                ]
                if 'flag' in row:
                    values.append("⚠️" if row['flag'] else "")
                
                self.tree.insert("", "end", values=values)
        
        self.update_pagination_info()
    
    def update_pagination_info(self):
        """Atualiza as informações de paginação"""
        total_pages = max(1, (self.total_rows // self.rows_per_page) + 1)
        current_page = min(self.current_page + 1, total_pages)
        
        start_rec = self.current_page * self.rows_per_page + 1
        end_rec = min((self.current_page + 1) * self.rows_per_page, self.total_rows)
        
        self.info_label.config(
            text=f"Inventário: {self.current_inventory['name'] if self.current_inventory else 'Nenhum'} | "
                 f"Página {current_page}/{total_pages} | "
                 f"Registros {start_rec}-{end_rec} de {self.total_rows} | "
                 f"Filtro: {self.current_filter or 'Nenhum'}"
        )
    
    def first_page(self):
        """Vai para a primeira página"""
        self.current_page = 0
        self.load_current_page()
    
    def prev_page(self):
        """Volta uma página"""
        if self.current_page > 0:
            self.current_page -= 1
            self.load_current_page()
    
    def next_page(self):
        """Avança uma página"""
        if (self.current_page + 1) * self.rows_per_page < self.total_rows:
            self.current_page += 1
            self.load_current_page()
    
    def last_page(self):
        """Vai para a última página"""
        self.current_page = (self.total_rows // self.rows_per_page)
        self.load_current_page()
    
    def load_current_page(self):
        """Carrega a página atual dos dados"""
        if not self.current_inventory or 'data_file' not in self.current_inventory:
            return
        
        try:
            if self.current_inventory['data_file'].endswith('.parquet'):
                df = pd.read_parquet(self.current_inventory['data_file'])
            else:
                df = pd.read_csv(self.current_inventory['data_file'], sep=';')
            
            if self.current_filter:
                df = df[df.apply(lambda row: any(self.current_filter.lower() in str(cell).lower() for cell in row), axis=1)]
            
            self.total_rows = len(df)
            
            for item in self.tree.get_children():
                self.tree.delete(item)
            
            start_idx = self.current_page * self.rows_per_page
            end_idx = min((self.current_page + 1) * self.rows_per_page, self.total_rows)
            
            for _, row in df.iloc[start_idx:end_idx].iterrows():
                values = [
                    row.get('GTIN', ''),
                    row.get('Código Interno', ''),
                    row.get('Descrição', ''),
                    f"R$ {row.get('Preço', 0):.2f}",
                    f"R$ {row.get('Desconto', 0):.2f}",
                    f"R$ {row.get('Custo', 0):.2f}",
                    row.get('Seção', '')
                ]
                if 'flag' in row:
                    values.append("⚠️" if row['flag'] else "")
                
                self.tree.insert("", "end", values=values)
            
            self.update_pagination_info()
            
        except Exception as e:
            messagebox.showerror("Erro", f"Falha ao carregar dados:\n{str(e)}")
    
    def apply_filter(self):
        """Aplica o filtro digitado pelo usuário"""
        filter_text = self.filter_entry.get().strip()
        self.current_filter = filter_text
        self.current_page = 0
        self.load_current_page()
    
    def setup_auto_save(self):
        """Configura o salvamento automático periódico"""
        def auto_save():
            if self.current_inventory and hasattr(self, 'unsaved_changes') and self.unsaved_changes:
                try:
                    self.inventory_manager.save_inventory(self.current_inventory)
                    self.unsaved_changes = False
                    self.logger.info("Inventário salvo automaticamente")
                except Exception as e:
                    self.logger.error(f"Erro no salvamento automático: {str(e)}")
            self.root.after(self.auto_save_interval, auto_save)
        
        self.root.after(self.auto_save_interval, auto_save)
    
    def on_closing(self):
        """Executa ao fechar a aplicação"""
        if hasattr(self, 'unsaved_changes') and self.unsaved_changes:
            if messagebox.askyesno("Salvar alterações", "Há alterações não salvas. Deseja salvar antes de sair?"):
                self.inventory_manager.save_inventory(self.current_inventory)
        self.root.destroy()

if __name__ == "__main__":
    root = tk.Tk()
    app = InventoryApp(root)
    root.mainloop()