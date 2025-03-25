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

class InventarioApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Sistema de Gerenciamento de Inventário - Versão 2.0")
        self.root.geometry("1000x750")
        
        # Configuração de estilo
        self.setup_styles()
        
        # Variáveis para armazenar dados
        self.nome_inventario = tk.StringVar()
        self.nome_loja = tk.StringVar()
        self.nome_pessoa = tk.StringVar()
        self.txt_file_path = tk.StringVar()
        self.selected_inventory = tk.StringVar()
        
        # Configurar o notebook (abas)
        self.notebook = ttk.Notebook(root)
        self.notebook.pack(fill='both', expand=True, padx=10, pady=10)
        
        # Abas
        self.create_tab1()  # Criar Inventário
        self.create_tab2()  # Processar TXT
        self.create_tab3()  # Consolidar Dados
        self.create_tab4()  # Visualizar Dados
        
        # Barra de status
        self.status_var = tk.StringVar()
        self.status_var.set("Pronto")
        self.status_bar = ttk.Label(root, textvariable=self.status_var, relief='sunken')
        self.status_bar.pack(side='bottom', fill='x')
        
        # Configurar logging
        self.setup_logging()
        
        # Atualizar lista de inventários disponíveis
        self.update_inventory_list()
    
    def setup_styles(self):
        """Configura estilos para a interface"""
        style = ttk.Style()
        style.configure('TFrame', background='#f0f0f0')
        style.configure('TLabel', background='#f0f0f0', font=('Arial', 10))
        style.configure('TButton', font=('Arial', 10))
        style.configure('TNotebook.Tab', font=('Arial', 10, 'bold'))
        style.configure('Header.TLabel', font=('Arial', 12, 'bold'))
        
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
        ttk.Button(btn_frame, text="Criar Inventário", command=self.criar_inventario, 
                  style='Accent.TButton').pack(pady=10, ipadx=20, ipady=5)
        
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
        ttk.Button(btn_frame, text="Processar TXT", command=self.processar_txt, 
                  style='Accent.TButton').pack(pady=10, ipadx=20, ipady=5)
        
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
        ttk.Button(btn_frame, text="Iniciar Consolidação", command=self.iniciar_consolidacao, 
                  style='Accent.TButton').pack(pady=10, ipadx=20, ipady=5)
        
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
        ttk.Button(control_frame, text="Carregar Dados", command=self.carregar_dados, 
                  style='Accent.TButton').pack(side='right', padx=10)
        
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
        """Cria um novo inventário"""
        nome = self.nome_inventario.get().strip()
        loja = self.nome_loja.get().strip()
        pessoa = self.nome_pessoa.get().strip()
        
        if not nome or not loja or not pessoa:
            messagebox.showerror("Erro", "Todos os campos são obrigatórios!")
            return
        
        # Executa em uma thread separada para não travar a interface
        def run():
            self.status_var.set(f"Criando inventário: {nome}...")
            self.log_message(self.log_area, f"Iniciando criação do inventário: {nome}")
            
            # Redireciona a saída padrão para capturar logs
            old_stdout = sys.stdout
            sys.stdout = mystdout = StringIO()
            
            try:
                # Cria a estrutura de pastas
                self.criar_estrutura_pastas(nome, loja, pessoa)
                
                # Atualiza a interface
                self.root.after(0, lambda: messagebox.showinfo(
                    "Sucesso", 
                    f"Inventário '{nome}' criado com sucesso!\n"
                    f"Loja: {loja}\n"
                    f"Responsável: {pessoa}"
                ))
                
                self.update_inventory_list()
                
            except Exception as e:
                self.root.after(0, lambda: messagebox.showerror(
                    "Erro", 
                    f"Falha ao criar inventário: {str(e)}"
                ))
                self.logger.error(f"Erro ao criar inventário: {str(e)}")
                
            finally:
                sys.stdout = old_stdout
                output = mystdout.getvalue()
                self.log_message(self.log_area, output)
                self.status_var.set("Pronto")
        
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
        with open(os.path.join(caminho_inventario, "informacoes.txt"), 'w') as f:
            f.write(f"Nome do Inventário: {nome_inventario}\n")
            f.write(f"Nome da Loja: {nome_loja}\n")
            f.write(f"Responsável: {nome_pessoa}\n")
        
        # Cria pasta na área de trabalho
        desktop = os.path.join(os.path.expanduser("~"), "Desktop")
        pasta_contagem = os.path.join(desktop, f"{nome_inventario} - Inserir Dados de Contagem")
        os.makedirs(pasta_contagem)
        
        self.logger.info(f"Estrutura criada com sucesso em: {caminho_inventario}")
        self.logger.info(f"Pasta de contagem criada na área de trabalho: {pasta_contagem}")
    
    def processar_txt(self):
        """Processa o arquivo TXT selecionado"""
        txt_file = self.txt_file_path.get()
        inventory = self.selected_inventory.get()
        
        if not txt_file:
            messagebox.showerror("Erro", "Selecione um arquivo TXT!")
            return
        
        if not inventory:
            messagebox.showerror("Erro", "Selecione um inventário!")
            return
        
        # Executa em uma thread separada
        def run():
            self.status_var.set(f"Processando arquivo: {os.path.basename(txt_file)}...")
            self.log_message(self.process_log, f"Iniciando processamento do arquivo: {txt_file}")
            
            try:
                # Configura caminhos
                input_path = txt_file
                output_folder = os.path.join("Inventário", inventory, "dados_processados")
                os.makedirs(output_folder, exist_ok=True)
                output_path = os.path.join(output_folder, "output.csv")
                
                # Processa o arquivo
                df = self.processar_arquivo_txt(input_path, output_path)
                
                # Atualiza a interface
                self.root.after(0, lambda: messagebox.showinfo(
                    "Sucesso",
                    f"Arquivo processado com sucesso!\n"
                    f"Registros processados: {len(df)}\n"
                    f"Salvo em: {output_path}"
                ))
                
            except Exception as e:
                self.root.after(0, lambda: messagebox.showerror(
                    "Erro",
                    f"Falha ao processar arquivo TXT:\n{str(e)}"
                ))
                self.logger.error(f"Erro no processamento: {str(e)}")
                
            finally:
                self.status_var.set("Pronto")
        
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
        
        # Executa em uma thread separada
        def run():
            self.status_var.set(f"Consolidando dados: {inventory}...")
            self.log_message(self.consolidate_log, f"Iniciando consolidação para: {inventory}")
            
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
                    time.sleep(0.5)  # Simula processamento
                    self.progress['value'] = i * 10
                    self.root.update_idletasks()
                
                # Processa os arquivos
                df_output = pd.read_csv(output_file, sep=";")
                df_consolidados = pd.read_csv(consolidados_file, sep=";")
                
                # Limpa nomes de colunas
                df_output.columns = df_output.columns.str.strip()
                df_consolidados.columns = df_consolidados.columns.str.strip()
                
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
                self.root.after(0, lambda: messagebox.showinfo(
                    "Sucesso",
                    f"Dados consolidados com sucesso!\n"
                    f"Arquivo gerado: {dados_tela_file}"
                ))
                
            except Exception as e:
                self.root.after(0, lambda: messagebox.showerror(
                    "Erro",
                    f"Falha na consolidação:\n{str(e)}"
                ))
                self.logger.error(f"Erro na consolidação: {str(e)}")
                
            finally:
                self.progress['value'] = 0
                self.status_var.set("Pronto")
        
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
            
        except Exception as e:
            messagebox.showerror("Erro", f"Falha ao carregar dados:\n{str(e)}")
            self.logger.error(f"Erro ao carregar dados: {str(e)}")

if __name__ == "__main__":
    root = tk.Tk()
    
    # Configura um estilo moderno
    style = ttk.Style()
    style.theme_use('clam')
    style.configure('Accent.TButton', foreground='white', background='#4a6ea9', font=('Arial', 10, 'bold'))
    
    app = InventarioApp(root)
    root.mainloop()