# core/file_processor.py
import pandas as pd
import re
from pathlib import Path
from typing import Tuple, Optional, Any
import logging
import chardet
import threading
import shutil
from datetime import datetime # Importar datetime

from .data_combiner import DataCombiner


class FileProcessor:
    def __init__(self, inventory_manager):
        self.inventory_manager = inventory_manager
        self.logger = logging.getLogger(__name__)
        
        # Mapeamento de colunas do Excel (Este mapeamento é para a "limpeza" de dados, não para o process_excel inicial)
        # Manter este mapeamento se for usado em outras partes do seu FileProcessor
        self.excel_column_mapping = {
            'GTIN': ['gtin', 'ean', 'código de barras', 'barcode'],
            'Codigo': ['codigo', 'código', 'id', 'produto', 'sku'],
            'Descricao': ['descricao', 'descrição', 'nome', 'produto'],
            'Preco': ['preco', 'preço', 'valor', 'preço de venda'],
            'Estoque': ['estoque', 'quantidade', 'qtd', 'saldo'],
            'Custo': ['custo', 'custo unitário', 'preço de custo'],
            'Secao': ['secao', 'seção', 'departamento', 'categoria'],
            'Endereco': ['endereco', 'endereço', 'local', 'posição'],
            'Operador': ['operador', 'funcionário', 'responsável']
        }

    @staticmethod
    def detect_encoding(file_path: str) -> Optional[str]:
        """Detecta a codificação de um arquivo com fallback inteligente."""
        try:
            with open(file_path, 'rb') as f:
                raw_data = f.read(10000)
                result = chardet.detect(raw_data)
                if result['confidence'] < 0.7 or not result['encoding']:
                    return 'utf-8'
                return result['encoding']
        except Exception as e:
            logging.warning(f"Falha ao detectar encoding: {e}")
            return 'utf-8'

    def _remove_leading_zeros(self, value: str) -> str:
        """Remove zeros à esquerda de uma string numérica."""
        # Garante que a entrada é uma string antes de chamar lstrip
        s_value = str(value)
        return s_value.lstrip('0') or '0'

    def _add_flag_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Faz o join do DataFrame processado com o arquivo `prod_flag.parquet`.
        Adiciona a coluna `Flag` com base no arquivo `prod_flag.parquet`.
        """
        try:
            # Caminho para o arquivo prod_flag.parquet
            # Supondo que prod_flag.parquet está em 'core/' junto com este arquivo
            flag_file = Path(__file__).parent / 'prod_flag.parquet'

            if not flag_file.exists():
                self.logger.warning("Arquivo 'prod_flag.parquet' não encontrado. Flags não serão adicionadas.")
                df['Flag'] = ''  # Adiciona coluna vazia se arquivo não existir
                return df

            # Carrega o arquivo prod_flag.parquet
            flag_df = pd.read_parquet(flag_file)

            # Verifica colunas obrigatórias
            required_columns = {'produto_key', 'flag'}
            if not required_columns.issubset(flag_df.columns):
                self.logger.error(f"Colunas obrigatórias {required_columns} ausentes no arquivo 'prod_flag.parquet'. Flags não serão adicionadas.")
                df['Flag'] = ''
                return df

            # Pré-processamento - Normalização das chaves
            def normalize_code(code: Any) -> str: # Tipo alterado para Any para lidar com nulos/números
                """Remove todos os não-dígitos e zeros à esquerda"""
                if pd.isna(code):
                    return ''
                return ''.join(filter(str.isdigit, str(code))).lstrip('0') or '0'

            # Aplica normalização
            # Certifica-se que 'Codigo' existe antes de normalizar
            if 'Codigo' in df.columns:
                df['Codigo_normalized'] = df['Codigo'].apply(normalize_code)
            else:
                self.logger.warning("Coluna 'Codigo' não encontrada no DataFrame principal para aplicar flags.")
                df['Flag'] = ''
                return df
            
            flag_df['produto_key_normalized'] = flag_df['produto_key'].apply(normalize_code)

            # Faz o join apenas pelo código normalizado
            merged_df = pd.merge(
                df,
                flag_df[['produto_key_normalized', 'flag']],
                how='left',
                left_on=['Codigo_normalized'],
                right_on=['produto_key_normalized']
            )

            # Adiciona a coluna Flag e remove colunas temporárias
            df['Flag'] = merged_df['flag'].fillna('')
            df.drop(columns=['Codigo_normalized'], inplace=True, errors='ignore') # erro ignore para compatibilidade

            # Estatísticas
            flagged_count = (df['Flag'] != '').sum()
            self.logger.info(f"Flags aplicadas em {flagged_count} de {len(df)} produtos ({(flagged_count/len(df))*100:.2f}%)")
            
            # Debug: mostra exemplos de matches
            if flagged_count > 0:
                sample_matches = df[df['Flag'] != ''].head(3)[['Codigo', 'Flag']]
                self.logger.debug(f"Exemplos de matches:\n{sample_matches.to_string()}")
            
            return df

        except Exception as e:
            self.logger.error(f"Erro ao adicionar flags: {e}", exc_info=True)
            df['Flag'] = ''
            return df

    def process_initial_txt(self, file_path: str) -> Tuple[bool, str]:
        """Processa o arquivo TXT inicial e salva como parquet"""
        try:
            # [CITE: 3] Este é o _get_active_data_path que havíamos discutido
            data_path = Path(self.inventory_manager.get_active_inventory_data_path())
            if not data_path:
                return False, "Nenhum inventário ativo selecionado."

            # Garante que a pasta existe
            data_path.mkdir(parents=True, exist_ok=True)

            # Detecta a codificação do arquivo TXT
            encodings_to_try = [self.detect_encoding(file_path), 'utf-8', 'latin-1']
            lines = []

            for encoding in filter(None, set(encodings_to_try)):
                try:
                    with open(file_path, 'r', encoding=encoding) as f:
                        lines = f.readlines()
                    break
                except UnicodeDecodeError:
                    continue

            if not lines:
                return False, "Não foi possível ler o arquivo."

            # Expressão regular para parsing
            pattern = re.compile(
                r'^(?P<gtin>\d{13})\s+'
                r'(?P<codigo>\d{9})\s+'
                r'(?P<descricao>.+?)\s+'
                r'(?P<preco>\d{8})\s+'
                r'(?P<estoque>\d{8})\s+'
                r'(?P<custo>\d{8})\s+'
                r'(?P<secao>\d{5})$'
            )

            data = []
            for line_num, line in enumerate(lines, 1):
                line = line.strip()
                if not line:
                    continue

                match = pattern.match(line)
                if not match:
                    self.logger.warning(f"Linha {line_num} ignorada: formato inválido.")
                    continue

                try:
                    # Capture os valores como string
                    preco_str = match.group('preco')
                    estoque_str = match.group('estoque')
                    custo_str = match.group('custo')
                    
                    # Remova qualquer vírgula ou ponto (que não seja decimal) antes de converter para int
                    # Embora o regex \d{8} não permita vírgulas/pontos, o .replace(r'[.,]', '') serve como segurança.
                    # O principal aqui é a divisão por 100.
                    preco_limpo = re.sub(r'[.,]', '', preco_str)
                    estoque_limpo = re.sub(r'[.,]', '', estoque_str)
                    custo_limpo = re.sub(r'[.,]', '', custo_str)
                    
                    data.append({
                        'GTIN': self._remove_leading_zeros(match.group('gtin')),
                        'Codigo': self._remove_leading_zeros(match.group('codigo')),
                        'Descricao': match.group('descricao').strip(),
                        # CORREÇÃO APLICADA: Divide por 100 para Preco
                        'Preco': int(preco_limpo) / 100,
                        # CORREÇÃO APLICADA: Divide por 100 para Estoque
                        'Estoque': int(estoque_limpo) / 100,
                        # CORREÇÃO APLICADA: Divide por 100 para Custo
                        'Custo': int(custo_limpo) / 100,
                        'Secao': self._remove_leading_zeros(match.group('secao')),
                    })
                except Exception as e:
                    self.logger.error(f"Erro ao processar linha {line_num}: {e}")

            if not data:
                return False, "Nenhum dado válido encontrado."

            df = pd.DataFrame(data)
            df = self._add_flag_data(df)

            columns_order = ['GTIN', 'Codigo', 'Descricao', 'Preco', 'Estoque',
                             'Custo', 'Secao', 'Flag']
            df = df[columns_order]

            # Salva os dados processados
            output_path = data_path / "initial_data.parquet"
            df.to_parquet(output_path, index=False)
            
            # [CITE: 3] REMOVA ESTE BLOCO - A combinação será disparada pelo DataCombiner
            # combiner = DataCombiner(data_path)
            # combiner.combine_data()
            
            return True, f"Arquivo processado com sucesso. {len(data)} itens importados."
        except Exception as e:
            self.logger.error(f"Erro ao processar TXT: {e}", exc_info=True)
            return False, f"Erro crítico: {str(e)}"
    
    # [CITE: 3] REMOVA ESTE MÉTODO - A combinação será disparada pelo DataCombiner
    # def _trigger_data_combination(self, data_path: str) -> bool:
    #     """Dispara a combinação de dados"""
    #     try:
    #         return DataCombiner(data_path).combine_data()
    #     except Exception as e:
    #         self.logger.error(f"Falha na combinação automática: {e}", exc_info=True)
    #         return False
            
    def _map_excel_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Mapeia as colunas EXATAS do seu Excel para o formato interno"""
        # [CITE: 3] Esta função será modificada e movida/integrada diretamente ao process_excel
        # para a nova lógica. O mapeamento será feito direto na função process_excel.
        # Por enquanto, vou deixá-la aqui, mas o process_excel não vai usá-la como está.
        mapped_df = pd.DataFrame()
        
        # Mapeamento ESPECÍFICO para seus arquivos
        column_mapping = {
            'GTIN': ['CÓD. BARRAS', 'COD. BARRAS', 'CODIGO BARRAS'],
            'Operador': ['OPERADOR'],
            'Endereco': ['ENDEREÇO', 'ENDERECO'],
            'Estoque': ['QNT. CONTADA', 'QUANTIDADE CONTADA', 'QNT CONTADA']
        }
        
        # Verifica cada coluna necessária
        for standard_col, possible_names in column_mapping.items():
            found = False
            for possible_name in possible_names:
                if possible_name in df.columns:
                    mapped_df[standard_col] = df[possible_name]
                    self.logger.debug(f"Mapeada coluna '{possible_name}' -> '{standard_col}'")
                    found = True
                    break
            
            if not found and standard_col in ['Endereco', 'Operador']:
                mapped_df[standard_col] = None  # Colunas opcionais
                self.logger.debug(f"Coluna opcional '{standard_col}' não encontrada")
            elif not found:
                self.logger.warning(f"Coluna obrigatória '{standard_col}' não encontrada. Procurado por: {possible_names}")
                return None
        
        return mapped_df

    def _clean_excel_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Processa os dados do SEU formato específico"""
        # [CITE: 3] Esta função parece ser para o formato de "contagem" do Excel.
        # A lógica dela será integrada e adaptada diretamente no process_excel.
        try:
            # Converte GTIN (que veio de CÓD. BARRAS)
            if 'GTIN' in df.columns:
                df['GTIN'] = (
                    df['GTIN']
                    .astype(str)
                    .str.replace(r'\D', '', regex=True)  # Remove não-dígitos
                    .apply(self._remove_leading_zeros)
                )
            
            # Converte estoque para numérico
            if 'Estoque' in df.columns:
                df['Estoque'] = pd.to_numeric(df['Estoque'], errors='coerce').fillna(0)
            
            # Processa texto das colunas opcionais
            for col in ['Operador', 'Endereco']:
                if col in df.columns:
                    df[col] = df[col].astype(str).str.strip()
            
            # Agrupa por GTIN (CÓD. BARRAS)
            if 'GTIN' in df.columns:
                agg_rules = {'Estoque': 'sum'}
                for col in ['Operador', 'Endereco']:
                    if col in df.columns:
                        agg_rules[col] = lambda x: ' / '.join(sorted(set(x.astype(str))))
                
                return df.groupby('GTIN', as_index=False).agg(agg_rules)
            
            return df
        except Exception as e:
            self.logger.error(f"Erro na limpeza dos dados: {str(e)}", exc_info=True)
            raise

    def process_excel(self, file_path: str) -> Tuple[bool, str]:
        """
        [CITE: 3] FUNÇÃO PRINCIPAL PARA EXCEL - Esta será a que passará pelas maiores mudanças.
        Processa arquivo Excel de contagem, salva o XLSX original
        e converte os dados para Parquet, armazenando-os como 'manual_counts.parquet'.
        """
        try:
            # [CITE: 3] Obtenha o caminho da pasta de dados do inventário ativo
            data_path = Path(self.inventory_manager.get_active_inventory_data_path())
            if not data_path:
                return False, "Nenhum inventário ativo selecionado"

            # [CITE: 3] Garante que a pasta de dados principal e a de 'manual_imports' existem
            data_path.mkdir(exist_ok=True)
            manual_imports_dir = data_path / "manual_imports"
            manual_imports_dir.mkdir(parents=True, exist_ok=True)

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            original_file_name = Path(file_path).name
            
            # [CITE: 3] Salva o arquivo XLSX original na pasta de imports manuais
            saved_xlsx_path = manual_imports_dir / f"import_{timestamp}_{original_file_name}"
            shutil.copy2(file_path, saved_xlsx_path) # Copia o arquivo, mantendo metadados
            self.logger.info(f"Arquivo XLSX original salvo em: {saved_xlsx_path}")

            self.logger.info(f"Processando Excel: {file_path}")
            df = pd.read_excel(file_path, sheet_name=0)
            df.dropna(how='all', inplace=True) # Remove linhas completamente vazias

            if len(df) < 1:
                return False, "Planilha vazia ou sem dados válidos"

            # [CITE: 3] NOVO: Mapeamento de colunas do Excel para nomes padronizados (mais robusto)
            # Este mapeamento é para pegar os nomes do seu Excel e transformá-los nos nomes que o sistema espera
            # ATENÇÃO: Verifique ESTES nomes (à esquerda) com os cabeçalhos REAIS do seu Excel
            column_rename_map = {
                'cód. barras': 'COD_BARRAS',
                'cod. barras': 'COD_BARRAS',
                'codigo barras': 'COD_BARRAS',
                'codigo_barras': 'COD_BARRAS', # Adicionado por segurança
                'código de barras': 'COD_BARRAS', # Adicionado por segurança
                
                'qnt. contada': 'QNT_CONTADA',
                'quantidade contada': 'QNT_CONTADA',
                'qnt contada': 'QNT_CONTADA', # Adicionado por segurança
                'quantidade_contada': 'QNT_CONTADA', # Adicionado por segurança
                
                'operador': 'OPERADOR',
                'endereço': 'ENDERECO', # Pode ser 'endereço' ou 'endereco'
                'endereco': 'ENDERECO',
                'loja key': 'LOJA_KEY', # Adicionado: Se LOJA KEY vem no Excel
                'loja_key': 'LOJA_KEY', # Adicionado: Se LOJA KEY vem no Excel
            }
            
            # Normaliza os nomes das colunas existentes no DataFrame
            df.columns = [column_rename_map.get(col.lower().strip(), col) for col in df.columns]
            
            # [CITE: 3] Verifica colunas obrigatórias após o renomeamento
            required_columns = {'COD_BARRAS', 'QNT_CONTADA'}
            missing = required_columns - set(df.columns)
            if missing:
                return False, f"Colunas obrigatórias faltando após renomeamento: {', '.join(missing)}. " \
                              f"Verifique o mapeamento e os cabeçalhos do Excel."

            # [CITE: 3] Processa dados: COD_BARRAS
            df['COD_BARRAS'] = (
                df['COD_BARRAS']
                .astype(str)
                .str.replace(r'\.0$', '', regex=True) # Remove '.0' de números interpretados como float
                .str.replace(r'\D', '', regex=True) # Remove não-dígitos
                .apply(self._remove_leading_zeros)
            )

            # [CITE: 3] Processa dados: QNT_CONTADA
            df['QNT_CONTADA'] = pd.to_numeric(df['QNT_CONTADA'], errors='coerce').fillna(0)

            # [CITE: 3] Processa dados: Colunas de texto (OPERADOR, ENDERECO)
            text_cols = ['OPERADOR', 'ENDERECO']
            for col in text_cols:
                if col in df.columns:
                    df[col] = df[col].astype(str).str.strip()
                else: # Adiciona a coluna se não existir, com valor padrão vazio
                    df[col] = '' 
            
            # [CITE: 3] Adiciona LOJA_KEY se ela não veio no Excel (pode ser obtida do inventário ativo)
            if 'LOJA_KEY' not in df.columns:
                inv_info = self.inventory_manager.get_active_inventory_info()
                if inv_info and 'loja' in inv_info:
                    df['LOJA_KEY'] = int(inv_info['loja']) # Assume que 'loja' do inv_info é a loja_key numérica
                else:
                    self.logger.warning("LOJA_KEY não encontrada no Excel e não disponível no inventário ativo. Definindo como 0.")
                    df['LOJA_KEY'] = 0 # Valor padrão se não for encontrado

            # [CITE: 3] Regras de agregação (para somar quantidades e concatenar operadores/endereços)
            agg_rules = {'QNT_CONTADA': 'sum'}
            # Adiciona Operador e Endereco às regras de agregação se existirem no DF
            for col in text_cols:
                if col in df.columns:
                    # Agrega strings únicas, converte para string e join com ' / '
                    agg_rules[col] = lambda x: ' / '.join(sorted(set(str(val) for val in x if pd.notna(val) and str(val).strip())))
            
            # [CITE: 3] Agrupa por COD_BARRAS (e LOJA_KEY se você quiser considerar itens da mesma loja)
            # Recomendo agrupar por ['COD_BARRAS', 'LOJA_KEY'] se diferentes lojas podem ter o mesmo COD_BARRAS
            grouped_df = df.groupby(['COD_BARRAS', 'LOJA_KEY'], as_index=False).agg(agg_rules)

            # [CITE: 3] NOVO: O arquivo de saída para contagens manuais será 'manual_counts.parquet'
            output_parquet_file = data_path / "manual_counts.parquet"
            
            # [CITE: 3] Carrega dados existentes (manual_counts.parquet) se houver
            if output_parquet_file.exists():
                existing_df = pd.read_parquet(output_parquet_file)
                
                # [CITE: 3] Concatena os dados existentes com os novos
                final_df = pd.concat([existing_df, grouped_df], ignore_index=True)
                
                # [CITE: 3] Reaplica a agregação para garantir que novas contagens do mesmo item/loja somem
                # e operadores/endereços sejam atualizados
                final_df = final_df.groupby(['COD_BARRAS', 'LOJA_KEY'], as_index=False).agg(agg_rules)
                
            else:
                final_df = grouped_df

            # [CITE: 3] Salva o arquivo atualizado (manual_counts.parquet)
            final_df.to_parquet(output_parquet_file, index=False)
            
            # [CITE: 3] REMOVA ESTE BLOCO - A combinação será disparada pelo DataCombiner
            # combiner = DataCombiner(data_path)
            # combiner.combine_data()

            return True, (f"Dados processados com sucesso. "
                          f"Total de {len(final_df)} itens únicos contados. "
                          f"Quantidade total: {final_df['QNT_CONTADA'].sum():.0f}")
        except Exception as e:
            self.logger.error(f"Erro ao processar Excel: {e}", exc_info=True)
            return False, f"Erro ao processar Excel: {str(e)}"