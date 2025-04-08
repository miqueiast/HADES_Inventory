import pandas as pd

# Lendo os arquivos Parquet
initial_data = pd.read_parquet('initial_data.parquet')
prod_flag = pd.read_parquet('prod_flag.parquet')

# Exibindo as 5 primeiras linhas de cada DataFrame (opcional)
print("Initial Data:")
print(initial_data.head())

print("\nProd Flag:")
print(prod_flag.head())
