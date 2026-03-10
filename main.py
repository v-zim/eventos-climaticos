import matplotlib.pyplot as plt
import polars as pl

import downloader
import parquet_manager

PASTA_PARQUETS = "C:/Users/Administrador/Documents/Developer/Python/Projeto Eventos Climaticos/parquets"

# Checar se existem novos dados desde a última atualização, e baixá-los se for o caso
check_download = downloader.extrair_dados_inmet()

# Caso não seja possível fazer o download dos dados, utilizar arquivos locais
if not check_download:
    downloader.extrair_dados_locais()

# Gerar DF unificado
df = parquet_manager.gerar_df_unificado()
print(df.describe())
