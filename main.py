# Arquivo para gerenciar os parquets remotos

import matplotlib.pyplot as plt
import polars as pl
import polars.selectors as cs
import seaborn as sbn

import downloader
import parquet_manager

# Checar se existem novos dados desde a última atualização, e baixá-los se for o caso
check_download = downloader.extrair_dados_inmet()

# Caso não seja possível fazer o download dos dados, utilizar arquivos locais
if not check_download:
    downloader.extrair_dados_locais()

# 1. Manipulação de DFs

# Gerar DF unificado
df = parquet_manager.gerar_df_unificado()

# As duas formas abaixo geram o mesmo resultado
# df_filtrado = df.filter(pl.col('ESTACAO').is_in(['PORTO ALEGRE - JARDIM BOTANICO', 'PORTO ALEGRE- BELEM NOVO']))
df_porto_alegre = df.filter(pl.col('ESTACAO').str.contains('PORTO ALEGRE'))

# Ajustar DF para usar com o seaborn/plotly
df_porto_alegre_ajustado = parquet_manager.ajustar_df(df_porto_alegre)

df_por_estacao = df_porto_alegre_ajustado \
    .group_by('PERIODO', 'INDICE ESTACAO', 'ESTACAO DO ANO', 'ESTACAO') \
    .agg(pl.col('PRECIPITACAO TOTAL, HORARIO (mm)').sum()) \
    .sort('PERIODO', 'INDICE ESTACAO')

# Disponibilizar DFs remotamente
parquet_manager.salvar_df_remoto(df_porto_alegre, "porto_alegre_total")
parquet_manager.salvar_df_remoto(df_porto_alegre_ajustado, "porto_alegre_ajustado")
parquet_manager.salvar_df_remoto(df_por_estacao, "porto_alegre_por_estacao")
