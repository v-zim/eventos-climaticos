import matplotlib.pyplot as plt
import polars as pl
import polars.selectors as cs
import seaborn as sbn

import plotly.express as px
import plotly.io as pio

import downloader
import parquet_manager
import uploader

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
df_filtrado = df.filter(pl.col('ESTACAO').str.contains('PORTO ALEGRE'))

# Ajustar DF para usar com o seaborn/plotly
df_sbn = parquet_manager.ajustar_df(df_filtrado)

df_por_estacao = df_sbn \
    .group_by('PERIODO', 'INDICE ESTACAO', 'ESTACAO DO ANO', 'ESTACAO') \
    .agg(pl.col('PRECIPITACAO TOTAL, HORARIO (mm)').sum()) \
    .sort('PERIODO', 'INDICE ESTACAO')

# Disponibilizar DFs remotamente
parquet_manager.salvar_df_remoto(df_filtrado, "porto_alegre_total")
parquet_manager.salvar_df_remoto(df_por_estacao, "porto_alegre_por_estacao")

# 2. Geração de gráficos

plt.style.use('ggplot')
plt.rcParams['figure.figsize'] = (15, 5)

pio.renderers.default = "browser"
fig = px.bar(data_frame=df_por_estacao, x='PERIODO', y='PRECIPITACAO TOTAL, HORARIO (mm)', color='ESTACAO DO ANO')
# fig.show()

uploader.subir_grafico(fig)

# sbn.lineplot(df_por_estacao, x='PERIODO', y='PRECIPITACAO TOTAL, HORARIO (mm)', hue='ESTACAO DO ANO')
# plt.show()

# sbn.barplot(df_por_estacao, x='PERIODO', y='PRECIPITACAO TOTAL, HORARIO (mm)', hue='ESTACAO DO ANO')
# plt.show()

# sbn.barplot(df_por_estacao, x='ESTACAO DO ANO', y='PRECIPITACAO TOTAL, HORARIO (mm)', hue='PERIODO')
# plt.show()
