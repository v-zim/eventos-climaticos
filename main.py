import matplotlib.pyplot as plt
import polars as pl
import polars.selectors as cs
import seaborn as sbn

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
parquet_manager.salvar_df_unificado(df)

plt.style.use('ggplot')
plt.rcParams['figure.figsize'] = (15, 5)

# As duas formas abaixo geram o mesmo resultado
# df_filtrado = df.filter(pl.col('ESTACAO').is_in(['PORTO ALEGRE - JARDIM BOTANICO', 'PORTO ALEGRE- BELEM NOVO']))
# print(df_filtrado.describe())
df_filtrado = df.filter(pl.col('ESTACAO').str.contains('PORTO ALEGRE'))
print(df_filtrado.head(20))
print(df_filtrado.describe())

# Ajustar DF para usar com o seaborn
df_sbn = parquet_manager.gerar_df_seaborn(df_filtrado)

# df_por_dia = df_sbn \
#     .group_by('DATA (YYYY-MM-DD)', 'ESTACAO') \
#     .agg(pl.col('PRECIPITACAO TOTAL, HORARIO (mm)').sum()) \
#     .sort('DATA (YYYY-MM-DD)')
    
# print(df_por_dia.describe())

# sbn.lineplot(df_por_dia, x='DATA (YYYY-MM-DD)', y='PRECIPITACAO TOTAL, HORARIO (mm)', hue='ESTACAO')
# plt.show()

df_por_estacao = df_sbn \
    .group_by('PERIODO', 'INDICE ESTACAO', 'ESTACAO DO ANO', 'ESTACAO') \
    .agg(pl.col('PRECIPITACAO TOTAL, HORARIO (mm)').sum()) \
    .sort('PERIODO', 'INDICE ESTACAO')
    
print(df_por_estacao.describe())
print(df_por_estacao.head(10))

sbn.lineplot(df_por_estacao, x='PERIODO', y='PRECIPITACAO TOTAL, HORARIO (mm)', hue='ESTACAO DO ANO')
plt.show()

sbn.barplot(df_por_estacao, x='PERIODO', y='PRECIPITACAO TOTAL, HORARIO (mm)', hue='ESTACAO DO ANO')
plt.show()

sbn.barplot(df_por_estacao, x='ESTACAO DO ANO', y='PRECIPITACAO TOTAL, HORARIO (mm)', hue='PERIODO')
plt.show()
