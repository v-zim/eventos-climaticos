# Arquivo para gerenciar os parquets remotos

import matplotlib.pyplot as plt
import polars as pl
import polars.selectors as cs
import seaborn as sbn

import data_manager
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
# df_porto_alegre = df.filter(pl.col('ESTACAO').str.contains('PORTO ALEGRE'))

# SP
# Estacoes relevantes
estacoes_sp = [
    "BARRA DO TURVO",
    "BERTIOGA",
    "CACHOEIRA PAULISTA",
    "CAMPOS DO JORDAO",
    "IGUAPE",
    "ITAPEVA",
    "REGISTRO",
    "SAO LUIZ DO PARAITINGA",
    "SAO PAULO - INTERLAGOS",
    "SAO PAULO - MIRANTE",
    "SAO SEBASTIAO",
    "TAUBATE",
]

df_uf_sp = df.filter(
    pl.col('UF').str.contains('SP'),
    pl.col('ESTACAO').is_in(estacoes_sp)
)

# Ajustar DF para usar com o seaborn/plotly
df_ajustado = parquet_manager.ajustar_df(df_uf_sp)

df_por_estacao = df_ajustado \
    .group_by('PERIODO', 'INDICE ESTACAO', 'ESTACAO DO ANO', 'ESTACAO') \
    .agg(pl.col('PRECIPITACAO TOTAL, HORARIO (mm)').sum()) \
    .sort('PERIODO', 'INDICE ESTACAO')

df_sp_p95 = data_manager.classificar_evento_extremo(df_ajustado, 'PRECIPITACAO TOTAL, HORARIO (mm)', 0.95)
df_sp_p90 = data_manager.classificar_evento_extremo(df_ajustado, 'PRECIPITACAO TOTAL, HORARIO (mm)', 0.90)

# Disponibilizar DFs remotamente
parquet_manager.salvar_df_remoto(df_ajustado, "uf_sp")
parquet_manager.salvar_df_remoto(df_por_estacao, "uf_sp_precipitacao")
parquet_manager.salvar_df_remoto(df_sp_p95, "uf_sp_p95")
parquet_manager.salvar_df_remoto(df_sp_p90, "uf_sp_p90")
