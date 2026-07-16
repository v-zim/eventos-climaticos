# Arquivo para gerenciar os parquets remotos

import matplotlib.pyplot as plt
import polars as pl
import polars.selectors as cs
import seaborn as sbn

import data_manager
import downloader
import parquet_manager

# SP
# Estacoes relevantes
ESTACOES_SP = [
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

# # Checar se existem novos dados desde a última atualização, e baixá-los se for o caso
# check_download = downloader.extrair_dados_inmet()

# # Caso não seja possível fazer o download dos dados, utilizar arquivos locais
# if not check_download:
#     downloader.extrair_dados_locais()

# 1. Manipulação de DFs

# Gerar DF unificado
df = parquet_manager.gerar_df_unificado()

# Filtrar região desejada
df_uf_sp = df.filter(
    pl.col('UF').str.contains('SP'),
    pl.col('ESTACAO').is_in(ESTACOES_SP)
)

# Ajustar DF para usar com o seaborn/plotly
df_ajustado = parquet_manager.ajustar_df(df_uf_sp)

df_por_estacao = df_ajustado \
    .group_by('PERIODO', 'INDICE ESTACAO', 'ESTACAO DO ANO', 'ESTACAO') \
    .agg(pl.col('PRECIPITACAO TOTAL, HORARIO (mm)').sum()) \
    .sort('PERIODO', 'INDICE ESTACAO')

# df_sp_p95 = data_manager.classificar_evento_extremo(df_ajustado, 'PRECIPITACAO TOTAL, HORARIO (mm)', 95)
df_sp_p90 = data_manager.gerar_df_modelo(df_ajustado, 90)
df_sp_p95 = data_manager.gerar_df_modelo(df_ajustado, 95)

print(df_sp_p90.head(10))

# # Disponibilizar DFs remotamente
# parquet_manager.salvar_df_remoto(df_ajustado, "uf_sp")
# parquet_manager.salvar_df_remoto(df_por_estacao, "uf_sp_precipitacao")
parquet_manager.salvar_df_remoto(df_sp_p95, "uf_sp_p95")
parquet_manager.salvar_df_remoto(df_sp_p90, "uf_sp_p90")
