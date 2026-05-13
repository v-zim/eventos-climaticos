import streamlit as st

from pathlib import Path
import plotly
import plotly.express as px
import plotly.io as pio
import polars as pl

PASTA_PARQUETS = Path("parquets_remoto")

# print(Path.cwd())
# print(list(Path(".").iterdir()))

lista_parquets = list(PASTA_PARQUETS.glob("*.parquet"))
parquets = [p.name.split(".")[0] for p in lista_parquets]

# Plotar no app

st.title("Eventos climáticos")
escolhido = st.selectbox("Selecione o arquivo desejado", parquets)

caminho_arquivo = f"{PASTA_PARQUETS}/{escolhido}.parquet"
df = pl.read_parquet(caminho_arquivo)

# pio.renderers.default = "browser"
fig = px.bar(data_frame=df, x='PERIODO', y='PRECIPITACAO TOTAL, HORARIO (mm)', color='ESTACAO DO ANO')
st.plotly_chart(fig)
