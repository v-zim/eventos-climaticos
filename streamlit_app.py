import streamlit as st

from pathlib import Path
import plotly
import plotly.express as px
import plotly.io as pio
import polars as pl

PASTA_PARQUETS = Path("parquets_remoto")

# Para debug:
# print(Path.cwd())
# print(list(Path(".").iterdir()))

lista_parquets = list(PASTA_PARQUETS.glob("*.parquet"))
parquets = [p.name.split(".")[0] for p in lista_parquets]

# Plotar no app

st.title("Eventos climáticos")
escolhido = st.selectbox("Selecione o arquivo desejado", parquets)

caminho_arquivo = f"{PASTA_PARQUETS}/{escolhido}.parquet"
df = pl.read_parquet(caminho_arquivo)

fig = px.bar(data_frame=df, x='PERIODO', y='PRECIPITACAO TOTAL, HORARIO (mm)', color='ESTACAO DO ANO')
st.plotly_chart(fig)

# Ideias

# Criar presets para cada tipo de tabela
# eg df por estação -> color=estacao; df por mes -> color=mes
# nesse caso, separar os arquivos por pasta facilitaria trocar as variaveis do grafico

# Ter dfs mais genéricos permite criar gráficos usando seletores no próprio site
# nesse caso, o tamanho dos arquivos seria um fator limitante, se seguirmos usando o github para armazenar
# os dfs precisam ser criados já com as possibilidades de segmentação inclusas
# eg estacao do ano, mes, dia da semana, media movel n dias, soma ultimos n dias, etc
