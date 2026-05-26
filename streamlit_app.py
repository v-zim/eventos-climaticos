import streamlit as st

import datetime
from pathlib import Path
import plotly
import plotly.express as px
import plotly.io as pio
import polars as pl

PASTA_PARQUETS = Path("parquets_remoto")

# Para debug:
# print(Path.cwd())
# print(list(Path(".").iterdir()))

# Listar arquivos disponíveis
lista_parquets = list(PASTA_PARQUETS.glob("*.parquet"))
parquets = [p.name.split(".")[0] for p in lista_parquets]

# App

# Título
st.title("Eventos climáticos")

# Seletor de data frame
escolhido = st.selectbox("Selecione o arquivo desejado:", parquets)

caminho_arquivo = f"{PASTA_PARQUETS}/{escolhido}.parquet"
df = pl.read_parquet(caminho_arquivo)

# Preview da tabela
st.write("Preview do dataframe")
st.dataframe(df.tail(5))

# Seletor de periodo
ano_min = df[0, "ANO"]
mes_min = df[0, "MES"]
dia_min = df[0, "DIA"]
data_min = datetime.date(int(ano_min), int(mes_min), int(dia_min))

ano_max = df[-1, "ANO"]
mes_max = df[-1, "MES"]
dia_max = df[-1, "DIA"]
data_max = datetime.date(int(ano_max), int(mes_max), int(dia_max))

periodo = st.date_input(
    "Selecione o período desejado:",
    (data_min, data_max),
    data_min,
    data_max,
    format="DD/MM/YYYY",
)

df = df.filter(pl.col("DATA (YYYY-MM-DD)") >= str(periodo[0]))
df = df.filter(pl.col("DATA (YYYY-MM-DD)") <= str(periodo[1]))

# Seletor de colunas
colunas = st.multiselect("Selecione as colunas desejadas:", df.columns)
df_filtrado = df.select(colunas)

# st.table(set(df.dtypes))
# dtypes:
# Float32
# Time
# Date
# String

col_texto = [c for c in df_filtrado.columns if type(df_filtrado.get_column(c).dtype) == pl.String]
col_numerica = [c for c in df_filtrado.columns if type(df_filtrado.get_column(c).dtype) == pl.Float32]

# Gráfico

agrupar = st.multiselect("Agrupar dados por (escolha pelo menos 2 opções):", col_texto)
agregar = st.selectbox("Agregar dados de:", col_numerica)
opcoes_agregar = [
    "count",
    "max",
    "mean",
    "median",
    "min",
    "product",
    "quantile",
    "std",
    "sum",
    "var",
]
agregar_por = st.selectbox("Opções de agregação:", opcoes_agregar)

eixo_x = st.selectbox("Eixo x:", agrupar)
series = st.selectbox("Series:", agrupar)

# Tipo de gráfico
opcoes_grafico = [
    "bar",
    "box",
    "ecdf",
    "funnel",
    "histogram",
    "line",
    "scatter",
    "strip",
    "violin",
]
tipo_grafico = st.selectbox("Selecione o tipo de gráfico:", opcoes_grafico)

try:
    match agregar_por:
        case "count":
            df_grafico = df_filtrado.group_by(agrupar).agg(pl.col(agregar).count()).sort(agrupar[0], agrupar[1])
        case "max":
            df_grafico = df_filtrado.group_by(agrupar).agg(pl.col(agregar).max()).sort(agrupar[0], agrupar[1])
        case "mean":
            df_grafico = df_filtrado.group_by(agrupar).agg(pl.col(agregar).mean()).sort(agrupar[0], agrupar[1])
        case "median":
            df_grafico = df_filtrado.group_by(agrupar).agg(pl.col(agregar).median()).sort(agrupar[0], agrupar[1])
        case "min":
            df_grafico = df_filtrado.group_by(agrupar).agg(pl.col(agregar).min()).sort(agrupar[0], agrupar[1])
        case "product":
            df_grafico = df_filtrado.group_by(agrupar).agg(pl.col(agregar).product()).sort(agrupar[0], agrupar[1])
        case "quantile":
            df_grafico = df_filtrado.group_by(agrupar).agg(pl.col(agregar).quantile()).sort(agrupar[0], agrupar[1])
        case "std":
            df_grafico = df_filtrado.group_by(agrupar).agg(pl.col(agregar).std()).sort(agrupar[0], agrupar[1])
        case "sum":
            df_grafico = df_filtrado.group_by(agrupar).agg(pl.col(agregar).sum()).sort(agrupar[0], agrupar[1])
        case "var":
            df_grafico = df_filtrado.group_by(agrupar).agg(pl.col(agregar).var()).sort(agrupar[0], agrupar[1])

    st.write("Preview da tabela")
    st.dataframe(df_grafico)

    st.write("Gráfico:")

    match tipo_grafico:
        case "bar":
            fig = px.bar(data_frame=df_grafico, x=eixo_x, y=agregar, color=series)
        case "box":
            fig = px.box(data_frame=df_grafico, x=eixo_x, y=agregar, color=series)
        case "ecdf":
            fig = px.ecdf(data_frame=df_grafico, x=eixo_x, y=agregar, color=series)
        case "funnel":
            fig = px.funnel(data_frame=df_grafico, x=eixo_x, y=agregar, color=series)
        case "histogram":
            fig = px.histogram(data_frame=df_grafico, x=eixo_x, y=agregar, color=series)
        case "line":
            fig = px.line(data_frame=df_grafico, x=eixo_x, y=agregar, color=series)
        case "scatter":
            fig = px.scatter(data_frame=df_grafico, x=eixo_x, y=agregar, color=series)
        case "strip":
            fig = px.strip(data_frame=df_grafico, x=eixo_x, y=agregar, color=series)
        case "violin":
            fig = px.violin(data_frame=df_grafico, x=eixo_x, y=agregar, color=series)


    st.plotly_chart(fig)

except:
    st.write("Gráfico indisponível")



# Ideias

# Criar presets para cada tipo de tabela
# eg df por estação -> color=estacao; df por mes -> color=mes
# nesse caso, separar os arquivos por pasta facilitaria trocar as variaveis do grafico

# Ter dfs mais genéricos permite criar gráficos usando seletores no próprio site
# > colocar no próprio site as colunas do df, um seletor pro x, y, color, etc
# > nesse caso, o tamanho dos arquivos seria um fator limitante, se seguirmos usando o github para armazenar
# os dfs podem ser manipulados para incluir novos parametros.
# > eg estacao do ano, mes, dia da semana, media movel n dias, soma ultimos n dias, etc
# > é interessante dar esse tipo de liberdade no site ou é melhor já providenciar?
