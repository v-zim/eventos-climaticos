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

# Seletor de colunas
colunas = st.multiselect("Selecione as colunas desejadas:", df.columns)
df_filtrado = df.select(colunas)

# st.table(set(df.dtypes))
# dtypes:
# Float32
# Time
# Date
# String

opcoes_agg = ["Soma", "Média"]

col_texto = [c for c in df_filtrado.columns if type(df_filtrado.get_column(c).dtype) == pl.String]
col_numerica = [c for c in df_filtrado.columns if type(df_filtrado.get_column(c).dtype) == pl.Float32]

# Gráfico

agrupar = st.multiselect("Agrupar dados por:", col_texto)
agregar = st.selectbox("Agregar dados de:", col_numerica)
eixo_x = st.selectbox("Eixo x:", agrupar)
series = st.selectbox("Series:", agrupar)

try:
    df_grafico = df_filtrado.group_by(agrupar).agg(pl.col(agregar).sum()).sort(agrupar[0], agrupar[1])

    st.write("Preview da tabela")
    st.dataframe(df_grafico)

    st.write("Gráfico:")
    fig = px.line(data_frame=df_grafico, x=eixo_x, y=agregar, color=series)
    st.plotly_chart(fig)

except:
    st.write("Indisponível")



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
