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
st.dataframe(df.head(5))

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

col_string = [c for c in df_filtrado.columns if type(df_filtrado.get_column(c).dtype) == pl.String]
col_date = [c for c in df_filtrado.columns if type(df_filtrado.get_column(c).dtype) == pl.Date]
col_time = [c for c in df_filtrado.columns if type(df_filtrado.get_column(c).dtype) == pl.Time]
col_float32 = [c for c in df_filtrado.columns if type(df_filtrado.get_column(c).dtype) == pl.Float32]

print(col_float32)

df_seletor = pl.DataFrame(
    {
        "Coluna": col_float32,
        "Agregar por": ["Agregar por..." for c in col_float32]
    },
    strict=False
)

st.data_editor(
    df_seletor, 
    column_config={
        "Agregar por": st.column_config.SelectboxColumn(
            "Agregar por",
            options=opcoes_agg,
            required=True,
            width="small",
        )
    }
)



# Gráfico
st.write("Gráfico:")
try:
    fig = px.bar(data_frame=df, x='PERIODO', y='PRECIPITACAO TOTAL, HORARIO (mm)', color='ESTACAO DO ANO')
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
