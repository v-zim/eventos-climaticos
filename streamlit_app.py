import streamlit as st

from pathlib import Path
import plotly
import plotly.express as px
import plotly.io as pio

PASTA_PARQUETS = Path("parquets_remoto")

print(Path.cwd())
print(list(Path(".").iterdir()))

# Plotar no app

st.title("teste teste")
value = st.slider("alo", 0, 100)
st.write("valor:", value)

# pio.renderers.default = "browser"
# fig = px.bar(data_frame=df_por_estacao, x='PERIODO', y='PRECIPITACAO TOTAL, HORARIO (mm)', color='ESTACAO DO ANO')
