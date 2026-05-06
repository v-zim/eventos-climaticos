import plotly.express as px
import polars as pl
import streamlit as st

# st.title("teste")
# value = st.slider("alo", 0, 100)
# st.write("valor:", value)

def subir_grafico(fig):
    st.plotly_chart(fig)
