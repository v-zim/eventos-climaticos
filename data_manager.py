import polars as pl

import parquet_manager

LIMITE_PRECIPITACAO = 1
coluna = "PRECIPITACAO TOTAL, HORARIO (mm)"

# to-do: criar função genérica

df = parquet_manager.gerar_df_unificado()

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

df = df.filter(
    pl.col('UF').str.contains('SP'),
    pl.col('ESTACAO').is_in(estacoes_sp)
)

df = parquet_manager.ajustar_df(df)

def classificar_evento_extremo(
        df: pl.DataFrame,
        coluna: str,
        percentil: float = 0.95,
        valor_minimo: float = 1,
        agrupar_por_colunas: list[str] = ["ESTACAO", "MES"]
) -> pl.DataFrame:

    # Converter dados por hora em dados por dia
    df_por_dia = df \
        .group_by(["DATA (YYYY-MM-DD)", "ANO", "MES", "DIA", "ESTACAO"]) \
        .agg(
            pl.col(coluna) \
            .sum() \
            .alias(f"{coluna}_DIA")
        ) \
        .sort("DATA (YYYY-MM-DD)", "ESTACAO")

    # Agregar dados por mês e calcular o percentil diário
    df_percentil = df_por_dia \
        .filter(pl.col(f"{coluna}_DIA") >= valor_minimo) \
        .group_by(agrupar_por_colunas) \
        .agg(
            pl.col(f"{coluna}_DIA") \
            .quantile(0.95) \
            .alias(f"{coluna}_P{int(percentil * 100)}")
        )

    # Incluir percentil no DF por dia
    df_por_dia = df_por_dia.join(
        df_percentil,
        on=agrupar_por_colunas,
        how="left"
    )

    # Criar label de eventos extremos (valor diário > valor percentil do mês)
    df_classificado = df_por_dia.with_columns(
        (pl.col(f"{coluna}_DIA") > pl.col(f"{coluna}_P{int(percentil * 100)}")) \
        .cast(pl.Int8)
        .alias(f"{coluna}_EXTREMA")
    )

    return df_classificado
