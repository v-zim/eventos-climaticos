import polars as pl

COLUNA_PRECIPITACAO = "PRECIPITACAO TOTAL, HORARIO (mm)"
COLUNA_UMIDADE = "UMIDADE RELATIVA DO AR, HORARIA (%)"
COLUNA_TEMPERATURA = "TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)"
COLUNA_PRESSAO = "PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (mB)"
COLUNA_VENTO = "VENTO, VELOCIDADE HORARIA (m/s)"
COLUNA_RAJADA = "VENTO, RAJADA MAXIMA (m/s)"

def incluir_precipitacao(
        df: pl.DataFrame,
        percentil: int = 90,
        valor_minimo: float = 1,
        dias_acumulados: list[int] = [3, 7, 14, 30],
        agrupar_por_colunas: list[str] = ["ESTACAO", "MES"],
        coluna: str = COLUNA_PRECIPITACAO,
) -> pl.DataFrame:
    
    coluna_curta = "PRECIPITACAO"

    # Converter dados por hora em dados por dia
    df_por_dia = df \
        .group_by(["DATA (YYYY-MM-DD)", "ANO", "MES", "DIA", "ESTACAO"]) \
        .agg(
            pl.col(coluna) \
            .sum() \
            .alias(f"{coluna_curta}_acumulado_1d")
        ) \
        .sort("ESTACAO", "DATA (YYYY-MM-DD)")

    # Agregar dados por mês e calcular o percentil diário
    df_percentil = df_por_dia \
        .filter(pl.col(f"{coluna_curta}_acumulado_1d") >= valor_minimo) \
        .group_by(agrupar_por_colunas) \
        .agg(
            pl.col(f"{coluna_curta}_acumulado_1d") \
            .quantile(percentil / 100) \
            .alias(f"{coluna_curta}_P{int(percentil)}")
        )

    # Incluir percentil no DF por dia
    df_por_dia = df_por_dia.join(
        df_percentil,
        on=agrupar_por_colunas,
        how="left"
    )

    # Criar label de eventos extremos (valor diário > valor percentil do mês)
    df_classificado = df_por_dia.with_columns(
        (pl.col(f"{coluna_curta}_acumulado_1d") > pl.col(f"{coluna_curta}_P{int(percentil)}")) \
        .cast(pl.Int8)
        .alias(f"{coluna_curta}_EXTREMA")
    )

    # Calcular colunas acumuladas
    df_acumulados = df_classificado
    for d in dias_acumulados:
        df_acumulados = df_acumulados.with_columns(
            pl.col(f"{coluna_curta}_acumulado_1d")
            .rolling_sum(window_size=d, min_samples=1)
            .over('ESTACAO')
            .alias(f"{coluna_curta}_acumulado_{d}d")
        )
    
    return df_acumulados


def incluir_umidade(
        df: pl.DataFrame,
        coluna: str = COLUNA_UMIDADE
) -> pl.DataFrame:
    
    coluna_curta = "UMIDADE"

    # Calcular umidade média e máxima no dia
    df_umidade = df \
        .group_by(["DATA (YYYY-MM-DD)", "ANO", "MES", "DIA", "ESTACAO"]) \
        .agg(
            pl.col(coluna) \
                .mean() \
                .alias(f"{coluna_curta}_media_1d"),
            pl.col(coluna) \
                .max() \
                .alias(f"{coluna_curta}_max_1d")
        ) \
        .sort("ESTACAO", "DATA (YYYY-MM-DD)")
    
    return df_umidade


def incluir_temperatura(
        df: pl.DataFrame,
        agrupar_por_colunas: list[str] = ["ESTACAO", "MES"],
        coluna: str = COLUNA_TEMPERATURA,
) -> pl.DataFrame:
    
    coluna_curta = "TEMPERATURA"

    # Calcular temperatura mínima, máxima e média
    df_temperatura_dia = df \
        .group_by(["DATA (YYYY-MM-DD)", "ANO", "MES", "DIA", "ESTACAO"]) \
        .agg(
            pl.col(coluna) \
                .min() \
                .alias(f"{coluna_curta}_min_1d"),
            pl.col(coluna) \
                .max() \
                .alias(f"{coluna_curta}_max_1d"),
            pl.col(coluna) \
                .mean() \
                .alias(f"{coluna_curta}_media_1d"),
        ) \
        .sort("ESTACAO", "DATA (YYYY-MM-DD)")
    
    # Agregar dados por mês e calcular média mensal
    df_temperatura_mes = df_temperatura_dia \
        .group_by(agrupar_por_colunas) \
        .agg(
            pl.col(f"{coluna_curta}_media_1d") \
            .mean() \
            .alias(f"{coluna_curta}_media_mes")
        )

    # Incluir média mensal no df por dia
    df_temperatura = df_temperatura_dia.join(
        df_temperatura_mes,
        on=agrupar_por_colunas,
        how="left"
    )

    # Calcular anomalia (media_hoje - media_mes)
    df_temperatura = df_temperatura.with_columns(
        (pl.col(f"{coluna_curta}_media_1d") - pl.col(f"{coluna_curta}_media_mes")) \
        .cast(pl.Float32)
        .alias(f"{coluna_curta}_anomalia_1d")
    )
    
    return df_temperatura


def incluir_pressao(
        df: pl.DataFrame,
        dias: list[int] = [1, 2],
        coluna: str = COLUNA_PRESSAO,
) -> pl.DataFrame:
    
    coluna_curta = "PRESSAO"

    # Calcular pressão média
    df_pressao = df \
        .group_by(["DATA (YYYY-MM-DD)", "ANO", "MES", "DIA", "ESTACAO"]) \
        .agg(
            pl.col(coluna) \
                .mean() \
                .alias(f"{coluna_curta}_media_1d"),
        ) \
        .sort("ESTACAO", "DATA (YYYY-MM-DD)")
    
    # Calcular diferença de pressão
    for d in dias:
        df_pressao = df_pressao.with_columns(
            (pl.col(f"{coluna_curta}_media_1d") - pl.col(f"{coluna_curta}_media_1d").shift(d))
            .over('ESTACAO')
            .cast(pl.Float32)
            .alias(f"{coluna_curta}_diferenca_{d}d")
        )
    
    return df_pressao
    

def incluir_vento(
        df: pl.DataFrame,
        coluna_vento: str = COLUNA_VENTO,
        coluna_rajada: str = COLUNA_RAJADA,
) -> pl.DataFrame:
    
    coluna_vento_curta = "VENTO"
    coluna_rajada_curta = "RAJADA"

    # Calcular média, máxima e rajada máxima
    df_vento = df \
        .group_by(["DATA (YYYY-MM-DD)", "ANO", "MES", "DIA", "ESTACAO"]) \
        .agg(
            pl.col(coluna_vento) \
                .mean() \
                .alias(f"{coluna_vento_curta}_media_1d"),
            pl.col(coluna_vento) \
                .max() \
                .alias(f"{coluna_vento_curta}_max_1d"),
            pl.col(coluna_rajada) \
                .max() \
                .alias(f"{coluna_rajada_curta}_max_1d"),
        ) \
        .sort("ESTACAO", "DATA (YYYY-MM-DD)")
    
    return df_vento


def gerar_df_modelo(
    df: pl.DataFrame,
    p: int = 90
) -> pl.DataFrame:
    
    # Precipitação
    df_precipitacao = incluir_precipitacao(df, percentil=p)
    df_modelo = df_precipitacao

    dfs = []

    # Umidade
    df_umidade = incluir_umidade(df)
    dfs.append(df_umidade)

    # Temperatura
    df_temperatura = incluir_temperatura(df)
    dfs.append(df_temperatura)

    # Pressão
    df_pressao = incluir_pressao(df)
    dfs.append(df_pressao)

    # Vento
    df_vento = incluir_vento(df)
    dfs.append(df_vento)

    for df_aux in dfs:
        df_modelo = df_modelo.join(
            df_aux,
            on=["DATA (YYYY-MM-DD)", "ANO", "MES", "DIA", "ESTACAO"],
            how="left"
        )

    return df_modelo

