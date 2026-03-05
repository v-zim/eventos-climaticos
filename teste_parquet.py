import polars

PATH = "C:/Users/Administrador/Documents/Developer/Python/Projeto Eventos Climaticos/parquets"

CABECALHO_PADRAO = [
    "DATA (YYYY-MM-DD)",
    "HORA (UTC)",
    "PRECIPITACAO TOTAL, HORARIO (mm)",
    "PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (mB)",
    "PRESSAO ATMOSFERICA MAX.NA HORA ANT. (AUT) (mB)",
    "PRESSAO ATMOSFERICA MIN. NA HORA ANT. (AUT) (mB)",
    "RADIACAO GLOBAL (KJ/m²)",
    "TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)",
    "TEMPERATURA DO PONTO DE ORVALHO (°C)",
    "TEMPERATURA MAXIMA NA HORA ANT. (AUT) (°C)",
    "TEMPERATURA MINIMA NA HORA ANT. (AUT) (°C)",
    "TEMPERATURA ORVALHO MAX. NA HORA ANT. (AUT) (°C)",
    "TEMPERATURA ORVALHO MIN. NA HORA ANT. (AUT) (°C)",
    "UMIDADE REL. MAX. NA HORA ANT. (AUT) (%)",
    "UMIDADE REL. MIN. NA HORA ANT. (AUT) (%)",
    "UMIDADE RELATIVA DO AR, HORARIA (%)",
    "VENTO, DIRECAO HORARIA (gr) (° (gr))", 
    "VENTO, RAJADA MAXIMA (m/s)",
    "VENTO, VELOCIDADE HORARIA (m/s)",
    "REGIAO",
    "UF",
    "ESTACAO",
    "CODIGO (WMO)",
    "LATITUDE",
    "LONGITUDE",
    "ALTITUDE",
    "DATA DE FUNDACAO (YYYY-MM-DD)",
]

# str_teste = "00/01/01"
# s = str_teste.replace("/", "-").replace(";,", ";0,").replace("-9999", "null").replace("a", "b")
# print(0 < s.find("-") < 4)

df = polars.read_parquet(f"{PATH}/2000.parquet")
print(df.head(50))

# df = df.with_columns([
#     polars.col("Data")
#       .cast(polars.Utf8)                                    # 1. Ensure string type
#       .str.replace_all("/", "-", literal=True)              # 2. Replace all '/' with '-'
#       .str.strip_chars()                                    # 3. Remove any whitespace
#       .str.strptime(polars.Date, "%Y-%m-%d", strict=True)   # 4. Parse to proper date
#       .alias("Data")                                        # 5. Replace original column
# ])

# print(df.head(5))

