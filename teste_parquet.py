import polars

PATH = "C:/Users/Administrador/Documents/Developer/Projeto Eventos Climaticos"

df = polars.read_parquet(f"{PATH}/parquets/2001.parquet")

print(df)
