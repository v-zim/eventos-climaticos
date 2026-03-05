from pathlib import Path
import polars

PASTA_PARQUETS = "C:/Users/Administrador/Documents/Developer/Python/Projeto Eventos Climaticos/parquets"

def gerar_df_unificado():
    
    dfs = []

    # Iterar arquivos parquet na pasta
    for arquivo in Path(PASTA_PARQUETS).iterdir():
        if arquivo.is_file():
            
            print(f"Acessando {arquivo.name}...")

            # Criar df inicial
            df = polars.read_parquet(arquivo)

            # Checar colunas (útil para debug)
            # print(df.columns)

            dfs.append(df)
            
            print("Concluído.")

    # Unificar dfs e retornar
    df_unificado = polars.concat(dfs, how="vertical")
    return df_unificado
