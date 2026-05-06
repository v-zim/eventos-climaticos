from pathlib import Path
import os
import polars as pl
import zipfile

import util

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
PASTA_PARQUETS_LOCAL = "C:/Users/Administrador/Documents/Developer/Python/Projeto Eventos Climaticos/parquets_local"
PASTA_PARQUETS_REMOTO = "C:/Users/Administrador/Documents/Developer/Python/Projeto Eventos Climaticos/parquets_remoto"

def gerar_parquet(caminho_arquivo):
    # Acessar arquivos csv dentro do arquivo zip
    with zipfile.ZipFile(caminho_arquivo) as arquivo_zip:
        arquivos_csv = arquivo_zip.namelist()
        criarDF = True

        # Acessar arquivos csv
        for arquivo_csv in arquivos_csv:
            print(f"Acessando {arquivo_csv}...")

            with arquivo_zip.open(arquivo_csv, "r") as csv_atual:
                cabecalho = []
                valores = []
                cabecalho_extra = []
                valores_extra = []

                while True:
                    # Transformar linha de bytes para string e aplicar encoding para leitura correta dos caracteres especiais
                    linha_atual = csv_atual.readline().decode(encoding="latin_1")

                    # Padronizar e fazer correções nos dados
                    linha_ajustada = linha_atual.replace("/", "-").replace("00 UTC", ":00").replace(";,", ";0,").replace(",", ".").replace("-9999", "null").replace(";;", ";null;").replace(";\n", "").replace("\n", "")

                    # Separar valores da linha
                    elementos_linha = linha_ajustada.split(";")

                    # Parar loop ao final das linhas válidas
                    if len(elementos_linha) < 2:
                        break

                    # Transformar dados que antecedem a tabela em colunas adicionais
                    if len(elementos_linha) == 2:
                        cabecalho_extra.append(elementos_linha[0].replace(":", ""))

                        # Padronizar data (de dd-mm-aa, para aaaa-mm-dd)
                        # Encontrar posição do primeiro "-"
                        if(0 < elementos_linha[1].find("-") < 4):
                            data_atual = elementos_linha[1].split("-")
                            data_ajustada = f"20{data_atual[2]}-{data_atual[1]}-{data_atual[0]}"
                            valores_extra.append(data_ajustada)
                            continue

                        valores_extra.append(elementos_linha[1])
                        continue
                    
                    # Incluir dados no array principal
                    if len(cabecalho) == 0:
                        cabecalho = elementos_linha + cabecalho_extra
                    else:
                        valores.append(elementos_linha + valores_extra)

                # Criar DataFrame atual
                df_atual = pl.DataFrame(valores, schema=cabecalho, orient="row")

                # # Para testes
                # print(df_atual.head(50))

                # Criar DF consolidado ou agregar os dados
                print("Arquivo finalizado.")

                if criarDF:
                    df = df_atual
                    criarDF = False
                    continue

                df = pl.concat([df, df_atual], how="vertical")

        # Padronizar DataFrame
        df = pl.DataFrame(df, schema=CABECALHO_PADRAO)
        for i in range(len(CABECALHO_PADRAO)):
            if i == 0 or i == 26:
                # Date
                df = df.with_columns(
                    pl.col(CABECALHO_PADRAO[i]).str.to_date("%F")
                )
                continue

            elif i == 1:
                # Time
                df = df.with_columns(
                    pl.col(CABECALHO_PADRAO[i]).str.to_time("%R")
                )
                continue

            elif 19 <= i <= 22:
                # String
                continue

            else:
                # Float32
                df = df.with_columns(
                    pl.col(CABECALHO_PADRAO[i]).cast(pl.Float32, strict=False)
                )
                continue

        # Criar pasta parquets, caso necessário
        if not os.path.exists(PASTA_PARQUETS_LOCAL):
            os.makedirs(PASTA_PARQUETS_LOCAL)

        # Salvar DataFrame como parquet
        ano = caminho_arquivo.split("/")[-1].split(".")[0]
        salvar_df_local(df, ano)


def gerar_df_unificado():
    dfs = []

    # Iterar arquivos parquet na pasta
    for arquivo in Path(PASTA_PARQUETS_LOCAL).iterdir():
        if arquivo.is_file():
            # print(f"Acessando {arquivo.name}...")

            # Criar df inicial
            df = pl.read_parquet(arquivo)

            dfs.append(df)

    # Unificar dfs e retornar
    df_unificado = pl.concat(dfs, how="vertical")
    return df_unificado


def ajustar_df(df_original: pl.DataFrame) -> pl.DataFrame:
    df = df_original

    # Separar dados por sazonalidade
    # a. Por estação do ano
    datas = df.to_series(0).to_list()
    indices = []
    estacoes = []
    periodos = []
    for data in datas:
        resposta = util.calcular_estacao_periodo(data)
        indices.append(resposta.split("-")[0])
        estacoes.append(resposta.split("-")[1])
        periodos.append(resposta.split("-")[2])
    
    df = df.with_columns(
        pl.Series(name="INDICE ESTACAO", values=indices),
        pl.Series(name="ESTACAO DO ANO", values=estacoes), 
        pl.Series(name="PERIODO", values=periodos)
    )

    # Lidar com formato de data e hora
    # a. Converter data/hora em string
    df = df.with_columns(pl.col(CABECALHO_PADRAO[1]).dt.to_string(format="%H:%M"))
    df = df.with_columns(pl.col(CABECALHO_PADRAO[26]).dt.to_string(format="%Y-%m-%d"))
    df = df.with_columns(pl.col(CABECALHO_PADRAO[0]).dt.to_string(format="%Y-%m-%d"))

    return df


def salvar_df(df: pl.DataFrame, nome_arquivo: str, pasta: str):
    caminho_arquivo = f"{pasta}/{nome_arquivo}.parquet"

    # Sobrescrever arquivo anterior, caso exista
    if os.path.isfile(caminho_arquivo):
        os.remove(caminho_arquivo)

    df.write_parquet(caminho_arquivo)


def salvar_df_local(df: pl.DataFrame, nome_arquivo: str):
    salvar_df(df, nome_arquivo, PASTA_PARQUETS_LOCAL)

    
def salvar_df_remoto(df: pl.DataFrame, nome_arquivo: str):
    salvar_df(df, nome_arquivo, PASTA_PARQUETS_REMOTO)


