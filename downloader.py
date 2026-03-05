import os
import polars as pl
import time
import zipfile
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.support.wait import WebDriverWait

ARQUIVO_MAIS_RECENTE = "C:/Users/Administrador/Documents/Developer/Python/Projeto Eventos Climaticos/arquivo mais recente.txt"
PASTA_DOWNLOADS = "C:/Users/Administrador/Downloads"
PASTA_PARQUETS = "C:/Users/Administrador/Documents/Developer/Python/Projeto Eventos Climaticos/parquets"
URL_INMET = "https://portal.inmet.gov.br/dadoshistoricos"
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


def extrair_dados():
    # Checar último download
    ultimo_download = ""
    with open(ARQUIVO_MAIS_RECENTE, "r") as historico:
        ultimo_download = historico.readline()

    # Abrir driver
    driver = webdriver.Chrome()

    # Acessar site do INMET com os arquivos para download
    driver.get(URL_INMET)
    WebDriverWait(driver, 60).until(ec.presence_of_element_located((By.ID, "main")))

    # Listar arquivos para download
    todos_elementos = driver.find_elements(By.TAG_NAME, "a")
    for elemento in todos_elementos:
        if elemento.text.startswith("ANO"):
            nome_periodo = elemento.text
            ano = nome_periodo.split(" ")[1]
            nome_zip = elemento.get_attribute("href").split("/")[-1]

            # Checar se há necessidade de baixar arquivo
            if ultimo_download == "" or (ano >= ultimo_download.split(" ")[1] and nome_periodo != ultimo_download):
                # Baixar arquivo
                elemento.click()
                caminho_arquivo = f'{PASTA_DOWNLOADS}/{nome_zip}'
                print(f"Baixando arquivo {nome_zip}...")

                # Deletar parquet do ano atual, caso exista
                if os.path.isfile(f"{PASTA_PARQUETS}/{ano}.parquet"):
                    os.remove(f"{PASTA_PARQUETS}/{ano}.parquet")

                # Aguardar conclusão do download
                while not os.path.isfile(caminho_arquivo):
                    time.sleep(1)
                
                print("Download completo.")

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

                    # Salvar DataFrame como parquet
                    df.write_parquet(f"{PASTA_PARQUETS}/{ano}.parquet")

                # Atualizar nome no histórico
                with open(ARQUIVO_MAIS_RECENTE, "w") as historico:
                    historico.write(nome_periodo)

                # Deletar arquivo zip
                os.remove(caminho_arquivo)

    # Fechar driver
    driver.quit()
    print("Atualização dos arquivos completa.")
