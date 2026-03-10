import os
import polars as pl
import time
import zipfile
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.support.wait import WebDriverWait

import parquet_manager

ARQUIVO_MAIS_RECENTE = "C:/Users/Administrador/Documents/Developer/Python/Projeto Eventos Climaticos/arquivo mais recente.txt"
PASTA_DOWNLOADS = "C:/Users/Administrador/Downloads"
PASTA_PARQUETS = "C:/Users/Administrador/Documents/Developer/Python/Projeto Eventos Climaticos/parquets"
PASTA_ZIPS = "C:/Users/Administrador/Documents/Developer/Python/Projeto Eventos Climaticos - aux/zips"
URL_INMET = "https://portal.inmet.gov.br/dadoshistoricos"

def extrair_dados_inmet():
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

            # Caso os links para download estejam indisponíveis, abortar função
            if nome_zip == "":
                print("Links para download indisponíveis.")
                driver.quit()
                return False

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

                parquet_manager.gerar_parquet(caminho_arquivo)

                # Atualizar nome no histórico
                with open(ARQUIVO_MAIS_RECENTE, "w") as historico:
                    historico.write(nome_periodo)

                # Deletar arquivo zip
                os.remove(caminho_arquivo)

    # Fechar driver
    driver.quit()
    print("Atualização dos arquivos completa.")
    return True


def extrair_dados_locais():
    # Iterar arquivos zip na pasta
    for arquivo in Path(PASTA_ZIPS).iterdir():
        if arquivo.is_file():
            parquet_manager.gerar_parquet(f"{PASTA_ZIPS}/{arquivo.name}")

