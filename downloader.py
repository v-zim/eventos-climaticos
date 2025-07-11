import os
import polars
import time
import zipfile
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.support.wait import WebDriverWait

ARQUIVO_MAIS_RECENTE = "C:/Users/Administrador/Documents/Developer/Projeto Eventos Climaticos/arquivo mais recente.txt"
PASTA_DOWNLOADS = "C:/Users/Administrador/Downloads"
PASTA_PARQUETS = "C:/Users/Administrador/Documents/Developer/Projeto Eventos Climaticos/parquets"
URL_INMET = "https://portal.inmet.gov.br/dadoshistoricos"


def checar_inmet():
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
    all_elements = driver.find_elements(By.TAG_NAME, "a")
    for element in all_elements:
        if element.text.startswith("ANO"):
            nome_periodo = element.text
            ano = nome_periodo.split(" ")[1]
            nome_zip = element.get_attribute("href").split("/")[-1]

            # Checar se há necessidade de baixar arquivo
            if ultimo_download == "" or (ano >= ultimo_download.split(" ")[1] and nome_periodo != ultimo_download):
                # Baixar arquivo
                element.click()
                caminho_arquivo = f'{PASTA_DOWNLOADS}/{nome_zip}'
                print(f"Baixando arquivo {nome_zip}...")

                # Deletar parquet do ano atual, caso exista
                if os.path.exists(f"{PASTA_PARQUETS}/{ano}.parquet"):
                    os.remove(f"{PASTA_PARQUETS}/{ano}.parquet")

                # Aguardar conclusão do download
                while not os.path.exists(caminho_arquivo):
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

                                # Transformar dados que antecedem a tabela em colunas adicionais
                                elementos_linha = linha_atual.split(";")
                                if len(elementos_linha) == 2:
                                    cabecalho_extra.append(elementos_linha[0].replace(":", ""))
                                    valores_extra.append(elementos_linha[-1]. replace("\n", ""))
                                    continue

                                # Parar loop ao final das linhas válidas
                                if len(elementos_linha) < 2:
                                    break
                                
                                # Incluir dados no array principal
                                if len(cabecalho) == 0:
                                    cabecalho = elementos_linha + cabecalho_extra
                                else:
                                    valores.append(elementos_linha + valores_extra)

                            # Criar DataFrame atual
                            df_atual = polars.DataFrame(valores, schema=cabecalho, orient="row")

                            # Criar DF consolidado ou agregar os dados
                            print("Arquivo finalizado.")

                            if criarDF:
                                df = df_atual
                                criarDF = False
                                continue

                            df = polars.concat([df, df_atual], how="vertical")

                    # Salvar DataFrame como parquet
                    df.write_parquet(f"{PASTA_PARQUETS}/{ano}.parquet")

                # Atualizar nome no histórico
                with open(ARQUIVO_MAIS_RECENTE, "w") as historico:
                    historico.write(nome_periodo)

                # Deletar arquivo zip
                os.remove(caminho_arquivo)

    # Fechar driver
    driver.quit()
