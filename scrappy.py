import asyncio
import os
import time
from datetime import datetime
from email.generator import Generator
from pathlib import Path
from typing import Dict, List, Union

import pandas as pd
import requests
from bs4 import BeautifulSoup
from lxml import etree

# caminho para o diretório atual
CURRENT_PATH: str = str(Path().absolute())

# definindo a extensão do arquivo de saída
DEFAULT_OUTPUT_EXT: str = ".csv"

# XPATH do elemento a ser pesquisado
DEFAULT_XPATH: str = """//*[@id="firstHeading"]"""

# Headers utilizado na requisição de cada URL
HEADERS: Dict[str, str] = {
    "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
}

# caminho para o arquivo com as URLs
URLS_FILE_PATH: str = f"{CURRENT_PATH}/urls.txt"

# lista de urls de teste
URLS: List[str] = [
    "https://pt.wikipedia.org/wiki/Banana",
    "https://pt.wikipedia.org/wiki/Maçã",
    "https://pt.wikipedia.org/wiki/Coca-Cola",
    "https://pt.wikipedia.org/wiki/Maçã",
    "https://pt.wikipedia.org/wiki/Ford_Motor_Company",
]


# decorator para tentar rerun uma requisição caso falhe
def rerun(retries: int = 3, delay: int = 5):
    def decorator(func):
        def wrapper(*args, **kwargs):
            count: int = 0
            resultado = func(*args, **kwargs)
            while resultado == "" and count < retries:
                time.sleep(delay)
                resultado = func(*args, **kwargs)
                count += 1

            return resultado

        return wrapper

    return decorator


# carregando um generator com as urls contendo no arquivo
def load_urls_from_file(file_path: str) -> Generator:
    with open(fila_path, "r") as file:
        return (line.replace("\n", "") for line in file.readlines())


# recuperando o conteúdo da página de cada URL
@rerun(retries=5)
def get_page_data(url: str) -> str:
    response: requests.Response = requests.get(url, headers=HEADERS)

    if response.status_code == 200:
        return response.content.decode("UTF-8")

    return ""


# carregando o conteúdo da página em um objeto do BeautifulSoup
def load_soup(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "html.parser")


# recuperando valor do elemento informando no parâmetro element ou vindo do DEFAULT_XPATH
def get_specific_element(soup: BeautifulSoup, element: str = DEFAULT_XPATH):
    dom: etree.Element = etree.HTML(str(soup))
    try:
        return dom.xpath(element)[0].text
    except:
        return ""


# processando cada URL
def process_url(url: str) -> str:
    page_str = get_page_data(url)
    soap = load_soup(page_str)
    return get_specific_element(soap)


# escrevendo o arquivo de saída
def write_output_file(
    data: List[str], output_path_file: str, output_to_txt: bool = True
) -> None:
    output_file_name: str = (
        f"""{output_path_file}/{datetime.now().strftime("%Y%m%d%H%M%S")}"""
    )
    if output_to_txt:
        with open(f"{output_file_name}.txt", "w") as file:
            file.writelines(data)
        return
    df: pd.DataFrame = pd.DataFrame(data=data)

    if DEFAULT_OUTPUT_EXT == ".csv":
        df.to_csv(path_or_buf=f"{output_file_name}.csv", sep=";", index=False)
        return

    df.to_excel(path_or_buf=f"{output_file_name}.xslx", index=False)


# função principal que dispara o processo assíncrono
def main() -> None:
    loop: asyncio.BaseEventLoop = asyncio.new_event_loop()

    _urls: Union[List[str], Generator] = (
        load_urls_from_file(file_path=URLS_FILE_PATH)
        if os.path.exists(URLS_FILE_PATH)
        else URLS
    )

    coroutines: List[asyncio.Future] = [
        loop.run_in_executor(None, process_url, *[url]) for url in _urls
    ]

    full_result = loop.run_until_complete(asyncio.gather(*coroutines))

    str_result: List[str] = [f for f in full_result]
    loop.close()

    print(str_result)

    write_output_file(
        data=str_result, output_path_file=CURRENT_PATH, output_to_txt=True
    )


if __name__ == "__main__":
    main()
