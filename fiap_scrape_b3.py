import re
import time

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from bs4 import BeautifulSoup
import pandas as pd
import unidecode
import boto3

from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv()) # Load the .env file.

def save_parquet_to_s3(df, s3_bucket, s3_key, partition_cols=None):
    """
    Verifica se existe um arquivo com os mesmo dados no S3, se não existir, salva o parquet particionado (opcional).

    Args:
        df (pd.DataFrame): df
        s3_bucket (str): nome do bucket
        s3_key (str): s3 key
        partition_cols (str or list, optional): colunas para partição

    Returns:
        bool: True se der tudo certo, False se o arquivo já existe
    """
    # S3 Config
    s3_client = boto3.client('s3')

    try:
        # Checa se existe o arquivo no s3
        try:
            s3_client.head_object(Bucket=s3_bucket, Key=s3_key)
            print(f"File {s3_key} already exists in S3 bucket {s3_bucket}")
            return False
        except s3_client.exceptions.ClientError as e:
            # Caso não exista (erro), continua o processo
            if e.response['Error']['Code'] == '404':

                if isinstance(partition_cols, str):
                    partition_cols = [partition_cols]

                # Salva parquet temporário
                import tempfile
                import shutil

                with tempfile.TemporaryDirectory() as tmpdir:
                    # Armazena temp local
                    if partition_cols:
                        df.to_parquet(
                            tmpdir,
                            engine="pyarrow",
                            index=False,
                            partition_cols=partition_cols
                        )
                        # Upload dos arquivos particionados
                        for root, dirs, files in os.walk(tmpdir):
                            for file in files:
                                local_path = os.path.join(root, file)
                                relative_path = os.path.relpath(local_path, tmpdir)
                                s3_upload_key = os.path.join(s3_key, relative_path)
                                s3_client.upload_file(local_path, s3_bucket, s3_upload_key)
                    else:
                        # Salva o arquivo parquet
                        local_path = os.path.join(tmpdir, 'data.parquet')
                        df.to_parquet(local_path, engine="pyarrow", index=False)
                        s3_client.upload_file(local_path, s3_bucket, s3_key)

                print(f"Data saved to S3 path: s3://{s3_bucket}/{s3_key}")
                return True

            #
            raise

    except Exception as e:
        print(f"Error saving Parquet file to S3: {e}")
        return False


aws_access_key_id=os.getenv("AWS_KEY")
aws_secret_access_key=os.getenv("AWS_SECRET")
aws_session_token=os.getenv("AWS_TOKEN")

# Configura selenium
options = webdriver.ChromeOptions()
options.add_argument('--headless')
options.add_argument('--disable-gpu')
options.add_argument('--no-sandbox')
driver = webdriver.Chrome(options=options)

# URL para o scrape
url = "https://sistemaswebb3-listados.b3.com.br/indexPage/day/IBOV?language=en-us"


# Carrega Página
driver.get(url)
time.sleep(5)

# utiliza o dropdown do site para abrir todos as ações
dropdown = Select(driver.find_element(By.ID, "selectPage"))
dropdown.select_by_visible_text("120")
time.sleep(5)

# Pega o conteudo
html_content = driver.page_source

# Parser
soup = BeautifulSoup(html_content, 'html.parser')

# Fecha o browser selenium
driver.quit()

# Extrai as tags e infos do html
h2_tag = soup.find("h2", string=re.compile(r"Current Portfolio"))
if not h2_tag:
    raise Exception("Date not found in the <h2> tag!")
date_text = re.search(r"(\d{2}/\d{2}/\d{2})", h2_tag.text).group(1)
day, month, year = date_text.split("/")
formatted_date = f"20{year}-{day}-{month}"

# Encontra a tabela -
table = soup.find("table")
if not table:
    raise Exception("No table found on the webpage!")

# Pega os headers
headers = [header.text.strip() for header in table.find_all("th")]

# Pega as linhas
rows = []
for row in table.find_all("tr"):
    cells = [cell.text.strip() for cell in row.find_all(["td", "th"])]
    if cells:
        rows.append(cells)

#  Remnove primeira linha se for duplicada com o header
if rows and rows[0] == headers:
    rows.pop(0)

# Cria e transforma o dataset
df = pd.DataFrame(rows, columns=headers)

# Normalize column names
df.columns = [
    unidecode.unidecode(col).lower().replace(" ", "_") for col in df.columns
]

# Remove"Quantidade Teórica Total" ou "Redutor"
df = df[
    ~df.apply(lambda row: row.astype(str).str.contains("Total Theorethical Quantity|Reductor").any(), axis=1)
]

df = df.rename(columns={'part._(%)':'part_percentual'})

df['date'] = formatted_date

# Sa
parquet_file = f"./temp_data/"
df.to_parquet(parquet_file, engine="pyarrow", index=False, partition_cols='date')

print(f"Data saved to {parquet_file}")

import pyarrow.parquet as pq

"""### S3"""

# AWS creds
aws_access_key_id=aws_id
aws_secret_access_key=aws_key
aws_session_token=aws_token
region_name='us-east-1'

# S3 bucket
s3_bucket = "fiap-fase2"
s3_prefix = "bronze/"

# s3  client
s3_client = boto3.client(
    "s3",
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    aws_session_token=aws_session_token,
    region_name=region_name
)

s3_key = f"{s3_prefix}ibov"
save_parquet_to_s3(df, s3_bucket, s3_key, partition_cols=["date"])