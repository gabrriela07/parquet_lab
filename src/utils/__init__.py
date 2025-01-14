from sqlalchemy import create_engine
from unicodedata import normalize
from datetime import datetime
import re
import hashlib
from decouple import config
import warnings
import os
from os import listdir,getenv
import zipfile
import glob
import src.utils as utils
import pandas as pd
import zipfile
import shutil

MYSQL_USER = config('MYSQL_USER')
MYSQL_PASS = config('MYSQL_PASS')
MYSQL_HOST = config('MYSQL_HOST')
MYSQL_PORT = config('MYSQL_PORT')
ENV_BRONZE = config('ENV_BRONZE')
DUCKDB_DATABASE = config('DUCKDB_DATABASE')

def handle_conect_db(_mysql_db_name: str):
    # -- handle_conect_db
    engine = create_engine(f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASS}@{MYSQL_HOST}:{MYSQL_PORT}/{_mysql_db_name}')
    return engine

def handle_strip_string(str1_in: str) -> str:
    # -- Fun��o para remover objetos de strings
    # -- str1_in: string de entrada
 
    convert_string = str(str1_in)
    clear_obj = re.sub(r"^\s+|\s+$", "", convert_string)
    strip_string = clear_obj.strip().replace(' ', '').upper()  # equal TRIM
    hash_string_stripped = hashlib.md5(strip_string.encode())
    return hash_string_stripped.hexdigest()

def handle_normalize_strings(in_string: str) -> str:
    # -- handle_normalize_strings
    target = normalize('NFKD', in_string).encode('ASCII','ignore').decode('ASCII')
    target = target.replace('.','')
    target = target.replace('(','')
    target = target.replace(')','')
    target = target.replace('/','')
    target = target.replace('-','')
    return target

# comparando e adcionando colunas faltantes ao dataset original
def handle_headers_comparation(header_list, header_original) -> list:
    # -- handle_headers_comparation
    new_list = []
    for i in header_list:
        if i in header_original:
            pass
        else:
            new_list.append(i)
    return new_list

def handle_without_zero(in_string: str) -> str:
    # -- handle_without_zero
    _str_in = str(in_string)
    target = _str_in.replace('.0', '')
    target = target.strip()
    if target == '-3':
        str_output = 0
    else:
        str_output = target
    return str_output

def handle_ymonth(_dt: datetime) -> int:
    # -- handle_ymonth  
    s_year = _dt.year
    s_month = _dt.month
    s_ymonth = (s_year * 100 + s_month)
    return s_ymonth                                                        

if __name__ == '__main__':
    print('Tested!')

# ------------------------ parquet lab

def handle_count_files(path):
    """
    Retorna a quantidade de arquivos em um diretório
    """
    itens = os.listdir(path)
    files = [item for item in itens if os.path.isfile(os.path.join(path, item))]
    return len(files)

def handle_size_files(path):
    """
    Retorna o peso total dos arquivos em um diretório
    """
    itens = os.listdir(path)
    files = [item for item in itens if os.path.isfile(os.path.join(path, item))]
    peso_tt = sum(os.path.getsize(os.path.join(path, file)) for file in files)
    return peso_tt

def handle_unzip_files(path, path_tmp):
    """
    - Função:
        - Extrair os arquivos zipados e mover para uma pasta temporária
    - Args:
        - path(str): file target -> pasta onde os zips originais estão
        - path_tmp(str): path_tmp -> pasta temporária para extração de csv
    """
    files_extracted = 0

    # Verifica se o diretório temporário já existe, se não, cria
    os.makedirs(path_tmp, exist_ok=True)

    for item in os.listdir(path):
        if item.endswith('.zip'):
            zip_path = os.path.join(path, item)

            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                for file in zip_ref.namelist():
                    if file.endswith('.csv'):
                        extracted_path = zip_ref.extract(file, path_tmp)
                        zip_name = os.path.splitext(item)[0]  # Nome do arquivo ZIP sem extensão
                        new_path = os.path.join(path_tmp, f"{zip_name}.csv")
                        os.rename(extracted_path, new_path)

                        files_extracted += 1

def handle_new_dir_parquet(bucket_parquet, target):
    """
    - Etapas:
        - Verifica se a pasta com o mesmo nome ja existe no bucket de parquet's
        - Na ausência cria nova pasta com o mesmo nome do target
        - Se ja existe break
    - Args:
        - bucket_parquet(str): diretório raiz de parquets, onde será criada a nova pasta
        - target(str):
    """
    target_dir = os.path.join(bucket_parquet, target)

    # Verifica se o diretório já existe
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)

def handle_csv_to_parquet(src_dir, dest_dir):
    """
    Converte todos os arquivos CSV de um diretório para Parquet, move uma cópia para o diretório de destino,
    e depois apaga os arquivos CSV da pasta de destino.

    Args:
        src_dir (str): Diretório contendo os arquivos CSV a serem convertidos.
        dest_dir (str): Diretório onde os arquivos Parquet serão movidos.
    """

    # Contadores
    files_converted = 0
    files_removed = 0

    # Verifica se o diretório de destino existe, senão cria
    os.makedirs(dest_dir, exist_ok=True)

    # Percorre todos os arquivos no diretório de origem
    for file_name in os.listdir(src_dir):
        # Verifica se o arquivo tem a extensão .csv
        if file_name.endswith('.csv'):
            csv_path = os.path.join(src_dir, file_name)
            dest_csv_path = os.path.join(dest_dir, file_name)  # Caminho da cópia para o diretório de destino
            parquet_path = os.path.join(dest_dir, f"{os.path.splitext(file_name)[0]}.parquet")

            # Copia o arquivo CSV para o diretório de destino
            shutil.copy(csv_path, dest_csv_path)

            # Converte a cópia para Parquet se o arquivo Parquet ainda não existir
            if not os.path.exists(parquet_path):
                try:
                    try:
                        df = pd.read_csv(dest_csv_path, sep=';', encoding='ISO-8859-1', low_memory=False)
                    except UnicodeDecodeError:
                        df = pd.read_csv(dest_csv_path, sep=';', encoding='utf-8', low_memory=False)

                    if df.empty:
                        raise ValueError("O DataFrame está vazio após a leitura com sep=';'")
                except (pd.errors.ParserError, ValueError):
                    try:
                        df = pd.read_csv(dest_csv_path, sep=',', encoding='ISO-8859-1', low_memory=False)
                    except UnicodeDecodeError:
                        df = pd.read_csv(dest_csv_path, sep=',', encoding='utf-8', low_memory=False)

                # Verifica se o nome do target contém 'backlog'
                if 'backlog' in file_name.lower():
                    df = df.loc[df['SK_DATA'] != '--FIM--']
                    df['SK_DATA'] = pd.to_numeric(df['SK_DATA'], errors='coerce').astype('Int64')

                df.to_parquet(parquet_path, index=False)
                files_converted += 1

            # Apaga o arquivo CSV da pasta de destino após a conversão
            os.remove(dest_csv_path)
            files_removed += 1

def handle_clean_tmp(path):
    for file in os.listdir(path):
        file_path = os.path.join(path, file)
        if os.path.isfile(file_path):
            os.remove(file_path)