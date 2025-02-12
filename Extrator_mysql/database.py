import concurrent.futures
import gc
import logging
import os
import shutil
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import List, Dict, Tuple, Set, Optional

import pyarrow.parquet as pq
import pandas as pd
import polars as pl
import pymysql
import pyodbc
import pytz
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.engine import Connection
from sqlalchemy.exc import OperationalError

from config import DATABASE_CONFIG, GENERAL_CONFIG, STORAGE_CONFIG, obter_diretorio_temporario
from dicionario_dados import obter_dicionario_tipos, ajustar_tipos_dados
from storage import enviar_resultados

# ===================================================
# CONEXÃO COM O BANCO
# ===================================================
def obter_versao_mysql(host: str, port: int, database: str, user: str, password: str) -> Optional[str]:
    """
    Obtém a versão do MySQL conectando-se ao banco via PyMySQL.
    Em servidores antigos (ex.: 4.0.26), pode ocorrer erro de autenticação,
    fazendo com que a versão não seja obtida (retornando None).

    Args:
        host (str): Endereço do servidor.
        port (int): Porta do servidor.
        database (str): Nome do banco de dados.
        user (str): Nome de usuário.
        password (str): Senha de acesso.

    Returns:
        Optional[str]: Versão do MySQL (ex.: "8.0.23" ou "4.0.26") ou None se não for possível obter.
    """
    try:
        conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database)
        cursor = conn.cursor()
        cursor.execute("SELECT VERSION();")
        result = cursor.fetchone()
        version = result[0] if result else None
        cursor.close()
        conn.close()
        logging.info(f"Versão do MySQL detectada: {version}")
        return version
    except Exception as e:
        logging.error(f"Erro ao obter a versão do MySQL: {e}")
        return None

def detectar_driver_mysql(host: str, port: int, database: str, user: str, password: str,
                            driver_especifico: Optional[str] = None) -> Optional[str]:
    """
    Detecta e seleciona o driver ODBC mais adequado para MySQL disponível no sistema,
    identificando automaticamente a versão do banco e recomendando o driver ideal.
    Se um driver específico for informado e estiver disponível, ele será utilizado.

    Versões mais usadas de drivers ODBC para MySQL:
      - MySQL ODBC 8.0 Unicode Driver
      - MySQL ODBC 5.3 Unicode Driver
      - MySQL ODBC 5.1 Driver
      - MySQL ODBC 3.51 Driver

    Args:
        host (str): Endereço do servidor.
        port (int): Porta do servidor.
        database (str): Nome do banco de dados.
        user (str): Nome de usuário.
        password (str): Senha de acesso.
        driver_especifico (Optional[str]): Nome do driver desejado, se disponível.

    Returns:
        Optional[str]: O nome do driver MySQL mais adequado ou None se nenhum driver for detectado.
    """
    versao_banco = obter_versao_mysql(host, port, database, user, password)
    if versao_banco:
        try:
            major_version = int(versao_banco.split('.')[0])
        except Exception as e:
            logging.error(f"Erro ao interpretar a versão do MySQL: {versao_banco}. Erro: {e}")
            major_version = None
    else:
        major_version = None

    if major_version is None or major_version < 5:
        driver_recomendado = "MySQL ODBC 3.51 Driver"
    elif major_version >= 8:
        driver_recomendado = "MySQL ODBC 8.0 Unicode Driver"
    else:
        driver_recomendado = "MySQL ODBC 5.3 Unicode Driver"

    logging.info(f"Driver recomendado para MySQL versão {versao_banco if versao_banco else 'desconhecida'}: {driver_recomendado}")

    drivers = [driver for driver in pyodbc.drivers() if "MySQL" in driver]
    drivers.sort(reverse=True)

    if driver_especifico and driver_especifico in drivers:
        logging.info(f"Usando driver específico solicitado: {driver_especifico}")
        return driver_especifico

    if driver_recomendado in drivers:
        logging.info(f"O driver recomendado {driver_recomendado} está disponível e será utilizado.")
        return driver_recomendado
    else:
        if drivers:
            logging.error(f"O driver recomendado {driver_recomendado} para MySQL não está disponível. "
                          f"Utilizando o driver mais recente disponível: {drivers[0]}.")
            return drivers[0]
        else:
            logging.error("Nenhum driver ODBC do MySQL foi encontrado.")
            return None

# ------------------------------------------------------------------------------
# Classe wrapper que cria uma nova conexão para cada cursor via ODBC.
# Essa abordagem garante que a conexão retorne um cursor iterável, compatível com pandas.
# ------------------------------------------------------------------------------
class MultiplexConnection:
    def __init__(self, dsn: str, autocommit: bool):
        self._dsn = dsn
        self._autocommit = autocommit

    def cursor(self):
        # Cria uma nova conexão ODBC a cada chamada de cursor() e retorna seu cursor.
        return pyodbc.connect(self._dsn, autocommit=self._autocommit).cursor()

    def close(self):
        # Como cada cursor abre sua própria conexão, não há conexão persistente para fechar.
        pass

    def commit(self):
        pass

    def rollback(self):
        pass

def conectar_ao_banco(host: str = "localhost", port: int = 3306, database: str = None,
                      user: str = None, password: str = None, pool_size: int = 10,
                      max_overflow: int = 20, usar_odbc: bool = False) -> Optional[object]:
    """
    Conecta a um banco de dados MySQL utilizando ODBC ou SQLAlchemy.
    Tenta primeiramente via ODBC; se falhar, tenta via SQLAlchemy.
    A função utiliza os parâmetros de conexão para detectar a versão do MySQL e adaptar


    Args:
        host (str): Host do banco de dados (default: "localhost").
        port (int): Porta do banco de dados (default: 3306).
        database (str): Nome do banco de dados.
        user (str): Usuário para autenticação.
        password (str): Senha para autenticação.
        pool_size (int): Tamanho do pool de conexões (default: 10).
        max_overflow (int): Número máximo de conexões extras (default: 20).
        usar_odbc (bool): Indica se deve usar ODBC para conectar (default: False).

    Returns:
        Optional[object]: Objeto de conexão se bem-sucedido; caso contrário, None.
    """
    try:
        logging.info("Tentando conectar ao banco de dados MySQL...")
        # Força o uso de ODBC para testar (pois o modelo com MySQL 4.0.26 funcionou via ODBC)
        usar_odbc = True
        if usar_odbc:
            driver = detectar_driver_mysql(host, port, database, user, password)
            if not driver:
                raise Exception("Nenhum driver ODBC do MySQL disponível.")

            # Monta o DSN conforme o modelo que funcionou com MySQL 4.0.26.
            # Se o driver for o 3.51, adiciona DisableAuthenticationPlugins=1 para evitar o erro do plugin.
            dsn = (f"DRIVER={{{driver}}};"
                   f"SERVER={host};"
                   f"DATABASE={database};"
                   f"USER={user};"
                   f"PASSWORD={password};"
                   f"PORT={port};"
                   "OPTION=3;")
            if "3.51" in driver:
                dsn += "DisableAuthenticationPlugins=1;"

            # Retorna um objeto MultiplexConnection que implementa cursor() corretamente
            conexao = MultiplexConnection(dsn, autocommit=True)
            logging.info(f"Conexão com o banco de dados MySQL via ODBC ({driver}) estabelecida com sucesso.")
            return conexao
        else:
            # Conexão via SQLAlchemy
            if not all([host, database, user, password]):
                raise ValueError("Para conectar via SQLAlchemy, os parâmetros 'host', 'database', 'user' e 'password' são obrigatórios.")

            versao = obter_versao_mysql(host, port, database, user, password)
            try:
                major_version = int(versao.split('.')[0]) if versao else None
            except Exception:
                major_version = None

            if major_version is not None and major_version < 5:
                url_conexao = f"mysql+mysqldb://{user}:{password}@{host}:{port}/{database}"
            else:
                url_conexao = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"

            engine = create_engine(url_conexao, pool_pre_ping=True, pool_recycle=3600,
                                   pool_size=pool_size, max_overflow=max_overflow)
            conexao = engine.connect()
            logging.info("Conexão com o banco de dados MySQL via SQLAlchemy estabelecida com sucesso.")
            return conexao

    except Exception as e:
        logging.error(f"Erro ao conectar ao banco de dados MySQL: {e}")
        return None

def fechar_conexao(conexao: Connection):
    """
    Closes a database connection.

    This function is used to safely close an existing database connection.
    It ensures that proper logging is done upon successful closure or
    any failures encountered during the process.

    Args:
        conexao (Connection): The database connection object to be closed.

    Raises:
        Exception: If there is an error during the closing of the connection.
    """
    try:
        conexao.close()
        logging.info("Conexão com o banco de dados fechada.")
    except Exception as e:
        logging.error(f"Erro ao fechar a conexão: {e}")


# ===================================================
# EXECUÇÃO DE CONSULTAS
# ===================================================
def executar_consultas(
    conexoes_config: dict,
    consultas: List[Dict[str, str]],
    pasta_temp: str,
    paralela: bool = False,
    workers: int = 4,
) -> Tuple[Dict[str, str], Dict[str, Set[str]]]:
    """
    Execute multiple database queries and store the results in a temporary folder. Supports both sequential and
    parallel processing of queries.

    Args:
        conexoes_config (dict): Dictionary containing database connection configuration.
            Example keys might include 'host', 'port', 'user', 'password', and 'database'.
        consultas (List[Dict[str, str]]): List of dictionaries, each representing a query to be executed.
            Each dictionary must have at least the keys 'name' (str) for identifying the query and
            'query' (str), which holds the SQL query string to be executed.
        pasta_temp (str): Path to the temporary folder where results and processed data will be stored.
        paralela (bool, optional): Boolean indicating whether the queries should be processed in parallel.
            Defaults to False for sequential processing.
        workers (int, optional): Number of workers/threads to use for parallel processing. Ignored if
            paralela is False. Defaults to 4.

    Returns:
        Tuple[Dict[str, str], Dict[str, Set[str]]]: A tuple where:
            - The first element is a dictionary mapping the name of each query to the path of its results.
            - The second element is a dictionary mapping the name of each query to a set of file partitions
              created during query execution.

    Raises:
        Exception: Logs errors that occur during query execution, including database connection failures
            or query processing errors.
    """
    resultados = {}
    particoes_criadas = {}
    os.makedirs(pasta_temp, exist_ok=True)

    conexao_persistente = conectar_ao_banco(**conexoes_config)

    def processa_consulta(consulta: Dict[str, str]) -> Tuple[str, str, Set[str]]:
        nome_consulta = consulta.get("name", "").replace(" ", "")
        query = consulta.get("query")
        try:
            inicio = time.time()
            pasta_consulta, particoes = executar_consulta(conexao_persistente, nome_consulta, query, pasta_temp)
            duracao = time.time() - inicio
            logging.info(f"Consulta '{nome_consulta}' processada em {duracao:.2f} segundos.")
            return nome_consulta, pasta_consulta, particoes
        except Exception as e:
            logging.error(f"Erro ao processar consulta '{nome_consulta}': {e}")
            return nome_consulta, None, set()

    try:
        if paralela:
            with ThreadPoolExecutor(max_workers=workers) as executor:
                futuros = {executor.submit(processa_consulta, consulta): consulta for consulta in consultas}
                for futuro in concurrent.futures.as_completed(futuros):
                    nome_consulta, pasta_consulta, particoes = futuro.result()
                    if pasta_consulta:
                        resultados[nome_consulta] = pasta_consulta
                        particoes_criadas[nome_consulta] = particoes
        else:
            for consulta in consultas:
                nome_consulta, pasta_consulta, particoes = processa_consulta(consulta)
                if pasta_consulta:
                    resultados[nome_consulta] = pasta_consulta
                    particoes_criadas[nome_consulta] = particoes
    except Exception as e:
        logging.error(f"Erro na execução das consultas: {e}")
    finally:
        if conexao_persistente:
            fechar_conexao(conexao_persistente)

    return resultados, particoes_criadas

def executar_consulta(conexao, nome: str, query: str, pasta_temp: str) -> Tuple[str, Set[str]]:
    """
    Executes a database query, processes the result, and saves the output to a temporary folder.

    This function attempts to execute a SQL query using a given database connection.
    It logs the execution process, including any connection errors or exceptions, and retries
    the operation up to a specified number of attempts. If the query returns data, it processes
    the dataset and returns a path indicating where the data is stored along with information
    about the processed content.

    Parameters:
        conexao: Database connection object
            The active database connection to execute the query.
        nome: str
            A descriptive name for the query being executed, used for logging purposes.
        query: str
            SQL query string to execute using the given connection.
        pasta_temp: str
            Path to the temporary folder where the query results are processed and saved.

    Returns:
        Tuple[str, Set[str]]:
            A tuple containing:
            - A string representing the path to the processed data, if successful.
            - A set of additional metadata extracted during query result processing.

    Raises:
        OperationalError:
            If there is a database connection error during query execution.
        Exception:
            If any other error occurs during query execution or processing.
    """
    retries = 5
    for tentativa in range(retries):
        try:
            logging.info(f"Executando consulta: {nome}...")
            df_pandas = pd.read_sql(query, con=conexao)
            if df_pandas.empty:
                logging.warning(f"Consulta '{nome}' retornou um DataFrame vazio.")
                return "", set()
            total_registros = len(df_pandas)
            logging.info(f"Consulta '{nome}' finalizada. Total de registros: {total_registros}")
            return processar_dados(df_pandas, nome, pasta_temp)
        except OperationalError as e:
            logging.warning(f"Erro de conexão na consulta '{nome}', tentativa {tentativa+1}/{retries}: {e}")
            time.sleep(5)
        except Exception as e:
            logging.error(f"Erro ao executar a consulta '{nome}': {e}")
            return "", set()
    logging.error(f"Consulta '{nome}' falhou após {retries} tentativas.")
    return "", set()

def processar_dados(df_pandas: pd.DataFrame, nome: str, pasta_temp: str) -> Tuple[str, Set[str]]:
    """
    Processes a pandas DataFrame by applying transformations, converting it to a Polars DataFrame,
    and saving the data in a partitioned Parquet format. Additionally, it handles specific adjustments
    based on the data context (e.g., sales or purchases) and ensures certain columns are correctly
    formatted or present. The function creates a temporary directory for saving files and logs
    information about the process flow.

    Parameters:
        df_pandas (pd.DataFrame): Input pandas DataFrame to be processed.
        nome (str): Name of the dataset, used for context-specific column handling.
        pasta_temp (str): Path to the temporary folder where files will be saved.

    Returns:
        Tuple[str, Set[str]]: A tuple where the first element is the path to the generated dataset,
        and the second element is a set containing the paths of the created partitions.

    Raises:
        ValueError: If the required 'idEmpresa' column is missing from the processed Polars DataFrame.
    """
    try:
        os.makedirs(pasta_temp, exist_ok=True)
        pasta_consulta = os.path.join(pasta_temp, nome)
        logging.info(f"Processando dados da consulta '{nome}'...")
        coluna_data = None
        if nome == "Vendas" and "DataVenda" in df_pandas.columns:
            coluna_data = "DataVenda"
        elif nome == "Compras" and "DataEmissaoNF" in df_pandas.columns:
            coluna_data = "DataEmissaoNF"

        if "HoraVenda" in df_pandas.columns:
            if pd.api.types.is_timedelta64_dtype(df_pandas["HoraVenda"]):
                df_pandas["HoraVenda"] = df_pandas["HoraVenda"].apply(lambda x: str(x).split()[-1] if not pd.isna(x) else "00:00:00")
            elif df_pandas["HoraVenda"].dtype == "object":
                df_pandas["HoraVenda"] = df_pandas["HoraVenda"].astype(str).str.extract(r"(\d{2}:\d{2}:\d{2})")[0].fillna("00:00:00")

        # Conversão para Polars – utilizando a função from_pandas (ou from_arrow se for vantajoso)
        df_polars = pl.from_pandas(df_pandas).with_columns([
            pl.lit(datetime.now(pytz.timezone("America/Sao_Paulo")).strftime("%d/%m/%Y %H:%M:%S")).alias("DataHoraAtualizacao"),
            pl.lit(STORAGE_CONFIG["idemp"]).alias("idEmpresa"),
            pl.lit(STORAGE_CONFIG["idemp"]).alias("idEmp")
        ])

        if coluna_data:
            df_polars = df_polars.with_columns(pl.col(coluna_data).cast(pl.Utf8))
            df_polars = df_polars.with_columns([
                pl.col(coluna_data).str.slice(0, 4).alias("Ano"),
                pl.col(coluna_data).str.slice(5, 2).alias("Mes")
            ])
         #   amostra_particoes = df_polars.select(["Ano", "Mes", "Dia"]).unique().head(5)
         #   logging.info(f"Amostra das partições para '{nome}':\n{amostra_particoes.to_pandas().to_string(index=False)}")

        df_polars = ajustar_tipos_dados(df_polars, nome)
        if 'idEmpresa' not in df_polars.schema:
            raise ValueError("A coluna 'idEmpresa' é obrigatória para particionamento.")

        logging.info(f"Salvando '{nome}' em formato particionado...")
        partition_cols = ["idEmpresa"] + (["Ano", "Mes"] if coluna_data else [])
        pq.write_to_dataset(
            df_polars.to_arrow(),
            root_path=pasta_consulta,
            partition_cols=partition_cols,
            compression="snappy",
            use_dictionary=True,
            row_group_size=500_000
        )
        logging.info(f"Salvamento concluído para '{nome}'. Arquivos disponíveis em: {pasta_consulta}")

        particoes_criadas = {os.path.join(pasta_consulta, d) for d in os.listdir(pasta_consulta)}
        return pasta_consulta, particoes_criadas

    except Exception as e:
        logging.error(f"Erro ao processar dados da consulta '{nome}': {e}")
        return "", set()
