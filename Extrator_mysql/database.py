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
import pyodbc
import pytz
from sqlalchemy import create_engine
from sqlalchemy.engine import Connection
from sqlalchemy.exc import OperationalError

from config import DATABASE_CONFIG, GENERAL_CONFIG, STORAGE_CONFIG, obter_diretorio_temporario
from dicionario_dados import obter_dicionario_tipos, ajustar_tipos_dados
from storage import enviar_resultados

# ===================================================
# CONEXÃO COM O BANCO
# ===================================================
def detectar_driver_mysql():
    """
    Detecta automaticamente o driver ODBC do MySQL disponível no sistema.
    Retorna o nome do driver mais recente encontrado ou None se nenhum for encontrado.
    """
    drivers = [driver for driver in pyodbc.drivers() if "MySQL" in driver]
    if drivers:
        logging.info(f"Drivers MySQL ODBC detectados: {drivers}")
        return drivers[-1]  # Usa o driver mais recente disponível
    else:
        logging.error("Nenhum driver ODBC do MySQL foi encontrado.")
        return None


def conectar_ao_banco(host: str = "localhost", port: int = 3306, database: str = None,
                      user: str = None, password: str = None, pool_size: int = 10,
                      max_overflow: int = 20, usar_odbc: bool = False) -> Optional[Connection]:
    """
    Estabelece conexão com o banco de dados MySQL.
    Permite escolher entre SQLAlchemy (pymysql) e ODBC de forma dinâmica.

    :param host: Endereço do servidor MySQL (padrão "localhost").
    :param port: Porta do MySQL (padrão 3306).
    :param database: Nome do banco de dados.
    :param user: Usuário do banco de dados.
    :param password: Senha do banco de dados.
    :param pool_size: Tamanho do pool de conexões (apenas para SQLAlchemy).
    :param max_overflow: Número máximo de conexões extras (apenas para SQLAlchemy).
    :param usar_odbc: Se `True`, usa ODBC detectando o driver disponível. Se `False`, usa SQLAlchemy com pymysql.
    :return: Objeto de conexão (SQLAlchemy Connection ou pyodbc.Connection).
    """
    try:
        logging.info("Conectando ao banco de dados MySQL...")
        usar_odbc = True
        if usar_odbc:
            driver = detectar_driver_mysql()
            if not driver:
                raise Exception("Nenhum driver ODBC do MySQL disponível.")

            dsn = f"DRIVER={{{driver}}};SERVER={host};DATABASE={database};USER={user};PASSWORD={password};PORT={port};OPTION=3;"
            conexao = pyodbc.connect(dsn, autocommit=True)
            logging.info(f" Conexão com o banco de dados MySQL via ODBC ({driver}) estabelecida com sucesso.")
            return conexao

        else:
            # Conexão via SQLAlchemy (pymysql)
            if not all([host, database, user, password]):
                raise ValueError("Para conectar via SQLAlchemy, os parâmetros 'host', 'database', 'user' e 'password' são obrigatórios.")

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
    """Fecha a conexão com o banco de dados."""
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
    Executa as consultas no banco de dados de forma paralela ou sequencial.

    - Se paralela for False, uma única conexão é criada e reutilizada.
    - Se paralela for True, cada thread abre e fecha sua própria conexão.

    Retorna:
      - Um dicionário com o caminho final dos arquivos processados para cada consulta.
      - Um dicionário com os conjuntos de partições criadas.
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
    Executa uma consulta SQL e retorna o caminho da pasta com os arquivos particionados e as partições criadas.

    Se a consulta retornar um DataFrame vazio, retorna uma string vazia e um conjunto vazio.
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
    Processa os dados resultantes da consulta:
      - Realiza tratamentos (ajustes de colunas de data/hora, conversão para Polars).
      - Adiciona colunas auxiliares (DataHoraAtualizacao, idEmpresa).
      - Salva os dados em formato Parquet particionado (por idEmpresa e, se aplicável, por Ano/Mes/Dia).

    Retorna o caminho da pasta final e o conjunto de partições criadas.
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
                pl.col(coluna_data).str.slice(5, 2).alias("Mes"),
                pl.col(coluna_data).str.slice(8, 2).alias("Dia")
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
