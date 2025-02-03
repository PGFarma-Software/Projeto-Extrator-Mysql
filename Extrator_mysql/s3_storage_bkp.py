import asyncio
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List

from config import STORAGE_CONFIG

def formatar_particoes_log(particoes, nivel):
    """
    Formata o log de partições para exibição compacta, mostrando apenas a primeira e a última partição.

    Args:
        particoes (set): Conjunto de partições a serem exibidas.
        nivel (str): Nível da partição (Ano, Mês, Dia).

    Returns:
        str: String formatada com as informações das partições.
    """
    if not particoes:
        return ""

    particoes_ordenadas = sorted(particoes)
    primeira = particoes_ordenadas[0]
    ultima = particoes_ordenadas[-1]
    total = len(particoes_ordenadas)

    if total == 1:
        return f"{nivel}: {primeira} ({total} partição)"
    return f"{nivel}: {primeira} ... {ultima} ({total} partições)"


def definir_particoes_para_exclusao(particoes_existentes, particoes_recarregadas):
    """
    Define as partições (Ano, Mês, Dia, idEmpresa) que devem ser removidas do Azure ou S3,
    garantindo que apenas partições realmente afetadas sejam excluídas.

    Args:
        particoes_existentes (set): Conjunto de partições já existentes no armazenamento.
        particoes_recarregadas (set): Conjunto de partições que estão sendo recarregadas.

    Returns:
        dict: Dicionário contendo as partições a serem removidas, categorizadas em "Ano", "Mes", "Dia" e "idEmpresa".
    """
    particoes_para_excluir = {"Ano": set(), "Mes": set(), "Dia": set(), "idEmpresa": set()}

    # 🔹 Identificar dias que devem ser removidos
    for particao in particoes_recarregadas:
        if "Ano=" in particao and "Mes=" in particao and "Dia=" in particao:
            particoes_para_excluir["Dia"].add(particao)

    # 🔹 Identificar meses que podem ser removidos se TODOS os dias forem apagados
    meses_impactados = {p.rsplit("/", 1)[0] for p in particoes_para_excluir["Dia"]}
    for mes in meses_impactados:
        dias_existentes_no_mes = {p for p in particoes_existentes if p.startswith(mes)}
        if dias_existentes_no_mes == particoes_para_excluir["Dia"]:  # Se todos os dias forem excluídos
            particoes_para_excluir["Mes"].add(mes)

    # 🔹 Identificar anos que podem ser removidos se TODOS os meses forem apagados
    anos_impactados = {p.rsplit("/", 1)[0] for p in particoes_para_excluir["Mes"]}
    for ano in anos_impactados:
        meses_existentes_no_ano = {p for p in particoes_existentes if p.startswith(ano)}
        if meses_existentes_no_ano == particoes_para_excluir["Mes"]:  # Se todos os meses forem excluídos
            particoes_para_excluir["Ano"].add(ano)

    # 🔹 Identificar partições `idEmpresa` que devem ser removidas
    id_empresas_impactadas = {p.split("/")[0] for p in particoes_recarregadas if "idEmpresa=" in p}

    for id_empresa in id_empresas_impactadas:
        # 🔹 Capturar todas as partições existentes dessa empresa
        todas_particoes_empresa = {
            p for p in particoes_existentes if f"/{id_empresa}/" in p or p.startswith(f"/{id_empresa}")
        }
        # 🔹 Remover todas as partições que já estão marcadas para exclusão
        particoes_excluidas = (
            particoes_para_excluir["Dia"] |
            particoes_para_excluir["Mes"] |
            particoes_para_excluir["Ano"]
        )

        # 🔹 Subtrair as partições que já estão na lista de exclusão
        outras_particoes_dessa_empresa = todas_particoes_empresa - particoes_excluidas


        # 🔹 Se ainda existem partições restantes, NÃO excluir a empresa
        if outras_particoes_dessa_empresa:
            logging.info(f" {id_empresa} NÃO será excluída porque ainda há partições válidas")
        else:
            logging.info(f" {id_empresa} será excluída, pois todas suas partições foram removidas.")
            particoes_para_excluir["idEmpresa"].add(id_empresa)


    return particoes_para_excluir

def limpar_prefixo_no_s3(s3_client, bucket_name, caminho_destino, particoes_recarregadas, workers=5, nome_consulta=""):
    """
    Remove apenas as partições que precisam ser recarregadas no S3, garantindo a estrutura correta de Ano/Mês/Dia.

    Args:
        s3_client: Cliente do Amazon S3.
        bucket_name (str): Nome do bucket no S3.
        caminho_destino (str): Caminho base no S3 onde os dados estão armazenados.
        particoes_recarregadas (list): Lista de partições que foram atualizadas e precisam ser removidas antes da substituição.
        nome_consulta (str): Nome da consulta (usado para diferenciar a regra de limpeza).
        workers (int): Número de threads paralelas para exclusão.
    """
    try:
        if not particoes_recarregadas:
            logging.info(f"[{nome_consulta}] Nenhuma partição relevante encontrada para exclusão no S3.")
            return

        # 🔹 Obter todas as partições existentes no S3 antes da exclusão
        objetos_existentes = []
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=caminho_destino)

        while response.get("Contents"):
            objetos_existentes.extend(response["Contents"])
            if response.get("IsTruncated"):
                response = s3_client.list_objects_v2(
                    Bucket=bucket_name, Prefix=caminho_destino, ContinuationToken=response["NextContinuationToken"]
                )
            else:
                break

        particoes_existentes = {"/".join(obj["Key"].split("/")[:-1]) for obj in objetos_existentes}

        # 🔹 Definir as partições que devem ser removidas (Ano, Mês, Dia)
        particoes_para_excluir = definir_particoes_para_exclusao(particoes_existentes, set(particoes_recarregadas))

        # 🔹 Consolidar logs para evitar poluição visual
        log_particoes = [
            formatar_particoes_log(particoes_para_excluir["Ano"], "Anos"),
            formatar_particoes_log(particoes_para_excluir["Mes"], "Meses"),
            formatar_particoes_log(particoes_para_excluir["Dia"], "Dias"),
            formatar_particoes_log(particoes_para_excluir["idEmpresa"], "Empresas")
        ]

        log_particoes = [log for log in log_particoes if log]  # Remove entradas vazias

        if log_particoes:
            logging.info(f"[{nome_consulta}] Partições a serem removidas no S3:")
            for log in log_particoes:
                logging.info(f"  - {log}")

        objetos_para_excluir = []

        # 🔹 Identificar os arquivos a serem excluídos
        for particao in (particoes_para_excluir["Dia"] | particoes_para_excluir["Mes"]
                         | particoes_para_excluir["Ano"]| particoes_para_excluir["idEmpresa"]):
            prefixo_completo = f"{caminho_destino}/{particao}".rstrip("/") + "/"
            response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefixo_completo)

            if "Contents" in response:
                arquivos_para_excluir = [obj["Key"] for obj in response["Contents"] if not obj["Key"].endswith("/")]
                objetos_para_excluir.extend(arquivos_para_excluir)

        if not objetos_para_excluir:
            logging.info(f"[{nome_consulta}] Nenhum arquivo encontrado para exclusão.")
            return

        logging.info(f"[{nome_consulta}] Removendo {len(objetos_para_excluir)} arquivos no S3 de forma paralela...")

        # 🔹 Função para excluir um único objeto no S3
        def excluir_objeto(obj_key):
            try:
                s3_client.delete_object(Bucket=bucket_name, Key=obj_key)
                return obj_key, None  # Sucesso
            except Exception as e:
                return obj_key, str(e)  # Erro

        erros = []
        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_obj = {executor.submit(excluir_objeto, obj): obj for obj in objetos_para_excluir}

            for future in as_completed(future_to_obj):
                obj_key, erro = future.result()
                if erro:
                    erros.append(obj_key)
                    logging.error(f"Erro ao excluir objeto '{obj_key}': {erro}")

        logging.info(f"[{nome_consulta}] Limpeza no S3 concluída. Arquivos removidos: {len(objetos_para_excluir) - len(erros)}, Erros: {len(erros)}")

    except Exception as e:
        logging.error(f"[{nome_consulta}] Erro ao limpar partições no S3: {e}")
        raise


def realizar_upload_s3(temp_dir: str, caminho_destino: str, s3_config: Dict, workers: int = 5, nome_consulta: str = "") -> Dict[str, List[str]]:
    """
    Realiza o upload de arquivos para o S3 de forma paralela.

    Args:
        temp_dir (str): Diretório temporário contendo os arquivos.
        caminho_destino (str): Caminho no S3.
        s3_config (dict): Configurações do S3 (s3_client, bucket).
        workers (int): Número máximo de threads para paralelismo.
        nome_consulta (str): Nome da consulta (usado para logs mais claros).

    Returns:
        Dict[str, List[str]]: Dicionário com listas de arquivos enviados e arquivos com erro.
    """
    # 🔹 Identificar partições que serão limpas antes do upload
    particoes = [
        os.path.relpath(root, temp_dir).replace(os.sep, "/")
        for root, _, _ in os.walk(temp_dir) if "idEmpresa=" in root
    ]

    # 🔹 Definição da função de upload individual
    def upload_arquivo_s3(s3_client, bucket, local_path, destino_path):
        """Realiza o upload de um único arquivo para o S3."""
        try:
            with open(local_path, "rb") as data:
                s3_client.upload_fileobj(data, bucket, destino_path)
            return destino_path, None  # Sucesso
        except Exception as e:
            return destino_path, str(e)  # Erro

    try:
        logging.info(f"[{nome_consulta}] Iniciando upload para o S3 ({len(particoes)} partições) usando {workers} threads...")

        s3_client = s3_config["s3_client"]
        bucket = s3_config["bucket"]

        # 🔹 Obter lista de arquivos a serem enviados
        arquivos = [os.path.join(root, file) for root, _, files in os.walk(temp_dir) for file in files]

        if not arquivos:
            logging.info(f"[{nome_consulta}] Nenhum arquivo encontrado para upload em '{temp_dir}'.")
            return {"enviados": [], "erros": []}

        # 🔹 Limpeza seletiva no S3 apenas das partições relevantes
        limpar_prefixo_no_s3(s3_client, bucket, caminho_destino, particoes, workers, nome_consulta)

        enviados, erros = [], []

        # 🔹 Execução do upload em paralelo usando ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(
                    upload_arquivo_s3, s3_client, bucket, arquivo,
                    f"{caminho_destino}/{os.path.relpath(arquivo, temp_dir).replace(os.sep, '/')}"
                ): arquivo for arquivo in arquivos
            }

            for future in as_completed(futures):
                destino_path, erro = future.result()
                if erro:
                    erros.append(destino_path)
                    logging.error(f"[{nome_consulta}] Erro no upload de '{destino_path}': {erro}")
                else:
                    enviados.append(destino_path)

        # 🔹 Log final consolidado
        total_enviados = len(enviados)
        total_erros = len(erros)

        logging.info(f"[{nome_consulta}] Upload para S3 concluído. Total enviados: {total_enviados}, Total com erro: {total_erros}")

        if total_erros > 0:
            logging.warning(f"[{nome_consulta}] Arquivos que falharam no upload: {erros}")

        return {"enviados": enviados, "erros": erros}

    except Exception as e:
        logging.error(f"[{nome_consulta}] Erro no upload para o S3: {e}")
        return {"enviados": [], "erros": []}
