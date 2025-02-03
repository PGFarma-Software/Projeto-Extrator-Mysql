import asyncio
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

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
            logging.info(f"{id_empresa} NÃO será excluída porque ainda há partições válidas")
        else:
            logging.info(f"{id_empresa} será excluída, pois todas suas partições foram removidas.")
            particoes_para_excluir["idEmpresa"].add(id_empresa)


    return particoes_para_excluir




def limpar_prefixo_no_azure(blob_service_client, container_name, caminho_destino, particoes_recarregadas, workers=5, nome_consulta=""):
    """
    Remove apenas as partições que precisam ser recarregadas no Azure Blob Storage,
    garantindo que apenas arquivos sejam apagados antes da remoção dos diretórios vazios.

    Args:
        blob_service_client: Cliente do Azure Blob Storage.
        container_name (str): Nome do container no Azure.
        caminho_destino (str): Caminho base no Azure onde os dados estão armazenados.
        particoes_recarregadas (list): Lista de partições que foram atualizadas e precisam ser removidas antes da substituição.
        nome_consulta (str): Nome da consulta (usado para diferenciar a regra de limpeza).
        workers (int): Número de threads paralelas para exclusão.
    """
    try:
        if not particoes_recarregadas:
            logging.info(f"[{nome_consulta}] Nenhuma partição relevante encontrada para exclusão no Azure.")
            return

        # 🔹 Obter todas as partições existentes antes da exclusão
        container_client = blob_service_client.get_container_client(container_name)
        blobs_existentes = list(container_client.list_blobs(name_starts_with=caminho_destino))
        particoes_existentes = {"/".join(blob.name.split("/")[:-1]) for blob in blobs_existentes}

        # 🔹 Definir as partições que devem ser removidas (Ano, Mês, Dia, idEmpresa)
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

        logging.info(f"[{nome_consulta}] Partições a serem removidas no Azure:")
        for chave, particoes in particoes_para_excluir.items():
            if particoes:
                logging.info(f"- {chave}: {list(particoes)[:1]} ... {list(particoes)[-1:]} ({len(particoes)} partições)")

        blobs_para_excluir = []

        # 🔹 Identificar os arquivos a serem excluídos
        for particao in (particoes_para_excluir["Dia"] | particoes_para_excluir["Mes"] | particoes_para_excluir["Ano"] | particoes_para_excluir["idEmpresa"]):
            prefixo_completo = f"{caminho_destino}/{particao}".rstrip("/") + "/"
            blobs = list(container_client.list_blobs(name_starts_with=prefixo_completo))
            blobs_para_excluir.extend([blob.name for blob in blobs if not blob.name.endswith("/")])

        if not blobs_para_excluir:
            logging.info(f"[{nome_consulta}] Nenhum arquivo encontrado para exclusão.")
            return

        logging.info(f"[{nome_consulta}] Excluindo {len(blobs_para_excluir)} arquivos antes de remover diretórios...")

        # 🔹 Função para excluir um único blob
        def excluir_blob(blob_name):
            try:
                container_client.delete_blob(blob_name)
                return blob_name, None  # Sucesso
            except Exception as e:
                return blob_name, str(e)  # Erro

        erros = []
        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_blob = {executor.submit(excluir_blob, blob): blob for blob in blobs_para_excluir}

            for future in as_completed(future_to_blob):
                blob_name, erro = future.result()
                if erro:
                    erros.append(blob_name)
                    logging.error(f"Erro ao excluir blob '{blob_name}': {erro}")

        logging.info(f"[{nome_consulta}] Exclusão de arquivos concluída. Removidos: {len(blobs_para_excluir) - len(erros)}, Erros: {len(erros)}")

        # 🔹 Remover diretórios vazios após a exclusão dos arquivos
        logging.info(f"[{nome_consulta}] Verificando e removendo diretórios vazios...")

    except Exception as e:
        logging.error(f"[{nome_consulta}] Erro ao limpar partições no Azure: {e}")
        raise


def realizar_upload_azure(temp_dir, caminho_destino, azure_config, workers=10, nome_consulta=""):
    """
    Realiza o upload de arquivos para o Azure Blob Storage de forma paralela.

    Args:
        temp_dir (str): Diretório temporário contendo os arquivos a serem enviados.
        caminho_destino (str): Caminho no Azure Blob Storage.
        azure_config (dict): Configurações do Azure.
        workers (int): Número máximo de threads para paralelismo.
        nome_consulta (str): Nome da consulta (para logs mais claros).
    """
    # 🔹 Reduzir logs desnecessários de HTTP
    logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    # 🔹 Identificar partições a serem limpas antes do upload
    particoes = [
        os.path.relpath(root, temp_dir).replace(os.sep, "/")
        for root, _, _ in os.walk(temp_dir) if "idEmpresa=" in root
    ]


    # 🔹 Executa limpeza seletiva antes do upload
    limpar_prefixo_no_azure(
        azure_config["blob_service_client"], azure_config["container_name"], caminho_destino, particoes, workers, nome_consulta
    )

    # 🔹 Obter lista de arquivos a serem enviados
    arquivos = [os.path.join(root, file) for root, _, files in os.walk(temp_dir) for file in files]

    if not arquivos:
        logging.info(f"[{nome_consulta}] Nenhum arquivo encontrado para upload em '{temp_dir}'.")
        return {"enviados": [], "erros": []}

    enviados, erros = [], []

    def upload_arquivo_azure(blob_service_client, container_name, local_path, destino_path):
        """Realiza o upload de um único arquivo para o Azure Blob Storage."""
        try:
            with open(local_path, "rb") as data:
                blob_client = blob_service_client.get_blob_client(container_name, destino_path)
                blob_client.upload_blob(data, overwrite=True)
            return destino_path, None  # Sucesso
        except Exception as e:
            return destino_path, str(e)  # Erro

    # 🔹 Iniciar upload assíncrono usando threads
    logging.info(f"[{nome_consulta}] Iniciando upload para Azure ({len(arquivos)} arquivos) usando {workers} threads...")

    blob_service_client = azure_config["blob_service_client"]
    container_name = azure_config["container_name"]

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(
                upload_arquivo_azure, blob_service_client, container_name, arquivo,
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

    logging.info(f"[{nome_consulta}] Upload para Azure concluído. Total enviados: {total_enviados}, Total com erro: {total_erros}")

    if total_erros > 0:
        logging.warning(f"[{nome_consulta}] Arquivos que falharam no upload: {erros}")

    return {"enviados": enviados, "erros": erros}
