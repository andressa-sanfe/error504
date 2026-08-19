"""
Cloud Function HTTP para criar/atualizar tabelas externas Iceberg no BigQuery
a partir dos arquivos de metadados JSON mais recentes no GCS.

Correções aplicadas em relação à versão original (ver README, seção 5):
  1. list_blobs() filtrado por prefixo, em vez de varrer o bucket inteiro.
  2. CREATE OR REPLACE EXTERNAL TABLE em vez de get_table + delete_table +
     create_table + sleep(2).
  3. Criação das tabelas em paralelo (ThreadPoolExecutor) em vez de sequencial.
"""

import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

import functions_framework
from google.cloud import bigquery, storage

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Prefixo usado para filtrar a listagem do bucket -- evita varrer o bucket
# inteiro a cada execução (ver README, causa raiz item 1).
BRONZE_PREFIX = "bronze-layer/"

METADATA_PATTERN = re.compile(
    r"^bronze-layer/([^/]+)/([^/]+)/metadata/(.+\.metadata\.json)$"
)
SEQ_PATTERN = re.compile(r"^(\d+)-")

PLATFORM_MAP = {
    "facebook": "facebook_ads",
    "facebookpages": "facebook_pages",
    "googledv360": "google_dv360",
    "tiktok": "tiktok_ads",
}

# Conexão do BigLake usada para ler os arquivos Iceberg no GCS.
# Ajuste os valores padrão para o que já está configurado no projeto,
# ou defina via variável de ambiente no deploy.
ICEBERG_CONNECTION = os.environ.get("ICEBERG_CONNECTION", "iceberg_connection")
ICEBERG_CONNECTION_LOCATION = os.environ.get("ICEBERG_CONNECTION_LOCATION", "us-central1")

# Quantas tabelas recriar em paralelo. Ajuste conforme as cotas de API do projeto.
MAX_WORKERS = int(os.environ.get("MAX_WORKERS", "8"))


def find_latest_metadata_files(bucket):
    """Varre apenas o prefixo bronze-layer/ e mantém, por (plataforma, tabela),
    o arquivo de metadata com o maior número de sequência (o mais recente)."""
    latest_metadata_files = {}

    for blob in bucket.list_blobs(prefix=BRONZE_PREFIX):
        match = METADATA_PATTERN.match(blob.name)
        if not match:
            continue

        platform, table_name, metadata_file = match.groups()
        seq_match = SEQ_PATTERN.match(metadata_file)
        if not seq_match:
            continue

        seq_num = int(seq_match.group(1))
        key = (platform, table_name)
        if key not in latest_metadata_files or seq_num > latest_metadata_files[key][0]:
            latest_metadata_files[key] = (seq_num, blob.name)

    return latest_metadata_files


def build_table_targets(latest_metadata_files, project_id, dataset_id):
    """Converte o dicionário de metadata encontrados numa lista de tabelas a
    criar/recriar, já resolvendo o nome final no BigQuery e removendo
    duplicatas (mais de uma origem podendo mapear pro mesmo nome final)."""
    targets = []
    seen = set()

    for (platform, table_name), (_, metadata_path) in latest_metadata_files.items():
        platform_key = platform.split("_")[0]
        platform_mapped = PLATFORM_MAP.get(platform_key, platform)
        bq_table_id = f"stg_{platform_mapped}_{table_name}"

        if bq_table_id in seen:
            continue
        seen.add(bq_table_id)

        targets.append(
            {
                "full_table_id": f"{project_id}.{dataset_id}.{bq_table_id}",
                "metadata_path": metadata_path,
            }
        )

    return targets


def create_or_replace_iceberg_table(bq_client, project_id, bucket_name, target):
    """Cria (ou substitui, atomicamente) a tabela externa Iceberg com um único
    CREATE OR REPLACE EXTERNAL TABLE (ver README, causa raiz item 2). Isso
    substitui o padrão antigo de get_table + delete_table + create_table +
    sleep(2), que multiplicava chamadas de API e adicionava atraso artificial
    por tabela."""
    full_table_id = target["full_table_id"]
    uri = f"gs://{bucket_name}/{target['metadata_path']}"

    query = f"""
        CREATE OR REPLACE EXTERNAL TABLE `{full_table_id}`
        WITH CONNECTION `{project_id}.{ICEBERG_CONNECTION_LOCATION}.{ICEBERG_CONNECTION}`
        OPTIONS (
          format = 'ICEBERG',
          uris = ['{uri}']
        )
    """
    bq_client.query(query).result()
    return f"Tabela {full_table_id} criada/atualizada a partir de {uri}."


@functions_framework.http
def create_iceberg_external_tables(request):
    project_id = os.environ["PROJECT_ID"]
    dataset_id = os.environ["DATASET_ID"]
    bucket_name = os.environ["BUCKET_NAME"]

    bq_client = bigquery.Client(project=project_id)
    storage_client = storage.Client(project=project_id)

    try:
        bucket = storage_client.get_bucket(bucket_name)
    except Exception as e:
        logger.exception("Erro ao acessar bucket %s", bucket_name)
        return (f"Erro ao acessar bucket: {str(e)}", 500)

    latest_metadata_files = find_latest_metadata_files(bucket)
    targets = build_table_targets(latest_metadata_files, project_id, dataset_id)

    logs = []
    errors = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_target = {
            executor.submit(
                create_or_replace_iceberg_table, bq_client, project_id, bucket_name, t
            ): t
            for t in targets
        }
        for future in as_completed(future_to_target):
            target = future_to_target[future]
            try:
                logs.append(future.result())
            except Exception as e:
                msg = f"Erro ao criar tabela {target['full_table_id']}: {str(e)}"
                logger.exception(msg)
                errors.append(msg)

    status = "concluído" if not errors else "concluído_com_erros"
    return {
        "status": status,
        "tabelas_processadas": len(targets),
        "logs": logs,
        "erros": errors,
    }
