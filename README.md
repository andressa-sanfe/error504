# Incidente: erro 504 na atualização do dashboard


O 504 não está na conexão Power BI ↔ BigQuery está num pipeline de ingestão que recria tabelas externas Iceberg e vem estourando o prazo do Cloud Scheduler.
---


## 1. Trilha de investigação

### 1.1 Cloud Logging

Usando a investigação automática do Logs Explorer (Gemini Cloud Assist) num log de severidade `ERROR`, foi extraído o seguinte registro (arquivo completo em [`evidence/cloud-scheduler-log.json`](evidence/cloud-scheduler-log.json)):

```json
{
  "insertId": "m81m4uf6eghu1",
  "jsonPayload": {
    "@type": "type.googleapis.com/google.cloud.scheduler.logging.AttemptFinished",
    "jobName": "projects/babel-azza-analytics-medallion/locations/us-central1/jobs/create-iceberg-external-tables-scheduler",
    "targetType": "HTTP",
    "url": "https://create-iceberg-external-tables-444281546538.us-central1.run.app/",
    "status": "DEADLINE_EXCEEDED",
    "debugInfo": "URL_TIMEOUT-TIMEOUT_WEB. Original HTTP response code number = 504"
  },
  "httpRequest": { "status": 504 },
  "resource": {
    "type": "cloud_scheduler_job",
    "labels": {
      "project_id": "babel-azza-analytics-medallion",
      "job_id": "create-iceberg-external-tables-scheduler",
      "location": "us-central1"
    }
  },
  "timestamp": "2026-08-12T17:37:14.392130917Z",
  "severity": "ERROR"
}
```

**Leitura do log:** o **Cloud Scheduler** chamou um endpoint HTTP hospedado no **Cloud Run** (`create-iceberg-external-tables`) e não recebeu resposta dentro do prazo (`attempt_deadline`). O Scheduler cancelou a tentativa e registrou `DEADLINE_EXCEEDED`, com o código HTTP original 504, ou seja, quem "desistiu de esperar" foi o Scheduler, não o BigQuery nem o Power BI.

## 2. Causa raiz

### 2.1 O que a função faz

O código-fonte da função chamada pelo Scheduler está em [`evidence/create_iceberg_external_tables_before.py`](evidence/create_iceberg_external_tables_before.py).

1. Varre um bucket do Cloud Storage procurando os arquivos de metadata mais recentes de cada tabela Iceberg (Facebook Ads, Facebook Pages, Google DV360, TikTok Ads) na camada *bronze* do data lake.
2. Para cada uma, recria a tabela externa correspondente no BigQuery (`stg_facebook_ads`, `stg_google_dv360` etc.) apontando para o metadata mais novo.

### 2.2 Por que ela estoura o tempo

| # | Problema no código | Efeito |
|---|---|---|
| 1 | `bucket.list_blobs()` sem prefixo | Varre o bucket **inteiro** a cada execução. Como o Iceberg gera um `.metadata.json` novo a cada snapshot, esse histórico só cresce — a listagem fica mais lenta com o tempo. |
| 2 | `get_table` + `delete_table` + `create_table` + `time.sleep(2)` por tabela | 3–4 chamadas de API sequenciais por tabela, mais 2s de espera artificial. Com N tabelas, isso soma pelo menos `N × 2s` só de sleep. |
| 3 | Processamento sequencial, sem paralelismo, reprocessando tudo do zero | Nada roda em paralelo, e a função não distingue "o que já está atualizado" de "o que é novo". |

O prazo padrão do Cloud Scheduler para um alvo HTTP é de 3 minutos, configurável até um teto de 30 minutos — se o serviço não responde dentro desse prazo, a tentativa é cancelada e marcada como `DEADLINE_EXCEEDED`. Conforme o bucket e o número de tabelas crescem, era questão de tempo até essa função ultrapassar esse limite, o que explica por que o erro **começou a aparecer recentemente**.

## 3. Conexão com o erro no dashboard

Esta função recria as tabelas de staging da camada bronze. Se o relatório do Power BI lê, direta ou indiretamente (via view ou tabela derivada), uma dessas tabelas `stg_*`, os dois sintomas — o timeout no Scheduler e o 504 no refresh do Power BI — são explicados pela mesma causa raiz (crescimento de volume de dados), aparecendo em pontos diferentes do mesmo pipeline.

**Pendência a confirmar:** verificar se a(s) tabela(s) que o dashboard consulta é uma dessas staging tables (ou algo construído a partir delas). Isso fecha definitivamente a cadeia causal.

## 4. Solução proposta

Código corrigido em [`src/create_iceberg_external_tables.py`](src/create_iceberg_external_tables.py).

| Problema | Antes | Depois |
|---|---|---|
| Listagem do bucket | `list_blobs()` sem filtro | `list_blobs(prefix="bronze-layer/")` |
| Criar/recriar tabela | `get_table` + `delete_table` + `create_table` + `sleep(2)` | `CREATE OR REPLACE EXTERNAL TABLE` (uma única chamada atômica) |
| Execução | Sequencial, tabela por tabela | Paralela, via `ThreadPoolExecutor` |

Como paliativo imediato (não resolve a causa, só ganha tempo enquanto a correção não é implantada): aumentar o `attempt-deadline` do Cloud Scheduler e o timeout do Cloud Run. Como a função tende a continuar ficando mais lenta com o crescimento dos dados, isso sozinho não é solução definitiva.

## 5. Como aplicar

```bash
# 1. Deploy da função corrigida
gcloud functions deploy create-iceberg-external-tables \
  --gen2 \
  --runtime=python312 \
  --region=us-central1 \
  --source=./src \
  --entry-point=create_iceberg_external_tables \
  --trigger-http \
  --no-allow-unauthenticated \
  --set-env-vars=PROJECT_ID=SEU_PROJECT_ID,DATASET_ID=SEU_DATASET_ID,BUCKET_NAME=SEU_BUCKET,ICEBERG_CONNECTION=SUA_CONEXAO

# 2. Paliativo: aumentar o attempt-deadline do Scheduler enquanto o deploy não sai (máximo permitido: 30 min)
gcloud scheduler jobs update http create-iceberg-external-tables-scheduler \
  --location=us-central1 \
  --attempt-deadline=1800s
```

## 6. Recomendações para evitar recorrência

- **Alertar em vez de descobrir tarde:** criar um alerta no Cloud Monitoring para execuções `DEADLINE_EXCEEDED` do job `create-iceberg-external-tables-scheduler`.
- **Processamento incremental:** hoje a função reprocessa o bucket inteiro a cada execução. Migrar para um gatilho orientado a evento (Eventarc no upload de um novo `.metadata.json`) elimina o crescimento do tempo de execução ao longo do tempo.
- **Monitorar o tamanho do bucket/número de objetos** na camada bronze — é o indicador que antecipa esse tipo de estouro de timeout.
- **Confirmar a linhagem** dashboard → tabela BigQuery → tabela `stg_*` para fechar em definitivo a cadeia causal descrita na seção 4.

## Estrutura deste repositório

```
.
├── README.md
├── evidence/
│   ├── cloud-scheduler-log.json           # log bruto do incidente
│   └── create_iceberg_external_tables_before.py   # código original (com o bug)
└── src/
    ├── create_iceberg_external_tables.py  # código corrigido
    └── requirements.txt
```
