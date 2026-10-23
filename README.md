# wagering-processor

## SQS local

O Docker Compose inicia o LocalStack, cria a DLQ FIFO e a fila FIFO principal com
redrive para a DLQ após `SQS_MAX_RECEIVE_COUNT` entregas sem sucesso. A aplicação
só inicia depois que o bootstrap das filas termina com sucesso.

```powershell
docker compose up -d postgres localstack sqs-init
docker compose logs sqs-init
docker compose exec localstack awslocal sqs list-queues
```

Os nomes das filas e o limite de recebimentos podem ser configurados em `.env`;
os valores padrão estão em `.env.example`. Para executar também a API:

```powershell
docker compose up --build
```

## Inbox persistente

A migration cria `message_inbox`, com chave única por `(consumer_name, message_id)`.
O repositório recebe um `PoolClient` transacional e usa `INSERT ... ON CONFLICT`,
para que a inserção da inbox possa fazer parte da mesma transação do processamento
financeiro. O consumidor recebe mensagens FIFO, valida o envelope e reutiliza o caso
de uso financeiro. A inbox, a transação, o ledger e a atualização da wallet usam o
mesmo `PoolClient`; a mensagem só é removida da fila depois que o commit termina.
Falhas técnicas e mensagens inválidas permanecem sem ACK, recebem backoff exponencial
e são encaminhadas à DLQ pela política de redrive. Falhas de negócio 4xx são
registradas na inbox e confirmadas antes do ACK. O shutdown aguarda a mensagem em
processamento e encerra o long poll.

```powershell
bunx mikro-orm migration:up
bun test src/infrastructure/messaging/inbox.repository.spec.ts
```

Ao executar a API fora do Compose, configure `LOCALSTACK_HOST=localhost` e
`LOCALSTACK_PORT=4566`. No Compose, o endpoint SQS aponta para `localstack:4566`.
