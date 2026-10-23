# wagering-processor

## SQS local

O Docker Compose inicia o LocalStack, cria a fila FIFO principal, sua DLQ e uma
fila FIFO separada para eventos de integração. A fila de entrada redireciona para
a DLQ após `SQS_MAX_RECEIVE_COUNT` entregas sem sucesso. A aplicação só inicia
depois que o bootstrap das filas termina com sucesso.

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

## Transactional outbox

Cada transação financeira grava seus eventos de integração na tabela `outbox_events`
na mesma transação PostgreSQL da wallet, do transaction record, do ledger e (quando
aplicável) da inbox. O publisher reivindica uma linha por vez com
`FOR UPDATE SKIP LOCKED`, publica na fila FIFO dedicada `wager-integration-events.fifo`
e marca `published_at` somente após o SQS confirmar a publicação. Falhas recebem
backoff exponencial e ficam disponíveis para outro ciclo/instância. Um crash após o
envio SQS e antes do commit pode gerar redelivery; consumidores devem deduplicar por
`eventId`.

Eventos gravados: `WagerTransactionProcessed`, `WagerTransactionRejected`,
`WagerTransactionPendingReference` e `WalletBalanceChanged` somente se houver
lançamento e mudança de saldo. A fila de eventos é distinta da fila de entrada.

## Reprocessamento de referências pendentes

REFUND e ROLLBACK recebidos antes da transação de referência permanecem em
`PENDING_REFERENCE`. O worker consulta pendências vencidas no PostgreSQL, usando
`FOR UPDATE SKIP LOCKED` para coordenar várias instâncias. Ao localizar a
referência processada, ele trava a wallet e confirma a operação, ledger e eventos
de outbox numa única transação. Referências ainda ausentes são reagendadas com
backoff exponencial (1, 2, 4, ... segundos, limitado a 300 segundos), até
10 tentativas no total ou 24 horas desde a entrada em pendência. Ao esgotar a
política, a transação é rejeitada com `REFERENCE_NOT_FOUND` e o evento
`WagerTransactionRejected` é gravado atomicamente. Um índice único parcial e
uma verificação dentro do lock da wallet permitem somente uma reversão total
(REFUND ou ROLLBACK) processada por referência; tentativas concorrentes adicionais
são rejeitadas com `REFERENCE_ALREADY_REVERSED`.

## Reconciliação de saldo

`POST /wallets/:walletId/reconciliation` compara o saldo materializado com o saldo
calculado como saldo de abertura mais créditos menos débitos do ledger. A migration
persiste o saldo de abertura e faz backfill para as wallets existentes. Divergências
são retornadas como `consistent: false`, registradas em log estruturado sem valores
monetários e incrementam o contador Prometheus
`wallet_reconciliation_divergences_total`, exposto em `GET /metrics`. A reconciliação
é somente leitura e nunca corrige o saldo automaticamente. Para wallets anteriores
à migration, o backfill deriva o saldo de abertura do saldo armazenado e dos
lançamentos existentes; não é possível inferir divergências históricas anteriores
à criação desse baseline.

## Observabilidade

`GET /metrics` expõe métricas Prometheus para transações por status, duplicatas,
retries do consumidor SQS/outbox/reprocessador, mensagens visíveis na DLQ, contenção
de lock da wallet, latência de processamento, espera por lock, atraso da outbox e
divergências da reconciliação. Contagem de DLQ é aproximada conforme os atributos
do SQS; atraso da outbox corresponde à idade do evento não publicado mais antigo.
Contadores e histogramas são locais ao processo e devem ser agregados pelo
Prometheus ao consultar múltiplas instâncias.

Logs dos fluxos financeiros e workers são JSON e incluem IDs de correlação e de
mensagem/transação/wallet/provider quando disponíveis. Não registram payloads nem
valores monetários; erros são identificados por código/classe.

```powershell
bunx mikro-orm migration:up
docker compose up -d --build
docker compose exec localstack awslocal sqs list-queues
```
