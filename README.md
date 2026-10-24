# wagering-processor

Decisões, arquitetura e limitações conhecidas estão documentadas em
[ARCHITECTURE.md](./ARCHITECTURE.md).

## SQS local

O Docker Compose inicia o LocalStack, cria a fila FIFO principal, sua DLQ e uma
fila FIFO separada para eventos de integração. A fila de entrada redireciona para
a DLQ após `SQS_MAX_RECEIVE_COUNT` entregas sem sucesso. Antes da API, o Compose
aplica migrations com o usuário administrativo e provisiona o usuário PostgreSQL
restrito; a API só inicia depois desses passos e do bootstrap das filas.

```powershell
docker compose up -d postgres localstack sqs-init
docker compose logs sqs-init
docker compose exec localstack awslocal sqs list-queues
```

Os nomes das filas e o limite de recebimentos podem ser configurados em `.env`;
os valores padrão estão em `.env.example`. `SQS_WAIT_TIME_SECONDS` controla o
long polling do consumidor (0 a 20 segundos, padrão 20). Para executar a entrega
completa — PostgreSQL, migrations, usuário restrito, LocalStack, filas, API e workers:

```powershell
Copy-Item .env.example .env
docker compose up --build
```

O `.env.example` contém somente credenciais de desenvolvimento. O `.env` local é
ignorado pelo Git; use senhas próprias e não reutilize os valores de exemplo fora
do ambiente local.

Para executar a suíte padrão em container, sem instalar Bun no host:

```powershell
docker compose run --rm test
```

Com Bun 1.x instalado no host, as verificações equivalentes são:

```powershell
bun install --frozen-lockfile
bun run lint
bun run build
bun run test
```

O serviço `test` tem profile próprio e não inicia com `docker compose up`. Ele
prepara PostgreSQL, migrations, usuário de runtime e LocalStack como dependências.
Os testes opt-in com filas reais podem ser executados no mesmo container:

```powershell
docker compose run --rm -e RUN_LOCALSTACK_INTEGRATION=1 test bun test src/infrastructure/messaging/sqs-consumer.localstack.integration.spec.ts src/infrastructure/outbox/outbox-publisher.localstack.integration.spec.ts
docker compose run --rm -e RUN_LOCALSTACK_INTEGRATION=1 test bun test src/infrastructure/messaging/worker-crash-recovery.localstack.integration.spec.ts
```

O serviço `migrate` usa `POSTGRES_USER`/`POSTGRES_PASSWORD` e não sincroniza
automaticamente o schema. Para reaplicar migrations explicitamente sem iniciar
a API:

```powershell
docker compose run --rm migrate
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
bun test src/infrastructure/messaging/wagering-inbox.integration.spec.ts -t "suppresses 50 concurrent redeliveries"
```

Os testes de integração SQS criam filas FIFO temporárias no LocalStack. Cobrem
três consumidores independentes, retries, ACK após commit, redrive à DLQ,
publishers concorrentes e retomada do publisher após falha entre envio ao SQS e
marcação no PostgreSQL. Com PostgreSQL e LocalStack ativos:

```powershell
$env:RUN_LOCALSTACK_INTEGRATION = "1"
bun test src/infrastructure/messaging/sqs-consumer.localstack.integration.spec.ts src/infrastructure/outbox/outbox-publisher.localstack.integration.spec.ts
bun test src/infrastructure/messaging/worker-crash-recovery.localstack.integration.spec.ts
```

O teste de recuperação inicia processos worker Bun separados e os encerra à
força em dois pontos controlados: depois do commit financeiro, antes de o ACK
chegar ao SQS; e depois de o SQS aceitar um evento da outbox, antes de a resposta
chegar ao publisher e de `published_at` ser confirmado. Em seguida, reinicia os
workers e valida redelivery, deduplicação da inbox e recuperação da outbox/FIFO.

Ao executar a API fora do Compose, configure `LOCALSTACK_HOST=localhost` e
`LOCALSTACK_PORT=4566`. No Compose, o endpoint SQS aponta para `localstack:4566`.

## Credenciais PostgreSQL

O Compose mantém `POSTGRES_USER`/`POSTGRES_PASSWORD` para administração e
migrations. Após concluir as migrations, o serviço `postgres-app-user-init`
cria ou atualiza `POSTGRES_APP_USER`/`POSTGRES_APP_PASSWORD`, concede permissões
de runtime nas tabelas existentes e configura privilégios padrão para as próximas migrations. A API,
workers e MikroORM em runtime recebem somente as credenciais `POSTGRES_APP_*`;
as credenciais administrativas não são passadas ao container da aplicação.

O usuário da aplicação recebe somente `SELECT`, `INSERT` e `UPDATE`; não é
proprietário das tabelas, não pode criar objetos ou desabilitar triggers do
ledger, e não recebe `DELETE` nem `TRUNCATE`. Use uma senha forte e
exclusiva para `POSTGRES_APP_PASSWORD` fora do ambiente local. O comando de
migration executado no host continua usando `POSTGRES_USER`/`POSTGRES_PASSWORD`.

## Autenticação HTTP

Não implementei autenticação nesta versão. O tempo disponível e as prioridades do
desafio me levaram a concentrar o trabalho nas regras financeiras, concorrência,
idempotência e processamento de mensagens. Assim, os endpoints HTTP não validam
credenciais nem controlam o acesso por usuário; em produção, eu não exporia o
serviço sem uma camada de proteção.

Já utilizei Keycloak e tenho mais familiaridade com ele. Se fosse implementar
autenticação neste projeto, seguiria esse caminho usando OAuth 2.0/OpenID Connect:
configuraria um realm e clientes no Keycloak, e a API validaria access tokens
Bearer, incluindo assinatura via JWKS, `issuer`, `audience`, expiração e claims
de roles/scopes. No NestJS, adicionaria guards para autorizar as operações.
Ainda não implementei esse ponto de extensão no código; seria parte do próximo
passo. Também manteria as credenciais dos clientes fora do repositório e usaria
credenciais próprias de serviço para SQS e workers. Os health checks poderiam
continuar públicos ou ficar restritos à rede interna, conforme o ambiente.

## Idempotência HTTP

`POST /wagering/transactions` exige o header `Idempotency-Key` (até 128
caracteres). A chave não faz parte do JSON da transação; o corpo contém somente
os dados da operação. O PostgreSQL persiste o hash do payload canônico e a
resposta confirmada na mesma transação dos efeitos financeiros.

Repetir a chave com o mesmo payload retorna os campos da resposta original e
`idempotentReplay: true`, sem criar outra transação ou lançamento. Reutilizar a
chave com payload diferente retorna HTTP `409 Conflict`. Para aplicar a coluna
que armazena o corpo da resposta:

```powershell
bunx mikro-orm migration:up
```

Mensagens SQS continuam carregando `idempotencyKey` no próprio envelope.
O endpoint responde `201 Created` para transações processadas, `202 Accepted`
para estados pendentes, `422 Unprocessable Entity` para rejeições financeiras,
`200 OK` para replay idempotente, `409 Conflict` para colisões de chave/ID,
`400 Bad Request` para payload inválido e `503 Service Unavailable` para falhas
transitórias reconhecidas de infraestrutura. Rejeições persistidas incluem o
`failureCode` no corpo da resposta; valores monetários são serializados como
`{ "amount": "975.00", "currency": "BRL" }`.

## Consulta paginada do ledger

`GET /wallets/:walletId/ledger` aceita `limit` (1 a 100, padrão 50) e `cursor`
opaco opcional. A resposta inclui `entries`, `limit`, `hasMore` e `nextCursor`;
use o cursor retornado como query parameter na próxima solicitação. A paginação
usa a ordenação decrescente por `created_at` e `id`, com comparação dos dois
campos, para não pular entradas que compartilhem o mesmo timestamp. Cursor mal
formado e limites inválidos retornam HTTP `400`.

## Imutabilidade do ledger

Uma migration instala triggers PostgreSQL que rejeitam `UPDATE`, `DELETE` e
`TRUNCATE` em `wallet_ledger_entries`. Correções financeiras devem ser registradas
como novos lançamentos compensatórios; registros existentes não são editados. Os
testes de integração removem fixtures com uma transação de limpeza privilegiada,
sem relaxar a proteção usada pelo processamento normal. Como em qualquer proteção
por trigger, o superusuário ou proprietário do banco pode desabilitá-los; em
produção, a credencial da aplicação deve ser limitada e distinta da credencial
administrativa usada para migrations.

Uma migration adicional aplica constraints para valores monetários positivos,
transições exatas de saldo nos lançamentos, unicidade de lançamento por
transação, ownership da wallet pelo player e integridade das referências entre
transações e ledger. As novas constraints de validação e chaves estrangeiras são
criadas `NOT VALID` para não apagar nem bloquear a atualização de bancos locais
com dados legados; elas são aplicadas a todas as novas gravações. Antes de
validar registros preexistentes em uma instalação antiga, audite e corrija
eventuais resíduos de testes ou dados históricos.

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
calculado como baseline legado mais créditos menos débitos do ledger. Wallets novas
usam baseline zero e saldo inicial positivo gera uma transação `OPENING` e um
lançamento `CREDIT`, incluídos no cálculo. A migration persiste o baseline legado e
faz backfill para wallets existentes. Divergências são retornadas como
`consistent: false`, registradas em log estruturado sem valores monetários e
incrementam o contador Prometheus
`wallet_reconciliation_divergences_total`, exposto em `GET /metrics`. A reconciliação
é somente leitura e nunca corrige o saldo automaticamente. Para wallets anteriores
à migration, o backfill deriva o baseline do saldo armazenado e dos lançamentos
existentes; não é possível inferir divergências históricas anteriores à criação
desse baseline.

Ao criar uma wallet com saldo inicial positivo, a aplicação grava atomicamente a
wallet, uma transação interna `OPENING` processada, um lançamento `CREDIT` e os
eventos `WagerTransactionProcessed` e `WalletBalanceChanged` na outbox. Com saldo
inicial zero, nenhum registro de abertura financeira é criado. `OPENING` é uma
operação interna e não pode ser submetida como transação externa.

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
