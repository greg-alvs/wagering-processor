# Arquitetura e decisões técnicas

## Objetivo e escopo atual

O serviço processa transações de wagering com PostgreSQL como fonte de verdade
financeira e SQS FIFO como canal assíncrono. A correção financeira não depende
de estado local da aplicação: inbox, wallet, transação, ledger e outbox são
persistidos no PostgreSQL; workers usam locks do banco para coordenar instâncias.

Este documento descreve o comportamento encontrado no código e registra as
lacunas conhecidas em relação ao desafio. Itens marcados como pendentes não
devem ser interpretados como funcionalidades concluídas.

## Componentes e dependências

| Camada | Responsabilidade | Implementação |
|---|---|---|
| Domain | Dinheiro, wallet, lançamentos e regras de wagering | `src/domain/` |
| Application | Processamento financeiro comum aos canais HTTP e SQS | `WageringService` |
| Infrastructure | PostgreSQL, migrations, inbox, outbox, SQS e workers | `src/database/`, `src/migrations/`, `src/infrastructure/` |
| Presentation | Controllers, DTOs, validação HTTP e health checks | `src/wallets/`, `src/wagering/`, `src/app.controller.ts` |

O PostgreSQL é executado em container e o LocalStack fornece SQS durante o
desenvolvimento. O Compose aplica migrations versionadas com o usuário
administrativo, provisiona em seguida o usuário restrito de runtime e cria as
filas FIFO de transações, DLQ e eventos antes de iniciar a aplicação. O serviço
`migrate` executa `bunx mikro-orm migration:up`; a inicialização não sincroniza
automaticamente o schema.

O projeto registra entidades e migrations MikroORM, mas os caminhos críticos
usam `pg` diretamente. Isso permite passar um `PoolClient` e manter todas as
alterações financeiras e de inbox/outbox na mesma transação SQL. MikroORM não é
usado para calcular ou atualizar saldos.

## Dinheiro e consistência financeira

`Money` recebe e serializa valores como strings decimais, valida escala de duas
casas e representa unidades menores com `bigint`. As tabelas guardam valores em
`numeric(18,2)`. Wallets impedem saldo negativo no domínio e têm constraint de
não negatividade no banco.

No processamento HTTP/SQS:

1. O serviço abre uma transação PostgreSQL.
2. Para SQS, tenta inserir a chave `(consumer_name, message_id)` na inbox.
3. Consulta a chave de idempotência persistida e valida o hash SHA-256 de JSON
   canônico.
4. Trava a wallet com `SELECT ... FOR UPDATE`.
5. Aplica a regra do domínio, persiste transação, saldo, ledger e eventos de
   outbox.
6. Faz commit; somente então o consumidor SQS remove a mensagem.

`BET` debita, `WIN` credita, `LOSS` não altera o saldo e `REFUND`/`ROLLBACK`
dependem de uma referência processada. Referências ausentes são persistidas como
`PENDING_REFERENCE`; o worker usa `FOR UPDATE SKIP LOCKED`, backoff de 1 a 300
segundos, até 10 tentativas ou TTL de 24 horas. A política escolhida pelo
projeto permite uma única reversão total por referência (REFUND ou ROLLBACK),
protegida por índice único parcial. Divergências da reconciliação são reportadas,
nunca corrigidas automaticamente.

Ao criar uma wallet com saldo inicial positivo, a mesma transação grava a wallet,
uma transação interna `OPENING`, o crédito inicial no ledger e os eventos
`WagerTransactionProcessed` e `WalletBalanceChanged` na outbox. O ledger começa
com saldo anterior zero. Wallets com saldo zero não recebem transação nem
lançamento `OPENING`. `OPENING` é uma operação interna e não é aceito pelo
endpoint externo de wagering.

`opening_balance` atua como baseline apenas para wallets anteriores à migration,
derivado no backfill do saldo armazenado menos os lançamentos históricos. Wallets
novas usam baseline zero porque todo saldo inicial positivo já está representado
no ledger. O backfill não pode revelar divergências históricas anteriores ao
baseline criado.

## Idempotência, inbox e outbox

O hash do payload é calculado com chaves JSON ordenadas e SHA-256. A idempotência
é armazenada no PostgreSQL com unicidade para a chave. Inbox, efeitos financeiros
e outbox compartilham o mesmo `PoolClient` e commit. Uma reentrega da mesma
mensagem é reconhecida pela chave persistente da inbox.

No endpoint HTTP, `Idempotency-Key` é obrigatório no header e fica fora do payload
canônico. A resposta confirmada é persistida como JSONB junto da chave. Um replay
com payload igual retorna essa resposta com `idempotentReplay: true`; payload
diferente retorna `409 Conflict`. Mensagens SQS mantêm a chave dentro dos dados
da mensagem.

Os eventos `WagerTransactionProcessed`, `WagerTransactionRejected`,
`WagerTransactionPendingReference` e `WalletBalanceChanged` são gravados junto
com a operação financeira. O publisher reivindica eventos usando
`FOR UPDATE SKIP LOCKED`, respeita a sequência por agregado e publica na fila
FIFO dedicada. O envio pode ser repetido em caso de falha/crash; consumidores
downstream devem deduplicar por `eventId`.

### Lacunas conhecidas de contrato

- Registros de idempotência criados antes da migration que adiciona `response_body`
  não contêm snapshot da resposta. Como não é seguro reconstruir o saldo histórico,
  uma tentativa de replay desses registros retorna conflito explícito.

O endpoint de ledger usa paginação keyset estável ordenada por `(created_at DESC,
id DESC)`. O cursor opaco inclui o timestamp em precisão de microssegundos e o ID;
o limite padrão é 50 e o máximo é 100. A resposta inclui `hasMore` e `nextCursor`.

A migration instala triggers PostgreSQL que rejeitam `UPDATE`, `DELETE` e
`TRUNCATE` na tabela `wallet_ledger_entries`. Correções são novos lançamentos
compensatórios, nunca mutações de registros existentes. Administradores e
proprietários do banco ainda podem desabilitar triggers; a credencial da aplicação
deve ser de privilégio limitado e distinta da credencial administrativa.

## Concorrência e entrega assíncrona

O lock de wallet é PostgreSQL e, portanto, coordena processos em instâncias
distintas. A inbox usa uma chave primária composta, e workers de outbox e
referências reivindicam trabalho com `SKIP LOCKED`. Nenhuma dessas garantias
depende do mapa de locks em memória existente em `WagerTransactionProcessor`,
que não participa da transação SQL do caminho de produção.

O consumidor usa long polling, valida o envelope, chama o caso de uso comum,
faz ACK depois do commit e ajusta visibilidade em falhas para permitir retry.
A política de redrive da fila encaminha mensagens repetidamente não processadas
para a DLQ. O publisher de outbox e o worker de referências têm retry persistido
no banco.

## Observabilidade

`GET /metrics` publica métricas Prometheus para transações por status,
duplicatas, retries, contagem aproximada da DLQ, contenção e espera por lock,
latência de processamento, atraso da outbox e divergências de reconciliação.
Gauges de DLQ/outbox são consultados nas dependências; contadores e histogramas
são locais ao processo e precisam ser agregados pelo Prometheus em instalações
com várias réplicas.

Os caminhos instrumentados escrevem logs JSON com IDs disponíveis e sem payloads
ou valores monetários. O contador de contenção de wallet também usa uma espera
de lock de pelo menos 25 ms como indicador operacional; ele não equivale a uma
falha de lock do PostgreSQL.

## Segurança e configuração

Segredos e endpoints são fornecidos por variáveis de ambiente. O arquivo
`.env.example` contém valores para desenvolvimento local. A API usa
`POSTGRES_APP_USER`/`POSTGRES_APP_PASSWORD`; um serviço de bootstrap do Compose
cria o papel e concede DML nos objetos existentes e futuros criados pelo papel
administrativo de migrations. As credenciais administrativas (`POSTGRES_USER`/
`POSTGRES_PASSWORD`) não são passadas ao container da API. O papel de runtime não
é proprietário das tabelas, recebe `SELECT`/`INSERT`/`UPDATE`, não recebe
`DELETE`/`TRUNCATE` nem privilégios para alterar triggers. O desafio deixa a
autenticação HTTP a cargo do candidato; esta entrega explicitamente não implementa
autenticação nem autorização, e não deve ser exposta diretamente em produção sem
uma camada de proteção.

Uma evolução prevista é integrar Keycloak via OAuth 2.0/OpenID Connect. A API
atuaria como resource server, validando a assinatura dos access tokens Bearer
via JWKS, `issuer`, `audience`, expiração e roles/scopes, com guards NestJS para
autorizar operações e associar a identidade às permissões. Um gateway pode
adicionar validação de borda, sem substituir a autorização no serviço. Segredos
de clients seriam mantidos em secret manager. Identidades de SQS/workers seriam
separadas das identidades de usuários, usando IAM/credenciais de serviço; health
checks poderiam ser limitados à rede interna.

## Validação existente e trabalho pendente

Build TypeScript, testes de domínio e integrações com PostgreSQL estão disponíveis
via `bun run build` e `bun test`. `docker compose up --build` prepara PostgreSQL,
aplica migrations, inicializa as credenciais restritas, cria filas e inicia a API
e os workers. Os testes opt-in
`sqs-consumer.localstack.integration.spec.ts` e
`outbox-publisher.localstack.integration.spec.ts` criam filas FIFO temporárias.
Eles verificam três consumidores independentes, retry após falha transitória,
commit antes do ACK, redrive à DLQ, dois publishers concorrentes e retomada após
falha no intervalo entre envio ao SQS e atualização do registro no PostgreSQL.
Execute-os com `RUN_LOCALSTACK_INTEGRATION=1` e PostgreSQL/LocalStack ativos.

Também foram executadas três instâncias reais da API em containers separados,
cada uma processando seis mensagens SQS; as 18 transações foram confirmadas no
PostgreSQL e a inbox registrou as 18 mensagens, sem redrive à DLQ.

O teste de integração PostgreSQL `wagering-inbox.integration.spec.ts` dispara 50
processamentos concorrentes com o mesmo `consumerName` e `messageId`; valida
49 duplicatas e uma única alteração financeira, linha de inbox, transação e
conjunto de eventos.

Os seguintes cenários oficiais ainda precisam de testes automatizados dedicados:

- crash real de processo/host nos limites entre commit/ACK e publicação/registro,
  além de recuperação verificada após reinício do consumidor.

Não se afirma suporte verificado a esses cenários até que os testes correspondentes
sejam adicionados e executados.
