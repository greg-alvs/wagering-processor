# Contexto do projeto — Jungle Gaming Technical Challenge

## 1. Objetivo

Implementar o desafio técnico "Distributed Wagering Processor" da Jungle Gaming: uma API backend para processar transações de apostas, com consistência financeira, idempotência, concorrência segura e processamento assíncrono confiável.

O projeto deve ser adequado para avaliação técnica, com código organizado, testes automatizados, documentação e execução reproduzível.

## 2. Tecnologias obrigatórias

- Bun 1.x como runtime e gerenciador de pacotes.
- TypeScript com strict mode.
- NestJS para a API.
- PostgreSQL para persistência.
- MikroORM como ORM preferencial, com migrations versionadas e reversíveis.
- AWS SQS via LocalStack para desenvolvimento local.
- Docker Compose para executar todos os serviços, incluindo a aplicação na entrega final.
- Testes unitários, integração e concorrência.

Não utilizar Prisma. Não substituir as tecnologias exigidas por alternativas.

## 3. Estratégia de desenvolvimento

Desenvolver passo a passo, começando pela configuração mínima e avançando para as regras financeiras, API, mensageria e testes.

Durante o desenvolvimento, a aplicação poderá executar no Windows, enquanto PostgreSQL e LocalStack estarão em containers Docker. Na entrega final, toda a aplicação deverá ser executável via Docker Compose.

O objetivo é permitir que o avaliador execute o projeto com poucos comandos, seguindo o README.

## 4. Arquitetura desejada

Separar responsabilidades em camadas:

- Domain: Money, entidades e regras financeiras.
- Application: casos de uso, como processamento de transações e reconciliação.
- Infrastructure: MikroORM, PostgreSQL, SQS, inbox, outbox e workers.
- Presentation: controllers, DTOs, validação HTTP e tratamento de erros.

A API HTTP e o consumidor SQS devem reutilizar o mesmo caso de uso de processamento financeiro.

Evitar lógica de negócio em controllers e consumidores de mensagens.

## 5. Regras financeiras críticas

- Representar dinheiro com precisão decimal. Nunca utilizar number, float ou double para cálculos monetários.
- Utilizar strings decimais na entrada e saída e NUMERIC no PostgreSQL.
- Impedir saldos negativos, tanto na lógica de negócio quanto por constraints adequadas no banco.
- Criar uma wallet por playerId e currency.
- Manter o ledger imutável.
- Garantir consistência entre o saldo da wallet e a soma dos lançamentos do ledger.
- Criar constraints e índices únicos para proteger a integridade financeira.

Tipos de transação:

- BET: debita o saldo.
- WIN: credita o saldo.
- LOSS: registra a transação sem alterar o saldo.
- REFUND: devolve o valor de uma BET processada, no máximo uma vez.
- ROLLBACK: inverte uma BET, WIN ou REFUND processada, no máximo uma vez.

REFUND e ROLLBACK podem chegar antes da transação de referência. Nesses casos, registrar PENDING_REFERENCE e implementar reprocessamento com backoff e limite de tentativas ou TTL.

Definir explicitamente as regras de negócio para cada tipo antes de implementar.

## 6. Idempotência e concorrência

- Exigir Idempotency-Key nas requisições de criação de transações.
- A mesma chave e o mesmo payload devem retornar o resultado original sem duplicar efeitos.
- A mesma chave com payload diferente deve gerar conflito.
- Calcular hash de um JSON canônico.
- Persistir a idempotência no banco, não apenas em memória.
- Proteger atualizações concorrentes de uma wallet usando transações SQL e locks apropriados.
- A solução deve funcionar com três ou mais instâncias da aplicação.
- Uma aposta deve atualizar saldo, transação e ledger atomicamente.

Cenário obrigatório: duas apostas simultâneas de R$ 80 em uma wallet com R$ 100. Apenas uma deve ser processada, restando R$ 20, e a outra deve ser rejeitada.

## 7. Mensageria confiável

Implementar:

- SQS FIFO `wager-transactions.fifo`.
- DLQ `wager-transactions-dlq.fifo`.
- Inbox persistente com unicidade por consumerName e messageId.
- ACK somente depois do commit da transação no banco.
- Retry de erros transitórios e encaminhamento para DLQ conforme a configuração.
- Transactional Outbox: persistir eventos na mesma transação SQL dos efeitos financeiros.
- Publisher de outbox com tolerância a duplicações, falhas e reinicializações.
- Shutdown gracioso dos consumidores.

Eventos mínimos: WagerTransactionProcessed, WagerTransactionRejected, WalletBalanceChanged somente quando o saldo mudar e WagerTransactionPendingReference.

## 8. Endpoints esperados

- POST /wallets
- GET /wallets/:walletId
- GET /wallets/:walletId/ledger
- GET /wagering/transactions/:transactionId
- GET /providers/:providerId/wagering/transactions/:externalTransactionId
- POST /wagering/transactions
- POST /wallets/:walletId/reconciliation
- GET /health/live
- GET /health/ready

Validar DTOs, documentar os contratos e utilizar códigos HTTP consistentes.

## 9. Testes e observabilidade

Criar testes para:

- Precisão monetária e regras do domínio.
- Saldo insuficiente e operações válidas.
- Idempotência e conflitos de chave.
- Concorrência sobre a mesma wallet.
- Duplicação e reentrega de mensagens.
- Transações fora de ordem.
- Recuperação da outbox após falhas.
- Consistência do saldo com o ledger.
- Integração com PostgreSQL real e LocalStack.

Adicionar logs estruturados em JSON com correlationId, messageId, transactionId, walletId e providerId, além de métricas relevantes e health checks.

## 10. Regras para a IA durante a implementação

1. Trabalhe em etapas pequenas, uma por vez.
2. Antes de modificar arquivos, inspecione a estrutura atual do projeto.
3. Explique brevemente o objetivo de cada etapa.
4. Forneça os caminhos exatos dos arquivos e os comandos necessários.
5. Não gere o projeto inteiro de uma só vez.
6. Não invente funcionalidades nem altere requisitos do desafio.
7. Não use Prisma, cálculos monetários com number ou persistência apenas em memória para garantir idempotência.
8. Não substitua migrations por sincronização automática de schema.
9. Não considere uma operação financeira concluída antes do commit no banco.
10. Se houver ambiguidade no desafio, consulte o README original e sinalize a dúvida antes de assumir uma regra.
11. Ao terminar cada etapa, execute ou indique os testes e comandos de validação.
12. Preserve alterações já feitas e evite reescrever arquivos inteiros sem necessidade.
13. Não avance para a próxima etapa sem apresentar o resultado e os próximos passos.
14. Priorize correção financeira, idempotência e concorrência antes de otimizações.

## 11. Critério de conclusão

O projeto deverá iniciar pelo Docker Compose, aplicar migrations, criar as filas necessárias, expor a API e executar os workers. O README deve explicar configuração, execução, testes, arquitetura e recuperação de falhas.

Prioridades: primeiro consistência financeira; depois API e regras; em seguida SQS, inbox/outbox e recuperação; por último observabilidade e documentação final.