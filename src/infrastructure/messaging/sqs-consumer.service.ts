import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import {
  ChangeMessageVisibilityCommand,
  DeleteMessageCommand,
  GetQueueUrlCommand,
  GetQueueAttributesCommand,
  ReceiveMessageCommand,
  SQSClient,
  SQSClientConfig,
} from '@aws-sdk/client-sqs';
import { WageringService } from '../../wagering/wagering.service';
import { PermanentMessageError, parseWagerTransactionMessage } from './wager-transaction-message';
import { ApplicationMetrics } from '../observability/application-metrics';

@Injectable()
export class SqsConsumerService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(SqsConsumerService.name);
  private readonly client: SQSClient;
  private readonly queueName: string;
  private readonly dlqQueueName: string;
  private readonly consumerName: string;
  private readonly waitTimeSeconds: number;
  private queueUrl: string | undefined;
  private dlqQueueUrl: string | undefined;
  private stopping = false;
  private polling: Promise<void> | undefined;

  constructor(
    config: ConfigService,
    private readonly wageringService: WageringService,
  ) {
    const clientConfig: SQSClientConfig = {
      region: config.get<string>('AWS_REGION', 'us-east-1'),
      credentials: {
        accessKeyId: config.get<string>('AWS_ACCESS_KEY_ID', 'test'),
        secretAccessKey: config.get<string>('AWS_SECRET_ACCESS_KEY', 'test'),
      },
    };
    const endpoint = config.get<string>('SQS_ENDPOINT');
    if (endpoint) {
      clientConfig.endpoint = endpoint;
    } else {
      clientConfig.endpoint = `http://${config.get<string>('LOCALSTACK_HOST', 'localhost')}:${config.get<string>('LOCALSTACK_PORT', '4566')}`;
    }

    this.client = new SQSClient(clientConfig);
    this.queueName = config.get<string>('SQS_MAIN_QUEUE_NAME', 'wager-transactions.fifo');
    this.dlqQueueName = config.get<string>('SQS_DLQ_QUEUE_NAME', 'wager-transactions-dlq.fifo');
    this.consumerName = config.get<string>('SQS_CONSUMER_NAME', 'wager-transaction-consumer');
    const waitTimeSeconds = Number(config.get<string>('SQS_WAIT_TIME_SECONDS', '20'));
    if (!Number.isInteger(waitTimeSeconds) || waitTimeSeconds < 0 || waitTimeSeconds > 20) {
      throw new Error('SQS_WAIT_TIME_SECONDS must be an integer between 0 and 20.');
    }
    this.waitTimeSeconds = waitTimeSeconds;
  }

  async onModuleInit(): Promise<void> {
    const queue = await this.client.send(new GetQueueUrlCommand({ QueueName: this.queueName }));
    if (!queue.QueueUrl) {
      throw new Error(`SQS did not return a URL for queue ${this.queueName}.`);
    }

    this.queueUrl = queue.QueueUrl;
    const deadLetterQueue = await this.client.send(
      new GetQueueUrlCommand({ QueueName: this.dlqQueueName }),
    );
    if (!deadLetterQueue.QueueUrl) {
      throw new Error(`SQS did not return a URL for dead-letter queue ${this.dlqQueueName}.`);
    }
    this.dlqQueueUrl = deadLetterQueue.QueueUrl;
    this.polling = this.poll();
  }

  async onModuleDestroy(): Promise<void> {
    this.stopping = true;
    await this.polling;
    this.client.destroy();
  }

  async checkReady(): Promise<void> {
    if (!this.queueUrl || !this.dlqQueueUrl) {
      throw new Error('SQS transaction queues have not been initialized.');
    }

    await this.client.send(
      new GetQueueAttributesCommand({
        QueueUrl: this.queueUrl,
        AttributeNames: ['QueueArn'],
      }),
    );
  }

  async getDeadLetterQueueMessageCount(): Promise<number> {
    if (!this.dlqQueueUrl) {
      throw new Error('SQS dead-letter queue has not been initialized.');
    }
    const result = await this.client.send(
      new GetQueueAttributesCommand({
        QueueUrl: this.dlqQueueUrl,
        AttributeNames: ['ApproximateNumberOfMessages'],
      }),
    );
    const messageCount = result.Attributes?.ApproximateNumberOfMessages;
    if (messageCount === undefined || !Number.isInteger(Number(messageCount)) || Number(messageCount) < 0) {
      throw new Error(`SQS returned an invalid message count for dead-letter queue ${this.dlqQueueName}.`);
    }
    return Number(messageCount);
  }

  private async poll(): Promise<void> {
    while (!this.stopping) {
      try {
        const response = await this.client.send(
          new ReceiveMessageCommand({
            QueueUrl: this.queueUrl,
            MaxNumberOfMessages: 1,
            WaitTimeSeconds: this.waitTimeSeconds,
            VisibilityTimeout: 30,
            MessageSystemAttributeNames: ['ApproximateReceiveCount'],
          }),
        );

        for (const message of response.Messages ?? []) {
          await this.processMessage(message);
        }
      } catch (error) {
        if (this.stopping) {
          break;
        }
        ApplicationMetrics.increment('wager_retries_total', { component: 'sqs_consumer' });
        this.logger.error(
          JSON.stringify({ event: 'sqs_receive_failed', errorCode: this.errorCode(error) }),
        );
        await this.delay(1000);
      }
    }
  }

  private async processMessage(message: {
    Body?: string;
    MessageId?: string;
    ReceiptHandle?: string;
    Attributes?: Record<string, string>;
  }): Promise<void> {
    if (!message.MessageId || !message.ReceiptHandle || !message.Body) {
      this.logger.error('Received an SQS message missing its ID, receipt handle, or body.');
      return;
    }

    let providerId: string | undefined;
    let walletId: string | undefined;
    try {
      const dto = await parseWagerTransactionMessage(message.Body);
      providerId = dto.providerId;
      walletId = dto.walletId;
      const result = await this.wageringService.processQueueTransaction(dto, message.MessageId, this.consumerName);

      await this.client.send(
        new DeleteMessageCommand({
          QueueUrl: this.queueUrl,
          ReceiptHandle: message.ReceiptHandle,
        }),
      );

      if ('terminalFailure' in result && result.terminalFailure) {
        this.logger.warn(
          JSON.stringify({
            event: 'sqs_terminal_business_failure_acknowledged',
            correlationId: message.MessageId,
            messageId: message.MessageId,
            providerId,
            walletId,
          }),
        );
      } else if ('duplicate' in result && result.duplicate) {
        this.logger.log(
          JSON.stringify({
            event: 'sqs_duplicate_acknowledged',
            correlationId: message.MessageId,
            messageId: message.MessageId,
            providerId,
            walletId,
          }),
        );
      }
    } catch (error) {
      const receiveCount = Number(message.Attributes?.ApproximateReceiveCount ?? 1);
      const visibilityTimeout = this.retryDelay(receiveCount);
      ApplicationMetrics.increment('wager_retries_total', { component: 'sqs_consumer' });
      try {
        await this.client.send(
          new ChangeMessageVisibilityCommand({
            QueueUrl: this.queueUrl,
            ReceiptHandle: message.ReceiptHandle,
            VisibilityTimeout: visibilityTimeout,
          }),
        );
      } catch (visibilityError) {
        this.logger.error(
          JSON.stringify({
            event: 'sqs_retry_visibility_update_failed',
            correlationId: message.MessageId,
            messageId: message.MessageId,
            errorCode: this.errorCode(visibilityError),
          }),
        );
      }

      const category = error instanceof PermanentMessageError ? 'permanent' : 'transient';
      this.logger.error(
        JSON.stringify({
          event: 'sqs_message_retry_scheduled',
          category,
          correlationId: message.MessageId,
          messageId: message.MessageId,
          providerId,
          walletId,
          receiveCount,
          retryDelaySeconds: visibilityTimeout,
          errorCode: this.errorCode(error),
        }),
      );
    }
  }

  private retryDelay(receiveCount: number): number {
    const attempt = Number.isInteger(receiveCount) && receiveCount > 0 ? receiveCount : 1;
    return Math.min(2 ** Math.min(attempt - 1, 8), 300);
  }

  private errorCode(error: unknown): string | undefined {
    if (typeof error === 'object' && error !== null && 'code' in error) {
      return String(error.code);
    }
    return error instanceof Error ? error.name : undefined;
  }

  private delay(milliseconds: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, milliseconds));
  }
}
