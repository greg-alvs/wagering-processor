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

@Injectable()
export class SqsConsumerService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(SqsConsumerService.name);
  private readonly client: SQSClient;
  private readonly queueName: string;
  private readonly consumerName: string;
  private queueUrl: string | undefined;
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
    this.consumerName = config.get<string>('SQS_CONSUMER_NAME', 'wager-transaction-consumer');
  }

  async onModuleInit(): Promise<void> {
    const queue = await this.client.send(new GetQueueUrlCommand({ QueueName: this.queueName }));
    if (!queue.QueueUrl) {
      throw new Error(`SQS did not return a URL for queue ${this.queueName}.`);
    }

    this.queueUrl = queue.QueueUrl;
    this.polling = this.poll();
  }

  async onModuleDestroy(): Promise<void> {
    this.stopping = true;
    await this.polling;
    this.client.destroy();
  }

  async checkReady(): Promise<void> {
    if (!this.queueUrl) {
      throw new Error('SQS transaction queue has not been initialized.');
    }

    await this.client.send(
      new GetQueueAttributesCommand({
        QueueUrl: this.queueUrl,
        AttributeNames: ['QueueArn'],
      }),
    );
  }

  private async poll(): Promise<void> {
    while (!this.stopping) {
      try {
        const response = await this.client.send(
          new ReceiveMessageCommand({
            QueueUrl: this.queueUrl,
            MaxNumberOfMessages: 1,
            WaitTimeSeconds: 20,
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
        this.logger.error(`SQS receive failed: ${this.errorMessage(error)}`);
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

    try {
      const dto = await parseWagerTransactionMessage(message.Body);
      const result = await this.wageringService.processQueueTransaction(dto, message.MessageId, this.consumerName);

      await this.client.send(
        new DeleteMessageCommand({
          QueueUrl: this.queueUrl,
          ReceiptHandle: message.ReceiptHandle,
        }),
      );

      if ('terminalFailure' in result && result.terminalFailure) {
        this.logger.warn(`Acknowledged terminal business failure for message ${message.MessageId}.`);
      } else if ('duplicate' in result && result.duplicate) {
        this.logger.log(`Acknowledged duplicate message ${message.MessageId}.`);
      }
    } catch (error) {
      const receiveCount = Number(message.Attributes?.ApproximateReceiveCount ?? 1);
      const visibilityTimeout = this.retryDelay(receiveCount);
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
          `Could not schedule retry for message ${message.MessageId}: ${this.errorMessage(visibilityError)}`,
        );
      }

      const category = error instanceof PermanentMessageError ? 'permanent' : 'transient';
      this.logger.error(
        `${category} processing failure for message ${message.MessageId}; retry in ${visibilityTimeout}s: ${this.errorMessage(error)}`,
      );
    }
  }

  private retryDelay(receiveCount: number): number {
    const attempt = Number.isInteger(receiveCount) && receiveCount > 0 ? receiveCount : 1;
    return Math.min(2 ** Math.min(attempt - 1, 8), 300);
  }

  private errorMessage(error: unknown): string {
    return error instanceof Error ? error.message : String(error);
  }

  private delay(milliseconds: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, milliseconds));
  }
}
