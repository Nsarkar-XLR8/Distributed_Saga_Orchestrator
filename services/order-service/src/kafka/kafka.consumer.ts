import { Injectable, OnModuleInit, OnModuleDestroy, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Kafka, Consumer } from 'kafkajs';
import { SagaOrchestratorService } from '../services/saga-orchestrator.service';
import { KafkaProducerService } from './kafka.producer';

@Injectable()
export class KafkaConsumerService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(KafkaConsumerService.name);
  private kafka: Kafka;
  private consumer: Consumer;
  private isRunning = false;

  constructor(
    private readonly configService: ConfigService,
    private readonly sagaOrchestrator: SagaOrchestratorService,
    private readonly kafkaProducer: KafkaProducerService,
  ) {
    const bootstrapServers = this.configService.get<string>(
      'KAFKA_BOOTSTRAP_SERVERS',
      'localhost:9092',
    );

    this.kafka = new Kafka({
      clientId: 'order-saga-orchestrator',
      brokers: bootstrapServers.split(','),
      retry: {
        initialRetryTime: 300,
        retries: 5,
      },
    });

    this.consumer = this.kafka.consumer({
      groupId: 'order-saga-orchestrator-group',
      allowAutoTopicCreation: true,
    });
  }

  async onModuleInit() {
    await this.startConsumerWithRetry();
  }

  async onModuleDestroy() {
    if (this.isRunning) {
      await this.consumer.disconnect();
      this.logger.log('Kafka saga consumer disconnected.');
    }
  }

  private async startConsumerWithRetry(retries = 5, delay = 2500) {
    for (let i = 0; i < retries; i++) {
      try {
        await this.consumer.connect();
        await this.consumer.subscribe({
          topics: ['inventory-events', 'payment-events'],
          fromBeginning: false,
        });

        this.isRunning = true;
        this.logger.log('✅ Kafka Saga Consumer connected & subscribed to [inventory-events, payment-events]');

        await this.consumer.run({
          eachMessage: async ({ topic, partition, message }) => {
            await this.processMessage(topic, partition, message);
          },
        });
        return;
      } catch (error) {
        this.logger.warn(
          `Kafka Consumer connection attempt ${i + 1}/${retries} failed: ${error.message}. Retrying in ${delay}ms...`,
        );
        await new Promise((res) => setTimeout(res, delay));
      }
    }
    this.logger.error('❌ Failed to start Kafka Consumer after maximum retries');
  }

  private async processMessage(topic: string, partition: number, message: any) {
    const rawValue = message.value?.toString();
    const key = message.key?.toString();

    if (!rawValue) {
      this.logger.warn(`Received empty message on topic ${topic}`);
      return;
    }

    try {
      let parsedEvent: any;
      try {
        parsedEvent = JSON.parse(rawValue);
      } catch (parseError) {
        // Poison Pill Detected: Invalid JSON! Route to DLT
        this.logger.error(
          `🚨 POISON PILL DETECTED on topic [${topic}] partition [${partition}]: ${parseError.message}. Routing to ${topic}.DLT`,
        );
        await this.kafkaProducer.emitEvent(`${topic}.DLT`, key || 'poison-pill', {
          originalTopic: topic,
          partition,
          rawPayload: rawValue,
          error: parseError.message,
          quarantinedAt: new Date().toISOString(),
        });
        return;
      }

      const eventType = parsedEvent.eventType || message.headers?.['x-event-type']?.toString();
      const correlationId = parsedEvent.correlationId || key || 'N/A';
      const eventId = parsedEvent.eventId || 'N/A';
      const payload = parsedEvent.payload || parsedEvent;

      this.logger.log(`📥 Received [${topic}] -> ${eventType} (Correlation: ${correlationId}, EventId: ${eventId})`);

      switch (eventType) {
        case 'InventoryReservedEvent':
          await this.sagaOrchestrator.handleInventoryReserved(payload, correlationId, eventId);
          break;

        case 'InventoryReservationFailedEvent':
          await this.sagaOrchestrator.handleInventoryReservationFailed(payload, correlationId, eventId);
          break;

        case 'PaymentCapturedEvent':
          await this.sagaOrchestrator.handlePaymentCaptured(payload, correlationId, eventId);
          break;

        case 'PaymentFailedEvent':
          await this.sagaOrchestrator.handlePaymentFailed(payload, correlationId, eventId);
          break;

        case 'InventoryReleasedEvent':
          await this.sagaOrchestrator.handleInventoryReleased(payload, correlationId, eventId);
          break;

        default:
          this.logger.debug(`Ignoring eventType [${eventType}] on topic [${topic}]`);
          break;
      }
    } catch (processingError) {
      this.logger.error(
        `Failed to process message on topic ${topic}: ${processingError.message}`,
        processingError.stack,
      );
      // Route unrecoverable processing failure to DLT
      try {
        await this.kafkaProducer.emitEvent(`${topic}.DLT`, key || 'failed-message', {
          originalTopic: topic,
          partition,
          rawPayload: rawValue,
          error: processingError.message,
          quarantinedAt: new Date().toISOString(),
        });
      } catch (dltError) {
        this.logger.error(`Failed to route poison message to DLT: ${dltError.message}`);
      }
    }
  }
}
