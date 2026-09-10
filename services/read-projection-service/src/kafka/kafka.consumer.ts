import { Injectable, OnModuleInit, OnModuleDestroy, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Kafka, Consumer } from 'kafkajs';
import { ProjectionService } from '../services/projection.service';

@Injectable()
export class KafkaProjectionConsumer implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(KafkaProjectionConsumer.name);
  private kafka: Kafka;
  private consumer: Consumer;
  private isRunning = false;

  constructor(
    private readonly configService: ConfigService,
    private readonly projectionService: ProjectionService,
  ) {
    const bootstrapServers = this.configService.get<string>(
      'KAFKA_BOOTSTRAP_SERVERS',
      'localhost:9092',
    );

    this.kafka = new Kafka({
      clientId: 'read-projection-consumer',
      brokers: bootstrapServers.split(','),
      retry: {
        initialRetryTime: 300,
        retries: 5,
      },
    });

    this.consumer = this.kafka.consumer({
      groupId: 'read-projection-service-group',
      allowAutoTopicCreation: true,
    });
  }

  async onModuleInit() {
    await this.startConsumerWithRetry();
  }

  async onModuleDestroy() {
    if (this.isRunning) {
      await this.consumer.disconnect();
      this.logger.log('Kafka projection consumer disconnected.');
    }
  }

  private async startConsumerWithRetry(retries = 5, delay = 2500) {
    for (let i = 0; i < retries; i++) {
      try {
        await this.consumer.connect();
        await this.consumer.subscribe({
          topics: ['order-events', 'inventory-events', 'payment-events'],
          fromBeginning: false,
        });

        this.isRunning = true;
        this.logger.log('✅ Read Projection Kafka Consumer connected & subscribed to [order-events, inventory-events, payment-events]');

        await this.consumer.run({
          eachMessage: async ({ topic, partition, message }) => {
            const rawValue = message.value?.toString();
            if (!rawValue) return;

            try {
              const envelope = JSON.parse(rawValue);
              const eventType = envelope.eventType || message.headers?.['x-event-type']?.toString();
              const correlationId = envelope.correlationId || message.key?.toString();

              this.logger.log(`📊 Projecting [${topic}] -> ${eventType} (Correlation: ${correlationId})`);
              
              // Process and accumulate into MongoDB
              await this.projectionService.processEvent(envelope);

              // Also broadcast the raw event stream event for the visualizer topology beam
              this.projectionService.broadcastLiveEvent('KAFKA_EVENT', {
                topic,
                partition,
                offset: message.offset,
                timestamp: message.timestamp,
                eventType,
                correlationId,
                envelope,
              });
            } catch (err) {
              this.logger.error(`Failed to project message from topic ${topic}: ${err.message}`);
            }
          },
        });
        return;
      } catch (error) {
        this.logger.warn(
          `Kafka Projection Consumer connection attempt ${i + 1}/${retries} failed: ${error.message}. Retrying in ${delay}ms...`,
        );
        await new Promise((res) => setTimeout(res, delay));
      }
    }
    this.logger.error('❌ Failed to start Kafka Projection Consumer after maximum retries');
  }
}
