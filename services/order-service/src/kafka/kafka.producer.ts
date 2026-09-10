import { Injectable, OnModuleInit, OnModuleDestroy, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Kafka, Producer, RecordMetadata } from 'kafkajs';

@Injectable()
export class KafkaProducerService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(KafkaProducerService.name);
  private kafka: Kafka;
  private producer: Producer;
  private isConnected = false;

  constructor(private readonly configService: ConfigService) {
    const bootstrapServers = this.configService.get<string>(
      'KAFKA_BOOTSTRAP_SERVERS',
      'localhost:9092',
    );

    this.kafka = new Kafka({
      clientId: 'order-service-producer',
      brokers: bootstrapServers.split(','),
      retry: {
        initialRetryTime: 300,
        retries: 5,
      },
    });

    this.producer = this.kafka.producer({
      allowAutoTopicCreation: true,
    });
  }

  async onModuleInit() {
    await this.connectWithRetry();
  }

  async onModuleDestroy() {
    if (this.isConnected) {
      await this.producer.disconnect();
      this.logger.log('Kafka producer disconnected.');
    }
  }

  private async connectWithRetry(retries = 5, delay = 2000) {
    for (let i = 0; i < retries; i++) {
      try {
        await this.producer.connect();
        this.isConnected = true;
        this.logger.log('✅ Kafka Producer connected successfully');
        return;
      } catch (error) {
        this.logger.warn(
          `Kafka Producer connection attempt ${i + 1}/${retries} failed: ${error.message}. Retrying in ${delay}ms...`,
        );
        await new Promise((res) => setTimeout(res, delay));
      }
    }
    this.logger.error('❌ Failed to connect Kafka Producer after maximum retries');
  }

  async emitEvent(
    topic: string,
    key: string,
    payload: any,
  ): Promise<RecordMetadata[]> {
    if (!this.isConnected) {
      // Attempt reconnect if disconnected during runtime
      try {
        await this.producer.connect();
        this.isConnected = true;
      } catch (err) {
        throw new Error(`Kafka Producer is not connected: ${err.message}`);
      }
    }

    const value = typeof payload === 'string' ? payload : JSON.stringify(payload);

    return this.producer.send({
      topic,
      messages: [
        {
          key,
          value,
          timestamp: Date.now().toString(),
          headers: {
            'x-correlation-id': payload?.correlationId || key,
            'x-event-type': payload?.eventType || 'UNKNOWN',
          },
        },
      ],
    });
  }
}
