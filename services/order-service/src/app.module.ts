import { Module, OnModuleInit, NestModule, MiddlewareConsumer, RequestMethod, Logger } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { ScheduleModule } from '@nestjs/schedule';
import { DataSource } from 'typeorm';

import { Order } from './entities/order.entity';
import { OrderItem } from './entities/order-item.entity';
import { OutboxEvent } from './entities/outbox-event.entity';
import { IngressIdempotencyKey } from './entities/ingress-idempotency.entity';
import { OrderController } from './controllers/order.controller';
import { OrderService } from './services/order.service';
import { OutboxRelayService } from './services/outbox-relay.service';
import { SagaOrchestratorService } from './services/saga-orchestrator.service';
import { SagaTimeoutService } from './services/saga-timeout.service';
import { KafkaProducerService } from './kafka/kafka.producer';
import { KafkaConsumerService } from './kafka/kafka.consumer';
import { IdempotencyMiddleware } from './middleware/idempotency.middleware';

@Module({
  imports: [
    ConfigModule.forRoot({ isGlobal: true }),
    ScheduleModule.forRoot(),
    TypeOrmModule.forRootAsync({
      imports: [ConfigModule],
      inject: [ConfigService],
      useFactory: (config: ConfigService) => ({
        type: 'postgres',
        host: config.get<string>('ORDER_DB_HOST', 'localhost'),
        port: config.get<number>('ORDER_DB_PORT', 5432),
        username: config.get<string>('ORDER_DB_USER', 'postgres'),
        password: config.get<string>('ORDER_DB_PASSWORD', 'postgres'),
        database: config.get<string>('ORDER_DB_NAME', 'order_db'),
        entities: [Order, OrderItem, OutboxEvent, IngressIdempotencyKey],
        synchronize: true,
      }),
    }),
    TypeOrmModule.forFeature([Order, OrderItem, OutboxEvent, IngressIdempotencyKey]),
  ],
  controllers: [OrderController],
  providers: [
    OrderService,
    OutboxRelayService,
    SagaOrchestratorService,
    SagaTimeoutService,
    KafkaProducerService,
    KafkaConsumerService,
  ],
  exports: [
    OrderService,
    OutboxRelayService,
    SagaOrchestratorService,
    SagaTimeoutService,
    KafkaProducerService,
    KafkaConsumerService,
  ],
})
export class AppModule implements OnModuleInit, NestModule {
  private readonly logger = new Logger(AppModule.name);

  constructor(private readonly dataSource: DataSource) {}

  configure(consumer: MiddlewareConsumer) {
    consumer
      .apply(IdempotencyMiddleware)
      .forRoutes({ path: 'api/v1/orders', method: RequestMethod.POST });
  }

  async onModuleInit() {
    this.logger.log('Order Service database schema initialized successfully.');
  }
}
