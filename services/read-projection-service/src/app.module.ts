import { Module } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { MongooseModule } from '@nestjs/mongoose';
import {
  OrderDashboard,
  OrderDashboardSchema,
} from './schemas/order-dashboard.schema';
import { ProjectionController } from './controllers/projection.controller';
import { ProjectionService } from './services/projection.service';
import { KafkaProjectionConsumer } from './kafka/kafka.consumer';

@Module({
  imports: [
    ConfigModule.forRoot({ isGlobal: true }),
    MongooseModule.forRootAsync({
      imports: [ConfigModule],
      inject: [ConfigService],
      useFactory: (config: ConfigService) => ({
        uri:
          config.get<string>('READ_MODEL_MONGO_URI') ||
          'mongodb://localhost:27017/read_model_db',
      }),
    }),
    MongooseModule.forFeature([
      { name: OrderDashboard.name, schema: OrderDashboardSchema },
    ]),
  ],
  controllers: [ProjectionController],
  providers: [ProjectionService, KafkaProjectionConsumer],
  exports: [ProjectionService, KafkaProjectionConsumer],
})
export class AppModule {}
