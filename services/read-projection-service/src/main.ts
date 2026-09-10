import { NestFactory } from '@nestjs/core';
import { Logger } from '@nestjs/common';
import { AppModule } from './app.module';

async function bootstrap() {
  const logger = new Logger('ReadProjectionBootstrap');
  const app = await NestFactory.create(AppModule);

  app.enableCors();

  const port = process.env.READ_PROJECTION_SERVICE_PORT || 3002;
  await app.listen(port);
  logger.log(`🚀 Read Projection Service is running on http://localhost:${port}`);
}

bootstrap();
