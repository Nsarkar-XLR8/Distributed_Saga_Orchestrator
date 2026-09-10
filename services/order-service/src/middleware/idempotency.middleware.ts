import {
  Injectable,
  NestMiddleware,
  BadRequestException,
  Logger,
} from '@nestjs/common';
import { Request, Response, NextFunction } from 'express';
import { DataSource } from 'typeorm';
import { IngressIdempotencyKey } from '../entities/ingress-idempotency.entity';

@Injectable()
export class IdempotencyMiddleware implements NestMiddleware {
  private readonly logger = new Logger(IdempotencyMiddleware.name);

  constructor(private readonly dataSource: DataSource) {}

  async use(req: Request, res: Response, next: NextFunction) {
    // Only apply idempotency filter to mutation endpoints (POST / PUT / PATCH)
    if (req.method !== 'POST' && req.method !== 'PUT' && req.method !== 'PATCH') {
      return next();
    }

    // Only apply to /api/v1/orders creation
    if (!req.path.includes('/api/v1/orders') || req.path.includes('/outbox')) {
      return next();
    }

    const idempotencyKey = req.headers['idempotency-key'] as string;

    if (!idempotencyKey || idempotencyKey.trim() === '') {
      throw new BadRequestException(
        'Missing required header: Idempotency-Key. Every checkout mutation requires an idempotent client key.',
      );
    }

    try {
      const cached = await this.dataSource
        .getRepository(IngressIdempotencyKey)
        .findOne({ where: { key: idempotencyKey } });

      if (cached) {
        this.logger.log(
          `⚡ Idempotency Hit for key: [${idempotencyKey}]. Returning cached HTTP ${cached.responseCode} response.`,
        );
        return res.status(cached.responseCode).json({
          ...cached.responseBody,
          _cached: true,
          _idempotentReplay: true,
        });
      }

      next();
    } catch (err) {
      this.logger.error(`Error checking idempotency key: ${err.message}`);
      next();
    }
  }
}
