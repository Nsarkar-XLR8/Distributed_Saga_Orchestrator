import { Injectable, Logger } from '@nestjs/common';
import { Interval, Cron, CronExpression } from '@nestjs/schedule';
import { DataSource } from 'typeorm';
import { OutboxEvent, OutboxStatus } from '../entities/outbox-event.entity';
import { KafkaProducerService } from '../kafka/kafka.producer';

@Injectable()
export class OutboxRelayService {
  private readonly logger = new Logger(OutboxRelayService.name);
  private isProcessing = false;

  constructor(
    private readonly dataSource: DataSource,
    private readonly kafkaProducer: KafkaProducerService,
  ) {}

  @Interval(400) // Polls every 400ms
  async pollAndRelayOutbox() {
    if (this.isProcessing) {
      return; // Prevent overlapping poller cycles on slow executions
    }

    this.isProcessing = true;

    try {
      // 1. Phase 1: Claim lease in SQL using FOR UPDATE SKIP LOCKED
      const claimedEvents = await this.claimLeasedBatch(50);

      if (!claimedEvents || claimedEvents.length === 0) {
        this.isProcessing = false;
        return;
      }

      this.logger.log(`Claimed ${claimedEvents.length} outbox event(s) for asynchronous relay.`);

      // 2. Phase 2: Dispatch to Kafka asynchronously OUTSIDE open DB locks
      await Promise.allSettled(
        claimedEvents.map(async (event) => {
          try {
            await this.kafkaProducer.emitEvent(
              event.topic,
              event.aggregateId,
              event.payload,
            );

            // 3. Phase 3: Mark PROCESSED upon Kafka acknowledgment
            await this.markProcessed(event.id);
            this.logger.log(`✅ Outbox event ${event.id} [${event.eventType}] successfully published to Kafka topic ${event.topic}`);
          } catch (error) {
            this.logger.error(
              `❌ Failed to relay outbox event ${event.id}: ${error.message}`,
            );
            await this.handleRelayFailure(event, error.message);
          }
        }),
      );
    } catch (error) {
      this.logger.error(`Error during outbox polling cycle: ${error.message}`);
    } finally {
      this.isProcessing = false;
    }
  }

  // Native 2-Phase Status Lease in SQL
  private async claimLeasedBatch(batchSize = 50): Promise<OutboxEvent[]> {
    const rawEvents = await this.dataSource.query(
      `
      UPDATE outbox_events
      SET status = '${OutboxStatus.IN_FLIGHT}', 
          lease_expires_at = NOW() + INTERVAL '10 seconds'
      WHERE id IN (
        SELECT id FROM outbox_events
        WHERE (status = '${OutboxStatus.PENDING}' OR (status = '${OutboxStatus.IN_FLIGHT}' AND lease_expires_at < NOW()))
        ORDER BY created_at ASC
        LIMIT $1
        FOR UPDATE SKIP LOCKED
      )
      RETURNING *;
      `,
      [batchSize],
    );

    return rawEvents.map((r: any) => {
      const event = new OutboxEvent();
      event.id = r.id;
      event.aggregateType = r.aggregate_type;
      event.aggregateId = r.aggregate_id;
      event.eventType = r.event_type;
      event.payload = typeof r.payload === 'string' ? JSON.parse(r.payload) : r.payload;
      event.topic = r.topic;
      event.correlationId = r.correlation_id;
      event.status = r.status;
      event.leaseExpiresAt = r.lease_expires_at;
      event.retryCount = r.retry_count || 0;
      event.lastError = r.last_error;
      event.createdAt = r.created_at;
      event.processedAt = r.processed_at;
      return event;
    });
  }

  private async markProcessed(eventId: string) {
    await this.dataSource
      .getRepository(OutboxEvent)
      .createQueryBuilder()
      .update(OutboxEvent)
      .set({
        status: OutboxStatus.PROCESSED,
        processedAt: new Date(),
        leaseExpiresAt: null,
      })
      .where('id = :id', { id: eventId })
      .execute();
  }

  private async handleRelayFailure(event: OutboxEvent, errorMessage: string) {
    const nextRetryCount = (event.retryCount || 0) + 1;
    const isFailed = nextRetryCount >= 5;

    await this.dataSource
      .getRepository(OutboxEvent)
      .createQueryBuilder()
      .update(OutboxEvent)
      .set({
        status: isFailed ? OutboxStatus.FAILED : OutboxStatus.PENDING,
        retryCount: nextRetryCount,
        lastError: errorMessage,
        leaseExpiresAt: null, // release lease so it can be retried
      })
      .where('id = :id', { id: event.id })
      .execute();
  }

  // Daily / Hourly Archival Cleanup Job
  @Cron(CronExpression.EVERY_HOUR)
  async cleanupArchivedOutboxEvents() {
    this.logger.log('Running Outbox archival cleanup job (purging PROCESSED events older than 24h)...');
    const result = await this.dataSource.query(`
      DELETE FROM outbox_events
      WHERE status = '${OutboxStatus.PROCESSED}'
      AND processed_at < NOW() - INTERVAL '24 hours';
    `);
    this.logger.log(`Outbox archival cleanup complete. Deleted ${result?.[1] || 0} old records.`);
  }
}
