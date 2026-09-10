import {
  Entity,
  Column,
  PrimaryColumn,
  CreateDateColumn,
  Index,
} from 'typeorm';

export enum OutboxStatus {
  PENDING = 'PENDING',
  IN_FLIGHT = 'IN_FLIGHT',
  PROCESSED = 'PROCESSED',
  FAILED = 'FAILED',
}

@Entity('outbox_events')
export class OutboxEvent {
  @PrimaryColumn({ type: 'varchar', length: 64 })
  id: string;

  @Column({ type: 'varchar', length: 64, name: 'aggregate_type' })
  aggregateType: string;

  @Column({ type: 'varchar', length: 64, name: 'aggregate_id' })
  aggregateId: string;

  @Column({ type: 'varchar', length: 64, name: 'event_type' })
  eventType: string;

  @Column({ type: 'jsonb' })
  payload: any;

  @Column({ type: 'varchar', length: 64 })
  topic: string;

  @Column({ type: 'varchar', length: 64, name: 'correlation_id' })
  correlationId: string;

  @Index('idx_outbox_lease')
  @Column({
    type: 'varchar',
    length: 20,
    default: OutboxStatus.PENDING,
  })
  status: OutboxStatus;

  @Column({ type: 'timestamp with time zone', nullable: true, name: 'lease_expires_at' })
  leaseExpiresAt: Date;

  @Column({ type: 'int', default: 0, name: 'retry_count' })
  retryCount: number;

  @Column({ type: 'text', nullable: true, name: 'last_error' })
  lastError: string;

  @CreateDateColumn({ type: 'timestamp with time zone', name: 'created_at' })
  createdAt: Date;

  @Column({ type: 'timestamp with time zone', nullable: true, name: 'processed_at' })
  processedAt: Date;
}
