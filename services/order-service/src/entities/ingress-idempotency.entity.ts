import { Entity, Column, PrimaryColumn, CreateDateColumn } from 'typeorm';

@Entity('ingress_idempotency_keys')
export class IngressIdempotencyKey {
  @PrimaryColumn({ type: 'varchar', length: 128 })
  key: string;

  @Column({ type: 'int', name: 'response_code' })
  responseCode: number;

  @Column({ type: 'jsonb', name: 'response_body' })
  responseBody: any;

  @CreateDateColumn({ type: 'timestamp with time zone', name: 'created_at' })
  createdAt: Date;
}
