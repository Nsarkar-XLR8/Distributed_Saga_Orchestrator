import {
  Entity,
  Column,
  PrimaryColumn,
  OneToMany,
  CreateDateColumn,
  UpdateDateColumn,
  Index,
} from 'typeorm';
import { OrderItem } from './order-item.entity';

export enum OrderStatus {
  CREATED = 'CREATED',
  INVENTORY_RESERVING = 'INVENTORY_RESERVING',
  PAYMENT_PENDING = 'PAYMENT_PENDING',
  CONFIRMED = 'CONFIRMED',
  COMPENSATING_INVENTORY = 'COMPENSATING_INVENTORY',
  CANCELLED = 'CANCELLED',
}

@Entity('orders')
export class Order {
  @PrimaryColumn({ type: 'varchar', length: 64 })
  id: string;

  @Column({ type: 'varchar', length: 64, name: 'customer_id' })
  customerId: string;

  @Index({ unique: true })
  @Column({ type: 'varchar', length: 128, name: 'idempotency_key' })
  idempotencyKey: string;

  @Column({ type: 'numeric', precision: 12, scale: 2, name: 'total_amount' })
  totalAmount: number;

  @Column({ type: 'varchar', length: 3, default: 'USD' })
  currency: string;

  @Column({
    type: 'varchar',
    length: 32,
    default: OrderStatus.CREATED,
  })
  status: OrderStatus;

  @Column({ type: 'varchar', length: 64, name: 'correlation_id' })
  correlationId: string;

  @Index('idx_orders_saga_timeout')
  @Column({ type: 'timestamp with time zone', name: 'expires_at' })
  expiresAt: Date;

  @CreateDateColumn({ type: 'timestamp with time zone', name: 'created_at' })
  createdAt: Date;

  @UpdateDateColumn({ type: 'timestamp with time zone', name: 'updated_at' })
  updatedAt: Date;

  @OneToMany(() => OrderItem, (item) => item.order, { cascade: true, eager: true })
  items: OrderItem[];
}
