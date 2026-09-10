import { Injectable, Logger } from '@nestjs/common';
import { Cron, CronExpression } from '@nestjs/schedule';
import { DataSource, In, LessThan } from 'typeorm';
import { v4 as uuidv4 } from 'uuid';
import { Order, OrderStatus } from '../entities/order.entity';
import { OutboxEvent, OutboxStatus } from '../entities/outbox-event.entity';

@Injectable()
export class SagaTimeoutService {
  private readonly logger = new Logger(SagaTimeoutService.name);
  private isChecking = false;

  constructor(private readonly dataSource: DataSource) {}

  @Cron('*/10 * * * * *') // Runs every 10 seconds (Dead Man's Switch)
  async checkSagaTimeouts() {
    if (this.isChecking) return;
    this.isChecking = true;

    try {
      const now = new Date();
      const orderRepo = this.dataSource.getRepository(Order);

      // Find timed-out orders in pending states
      const timedOutOrders = await orderRepo.find({
        where: {
          expiresAt: LessThan(now),
          status: In([
            OrderStatus.CREATED,
            OrderStatus.INVENTORY_RESERVING,
            OrderStatus.PAYMENT_PENDING,
          ]),
        },
      });

      if (timedOutOrders.length === 0) {
        return;
      }

      this.logger.warn(`⏳ DEAD MAN'S SWITCH: Detected ${timedOutOrders.length} timed-out saga order(s). Initiating automatic compensation.`);

      for (const order of timedOutOrders) {
        await this.compensateTimedOutOrder(order);
      }
    } catch (error) {
      this.logger.error(`Error in SagaTimeoutService: ${error.message}`, error.stack);
    } finally {
      this.isChecking = false;
    }
  }

  private async compensateTimedOutOrder(order: Order) {
    const queryRunner = this.dataSource.createQueryRunner();
    await queryRunner.connect();
    await queryRunner.startTransaction();

    try {
      // Re-fetch with pessimistic write lock
      const lockedOrder = await queryRunner.manager.findOne(Order, {
        where: { id: order.id },
        lock: { mode: 'pessimistic_write' },
      });

      if (!lockedOrder || lockedOrder.status === OrderStatus.CONFIRMED || lockedOrder.status === OrderStatus.CANCELLED) {
        await queryRunner.rollbackTransaction();
        return;
      }

      const previousStatus = lockedOrder.status;

      if (previousStatus === OrderStatus.PAYMENT_PENDING) {
        // Inventory was reserved, but payment timed out -> Compensate Inventory
        lockedOrder.status = OrderStatus.COMPENSATING_INVENTORY;
        await queryRunner.manager.save(Order, lockedOrder);

        const outboxEvent = new OutboxEvent();
        outboxEvent.id = uuidv4();
        outboxEvent.aggregateType = 'Order';
        outboxEvent.aggregateId = lockedOrder.id;
        outboxEvent.eventType = 'ReleaseInventoryCommand';
        outboxEvent.topic = 'order-events';
        outboxEvent.correlationId = lockedOrder.correlationId;
        outboxEvent.status = OutboxStatus.PENDING;
        outboxEvent.payload = {
          eventId: outboxEvent.id,
          eventType: 'ReleaseInventoryCommand',
          aggregateType: 'Order',
          aggregateId: lockedOrder.id,
          correlationId: lockedOrder.correlationId,
          timestamp: new Date().toISOString(),
          payload: {
            orderId: lockedOrder.id,
            reason: 'SAGA_DEAD_MANS_SWITCH_PAYMENT_TIMEOUT',
          },
        };

        await queryRunner.manager.save(OutboxEvent, outboxEvent);
        this.logger.warn(`⏰ Order ${lockedOrder.id} timed out in PAYMENT_PENDING. Enqueued ReleaseInventoryCommand for stock rollback.`);
      } else {
        // Timed out before inventory reserved -> Mark Cancelled directly
        lockedOrder.status = OrderStatus.CANCELLED;
        await queryRunner.manager.save(Order, lockedOrder);

        const outboxEvent = new OutboxEvent();
        outboxEvent.id = uuidv4();
        outboxEvent.aggregateType = 'Order';
        outboxEvent.aggregateId = lockedOrder.id;
        outboxEvent.eventType = 'OrderCancelledEvent';
        outboxEvent.topic = 'order-events';
        outboxEvent.correlationId = lockedOrder.correlationId;
        outboxEvent.status = OutboxStatus.PENDING;
        outboxEvent.payload = {
          eventId: outboxEvent.id,
          eventType: 'OrderCancelledEvent',
          aggregateType: 'Order',
          aggregateId: lockedOrder.id,
          correlationId: lockedOrder.correlationId,
          timestamp: new Date().toISOString(),
          payload: {
            orderId: lockedOrder.id,
            reason: 'SAGA_DEAD_MANS_SWITCH_INVENTORY_TIMEOUT',
            status: 'CANCELLED',
          },
        };

        await queryRunner.manager.save(OutboxEvent, outboxEvent);
        this.logger.warn(`⏰ Order ${lockedOrder.id} timed out before reservation response. Cancelled directly.`);
      }

      await queryRunner.commitTransaction();
    } catch (err) {
      await queryRunner.rollbackTransaction();
      this.logger.error(`Failed to compensate timed out order ${order.id}: ${err.message}`);
    } finally {
      await queryRunner.release();
    }
  }
}
