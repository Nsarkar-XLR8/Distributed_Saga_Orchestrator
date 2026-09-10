import { Injectable, Logger } from '@nestjs/common';
import { DataSource } from 'typeorm';
import { v4 as uuidv4 } from 'uuid';
import { Order, OrderStatus } from '../entities/order.entity';
import { OutboxEvent, OutboxStatus } from '../entities/outbox-event.entity';

@Injectable()
export class SagaOrchestratorService {
  private readonly logger = new Logger(SagaOrchestratorService.name);

  constructor(private readonly dataSource: DataSource) {}

  async handleInventoryReserved(payload: any, correlationId: string, eventId: string) {
    const orderId = payload.orderId;
    this.logger.log(`🔄 Saga Orchestrator: Received InventoryReservedEvent for order ${orderId}`);

    const queryRunner = this.dataSource.createQueryRunner();
    await queryRunner.connect();
    await queryRunner.startTransaction();

    try {
      const order = await queryRunner.manager.findOne(Order, {
        where: { id: orderId },
        lock: { mode: 'pessimistic_write' },
      });

      if (!order) {
        this.logger.warn(`Order ${orderId} not found in database for InventoryReservedEvent.`);
        await queryRunner.rollbackTransaction();
        return;
      }

      if (order.status !== OrderStatus.CREATED && order.status !== OrderStatus.INVENTORY_RESERVING) {
        this.logger.warn(`Order ${orderId} in unexpected state: ${order.status}. Skipping transition.`);
        await queryRunner.rollbackTransaction();
        return;
      }

      // Step 2 in Happy Path: Transition to PAYMENT_PENDING
      order.status = OrderStatus.PAYMENT_PENDING;
      await queryRunner.manager.save(Order, order);

      // Create InitiatePaymentCommand in Outbox
      const paymentCommandPayload = {
        orderId: order.id,
        customerId: order.customerId,
        amount: order.totalAmount,
        currency: order.currency,
        correlationId: correlationId || order.correlationId,
      };

      const outboxEvent = new OutboxEvent();
      outboxEvent.id = uuidv4();
      outboxEvent.aggregateType = 'Order';
      outboxEvent.aggregateId = order.id;
      outboxEvent.eventType = 'InitiatePaymentCommand';
      outboxEvent.topic = 'order-events';
      outboxEvent.correlationId = correlationId || order.correlationId;
      outboxEvent.status = OutboxStatus.PENDING;
      outboxEvent.payload = {
        eventId: outboxEvent.id,
        eventType: 'InitiatePaymentCommand',
        aggregateType: 'Order',
        aggregateId: order.id,
        correlationId: outboxEvent.correlationId,
        timestamp: new Date().toISOString(),
        payload: paymentCommandPayload,
      };

      await queryRunner.manager.save(OutboxEvent, outboxEvent);
      await queryRunner.commitTransaction();

      this.logger.log(`✅ Order ${orderId} transitioned to PAYMENT_PENDING. InitiatePaymentCommand queued in outbox.`);
    } catch (error) {
      await queryRunner.rollbackTransaction();
      this.logger.error(`Error in handleInventoryReserved for ${orderId}: ${error.message}`, error.stack);
      throw error;
    } finally {
      await queryRunner.release();
    }
  }

  async handleInventoryReservationFailed(payload: any, correlationId: string, eventId: string) {
    const orderId = payload.orderId;
    const reason = payload.reason || 'OUT_OF_STOCK';
    this.logger.warn(`❌ Saga Orchestrator: Received InventoryReservationFailedEvent for order ${orderId} (Reason: ${reason})`);

    const queryRunner = this.dataSource.createQueryRunner();
    await queryRunner.connect();
    await queryRunner.startTransaction();

    try {
      const order = await queryRunner.manager.findOne(Order, {
        where: { id: orderId },
        lock: { mode: 'pessimistic_write' },
      });

      if (!order) {
        await queryRunner.rollbackTransaction();
        return;
      }

      if (order.status === OrderStatus.CANCELLED) {
        await queryRunner.rollbackTransaction();
        return;
      }

      // Transition to CANCELLED
      order.status = OrderStatus.CANCELLED;
      await queryRunner.manager.save(Order, order);

      // Publish OrderCancelledEvent in Outbox
      const outboxEvent = new OutboxEvent();
      outboxEvent.id = uuidv4();
      outboxEvent.aggregateType = 'Order';
      outboxEvent.aggregateId = order.id;
      outboxEvent.eventType = 'OrderCancelledEvent';
      outboxEvent.topic = 'order-events';
      outboxEvent.correlationId = correlationId || order.correlationId;
      outboxEvent.status = OutboxStatus.PENDING;
      outboxEvent.payload = {
        eventId: outboxEvent.id,
        eventType: 'OrderCancelledEvent',
        aggregateType: 'Order',
        aggregateId: order.id,
        correlationId: outboxEvent.correlationId,
        timestamp: new Date().toISOString(),
        payload: { orderId, reason, status: 'CANCELLED' },
      };

      await queryRunner.manager.save(OutboxEvent, outboxEvent);
      await queryRunner.commitTransaction();

      this.logger.log(`🚫 Order ${orderId} cancelled due to stock reservation failure.`);
    } catch (error) {
      await queryRunner.rollbackTransaction();
      this.logger.error(`Error in handleInventoryReservationFailed: ${error.message}`, error.stack);
      throw error;
    } finally {
      await queryRunner.release();
    }
  }

  async handlePaymentCaptured(payload: any, correlationId: string, eventId: string) {
    const orderId = payload.orderId;
    const transactionId = payload.transactionId;
    this.logger.log(`🎉 Saga Orchestrator: Received PaymentCapturedEvent for order ${orderId} (tx: ${transactionId})`);

    const queryRunner = this.dataSource.createQueryRunner();
    await queryRunner.connect();
    await queryRunner.startTransaction();

    try {
      const order = await queryRunner.manager.findOne(Order, {
        where: { id: orderId },
        lock: { mode: 'pessimistic_write' },
      });

      if (!order) {
        await queryRunner.rollbackTransaction();
        return;
      }

      if (order.status === OrderStatus.CONFIRMED) {
        this.logger.log(`Order ${orderId} already CONFIRMED (idempotent).`);
        await queryRunner.rollbackTransaction();
        return;
      }

      // Step 3 in Happy Path: Transition to CONFIRMED
      order.status = OrderStatus.CONFIRMED;
      await queryRunner.manager.save(Order, order);

      // Publish OrderConfirmedEvent in Outbox
      const outboxEvent = new OutboxEvent();
      outboxEvent.id = uuidv4();
      outboxEvent.aggregateType = 'Order';
      outboxEvent.aggregateId = order.id;
      outboxEvent.eventType = 'OrderConfirmedEvent';
      outboxEvent.topic = 'order-events';
      outboxEvent.correlationId = correlationId || order.correlationId;
      outboxEvent.status = OutboxStatus.PENDING;
      outboxEvent.payload = {
        eventId: outboxEvent.id,
        eventType: 'OrderConfirmedEvent',
        aggregateType: 'Order',
        aggregateId: order.id,
        correlationId: outboxEvent.correlationId,
        timestamp: new Date().toISOString(),
        payload: {
          orderId,
          transactionId,
          totalAmount: order.totalAmount,
          currency: order.currency,
          status: 'CONFIRMED',
        },
      };

      await queryRunner.manager.save(OutboxEvent, outboxEvent);
      await queryRunner.commitTransaction();

      this.logger.log(`🏆 SAGA COMPLETE: Order ${orderId} is CONFIRMED.`);
    } catch (error) {
      await queryRunner.rollbackTransaction();
      this.logger.error(`Error in handlePaymentCaptured for ${orderId}: ${error.message}`, error.stack);
      throw error;
    } finally {
      await queryRunner.release();
    }
  }

  async handlePaymentFailed(payload: any, correlationId: string, eventId: string) {
    const orderId = payload.orderId;
    const failureReason = payload.failureReason || 'PAYMENT_DECLINED';
    this.logger.warn(`⚠️ Saga Orchestrator: Received PaymentFailedEvent for order ${orderId} (${failureReason}). Initiating compensation.`);

    const queryRunner = this.dataSource.createQueryRunner();
    await queryRunner.connect();
    await queryRunner.startTransaction();

    try {
      const order = await queryRunner.manager.findOne(Order, {
        where: { id: orderId },
        lock: { mode: 'pessimistic_write' },
      });

      if (!order) {
        await queryRunner.rollbackTransaction();
        return;
      }

      if (order.status === OrderStatus.COMPENSATING_INVENTORY || order.status === OrderStatus.CANCELLED) {
        await queryRunner.rollbackTransaction();
        return;
      }

      // Step 3 in Failure Path: Transition to COMPENSATING_INVENTORY
      order.status = OrderStatus.COMPENSATING_INVENTORY;
      await queryRunner.manager.save(Order, order);

      // Create ReleaseInventoryCommand in Outbox
      const outboxEvent = new OutboxEvent();
      outboxEvent.id = uuidv4();
      outboxEvent.aggregateType = 'Order';
      outboxEvent.aggregateId = order.id;
      outboxEvent.eventType = 'ReleaseInventoryCommand';
      outboxEvent.topic = 'order-events';
      outboxEvent.correlationId = correlationId || order.correlationId;
      outboxEvent.status = OutboxStatus.PENDING;
      outboxEvent.payload = {
        eventId: outboxEvent.id,
        eventType: 'ReleaseInventoryCommand',
        aggregateType: 'Order',
        aggregateId: order.id,
        correlationId: outboxEvent.correlationId,
        timestamp: new Date().toISOString(),
        payload: {
          orderId,
          reason: failureReason,
        },
      };

      await queryRunner.manager.save(OutboxEvent, outboxEvent);
      await queryRunner.commitTransaction();

      this.logger.log(`🔄 Compensating workflow triggered for order ${orderId}: ReleaseInventoryCommand queued.`);
    } catch (error) {
      await queryRunner.rollbackTransaction();
      this.logger.error(`Error in handlePaymentFailed for ${orderId}: ${error.message}`, error.stack);
      throw error;
    } finally {
      await queryRunner.release();
    }
  }

  async handleInventoryReleased(payload: any, correlationId: string, eventId: string) {
    const orderId = payload.orderId;
    this.logger.log(`🛡️ Saga Orchestrator: Received InventoryReleasedEvent for order ${orderId}. Finalizing cancellation.`);

    const queryRunner = this.dataSource.createQueryRunner();
    await queryRunner.connect();
    await queryRunner.startTransaction();

    try {
      const order = await queryRunner.manager.findOne(Order, {
        where: { id: orderId },
        lock: { mode: 'pessimistic_write' },
      });

      if (!order) {
        await queryRunner.rollbackTransaction();
        return;
      }

      // Transition to CANCELLED
      order.status = OrderStatus.CANCELLED;
      await queryRunner.manager.save(Order, order);

      // Publish OrderCancelledEvent in Outbox
      const outboxEvent = new OutboxEvent();
      outboxEvent.id = uuidv4();
      outboxEvent.aggregateType = 'Order';
      outboxEvent.aggregateId = order.id;
      outboxEvent.eventType = 'OrderCancelledEvent';
      outboxEvent.topic = 'order-events';
      outboxEvent.correlationId = correlationId || order.correlationId;
      outboxEvent.status = OutboxStatus.PENDING;
      outboxEvent.payload = {
        eventId: outboxEvent.id,
        eventType: 'OrderCancelledEvent',
        aggregateType: 'Order',
        aggregateId: order.id,
        correlationId: outboxEvent.correlationId,
        timestamp: new Date().toISOString(),
        payload: {
          orderId,
          reason: 'COMPENSATED_AFTER_PAYMENT_FAILURE',
          status: 'CANCELLED',
        },
      };

      await queryRunner.manager.save(OutboxEvent, outboxEvent);
      await queryRunner.commitTransaction();

      this.logger.log(`🏁 SAGA ROLLBACK COMPLETE: Order ${orderId} successfully compensated and CANCELLED.`);
    } catch (error) {
      await queryRunner.rollbackTransaction();
      this.logger.error(`Error in handleInventoryReleased for ${orderId}: ${error.message}`, error.stack);
      throw error;
    } finally {
      await queryRunner.release();
    }
  }
}
