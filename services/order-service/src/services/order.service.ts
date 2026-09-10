import { Injectable, BadRequestException, Logger } from '@nestjs/common';
import { DataSource } from 'typeorm';
import { v4 as uuidv4 } from 'uuid';
import { Order, OrderStatus } from '../entities/order.entity';
import { OrderItem } from '../entities/order-item.entity';
import { OutboxEvent, OutboxStatus } from '../entities/outbox-event.entity';
import { IngressIdempotencyKey } from '../entities/ingress-idempotency.entity';
import { CreateOrderDto } from '../dto/create-order.dto';

@Injectable()
export class OrderService {
  private readonly logger = new Logger(OrderService.name);

  constructor(private readonly dataSource: DataSource) {}

  async createOrder(idempotencyKey: string, dto: CreateOrderDto) {
    if (!idempotencyKey) {
      throw new BadRequestException('Idempotency-Key header is required');
    }

    const queryRunner = this.dataSource.createQueryRunner();
    await queryRunner.connect();
    await queryRunner.startTransaction();

    try {
      // 1. Check Ingress Idempotency
      const existingIdempotency = await queryRunner.manager.findOne(IngressIdempotencyKey, {
        where: { key: idempotencyKey },
      });

      if (existingIdempotency) {
        this.logger.log(`Idempotent request detected for key: ${idempotencyKey}`);
        await queryRunner.rollbackTransaction();
        return {
          statusCode: existingIdempotency.responseCode,
          data: existingIdempotency.responseBody,
          cached: true,
        };
      }

      // 2. Generate Identifiers
      const orderId = `ord_${uuidv4().substring(0, 8)}`;
      const correlationId = `corr_${uuidv4().substring(0, 8)}`;
      const totalAmount = dto.items.reduce(
        (sum, item) => sum + item.quantity * item.unitPrice,
        0,
      );
      const expiresAt = new Date(Date.now() + 60 * 1000); // 60 seconds Saga timeout deadline

      // 3. Create Order Entity
      const order = new Order();
      order.id = orderId;
      order.customerId = dto.customerId;
      order.idempotencyKey = idempotencyKey;
      order.totalAmount = totalAmount;
      order.currency = dto.currency || 'USD';
      order.status = OrderStatus.CREATED;
      order.correlationId = correlationId;
      order.expiresAt = expiresAt;

      await queryRunner.manager.save(Order, order);

      // 4. Create Order Items
      for (const itemDto of dto.items) {
        const item = new OrderItem();
        item.id = `item_${uuidv4().substring(0, 8)}`;
        item.orderId = orderId;
        item.productId = itemDto.productId;
        item.quantity = itemDto.quantity;
        item.unitPrice = itemDto.unitPrice;
        await queryRunner.manager.save(OrderItem, item);
      }

      // 5. Create OrderCreatedEvent + ReserveInventoryCommand in Outbox
      const reserveCommandPayload = {
        orderId,
        customerId: dto.customerId,
        totalAmount,
        currency: order.currency,
        items: dto.items.map((i) => ({ productId: i.productId, quantity: i.quantity })),
      };

      const outboxEvent = new OutboxEvent();
      outboxEvent.id = uuidv4();
      outboxEvent.aggregateType = 'Order';
      outboxEvent.aggregateId = orderId;
      outboxEvent.eventType = 'ReserveInventoryCommand';
      outboxEvent.topic = 'order-events';
      outboxEvent.correlationId = correlationId;
      outboxEvent.status = OutboxStatus.PENDING;
      outboxEvent.payload = {
        eventId: outboxEvent.id,
        eventType: 'ReserveInventoryCommand',
        aggregateType: 'Order',
        aggregateId: orderId,
        correlationId,
        timestamp: new Date().toISOString(),
        payload: reserveCommandPayload,
      };

      await queryRunner.manager.save(OutboxEvent, outboxEvent);

      const responseBody = {
        orderId,
        customerId: dto.customerId,
        status: order.status,
        totalAmount,
        currency: order.currency,
        correlationId,
        expiresAt,
        createdAt: new Date().toISOString(),
      };

      // 6. Record Ingress Idempotency
      const idempotencyRecord = new IngressIdempotencyKey();
      idempotencyRecord.key = idempotencyKey;
      idempotencyRecord.responseCode = 201;
      idempotencyRecord.responseBody = responseBody;
      await queryRunner.manager.save(IngressIdempotencyKey, idempotencyRecord);

      await queryRunner.commitTransaction();

      this.logger.log(`Order ${orderId} created successfully. Outbox event ${outboxEvent.id} queued.`);
      return {
        statusCode: 201,
        data: responseBody,
        cached: false,
      };
    } catch (error) {
      await queryRunner.rollbackTransaction();
      this.logger.error(`Failed to create order: ${error.message}`, error.stack);
      throw error;
    } finally {
      await queryRunner.release();
    }
  }

  async getOrderById(orderId: string) {
    const order = await this.dataSource.getRepository(Order).findOne({
      where: { id: orderId },
      relations: ['items'],
    });
    if (!order) {
      throw new BadRequestException(`Order ${orderId} not found`);
    }
    return order;
  }

  async getAllOrders() {
    return this.dataSource.getRepository(Order).find({
      relations: ['items'],
      order: { createdAt: 'DESC' },
      take: 50,
    });
  }

  async getOutboxEvents() {
    return this.dataSource.getRepository(OutboxEvent).find({
      order: { createdAt: 'DESC' },
      take: 30,
    });
  }
}
