import { Injectable, Logger } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import { Subject, Observable } from 'rxjs';
import { OrderDashboard, OrderDashboardDocument } from '../schemas/order-dashboard.schema';

@Injectable()
export class ProjectionService {
  private readonly logger = new Logger(ProjectionService.name);
  private readonly eventStream$ = new Subject<{ type: string; data: any }>();

  constructor(
    @InjectModel(OrderDashboard.name)
    private readonly orderDashboardModel: Model<OrderDashboardDocument>,
  ) {}

  getEventStream(): Observable<{ type: string; data: any }> {
    return this.eventStream$.asObservable();
  }

  broadcastLiveEvent(type: string, data: any) {
    this.eventStream$.next({ type, data });
  }

  async getOrderDashboard(orderId: string): Promise<OrderDashboard | null> {
    return this.orderDashboardModel.findById(orderId).exec();
  }

  async getAllOrders(): Promise<OrderDashboard[]> {
    return this.orderDashboardModel
      .find()
      .sort({ updatedAt: -1 })
      .limit(50)
      .exec();
  }

  // CQRS Unordered Accumulator Handler
  async processEvent(envelope: any) {
    const { eventType, aggregateId, timestamp, payload } = envelope;
    const orderId = payload?.orderId || aggregateId;

    if (!orderId) {
      this.logger.warn(`Received event without orderId: ${eventType}`);
      return;
    }

    this.logger.log(`Projecting event ${eventType} for order ${orderId}`);

    const timelineEntry = {
      event: eventType,
      timestamp: timestamp || new Date().toISOString(),
      payload,
    };

    let updateDoc: any = {
      $addToSet: { timeline: timelineEntry },
      $set: { lastUpdated: new Date() },
    };

    switch (eventType) {
      case 'OrderCreatedEvent':
      case 'ReserveInventoryCommand':
        updateDoc.$set = {
          ...updateDoc.$set,
          _id: orderId,
          customerId: payload.customerId,
          totalAmount: payload.totalAmount,
          currency: payload.currency || 'USD',
          items: payload.items || [],
          status: 'INVENTORY_RESERVING',
        };
        break;

      case 'InventoryReservedEvent':
        updateDoc.$set = {
          ...updateDoc.$set,
          'inventory.status': 'RESERVED',
          'inventory.reservationId': payload.reservationId,
          'inventory.warehouseId': payload.warehouseId,
          'inventory.updatedAt': new Date(timestamp),
          status: 'PAYMENT_PENDING',
        };
        break;

      case 'InventoryReservationFailedEvent':
        updateDoc.$set = {
          ...updateDoc.$set,
          'inventory.status': 'FAILED',
          'inventory.reason': payload.reason,
          'inventory.updatedAt': new Date(timestamp),
          status: 'CANCELLED',
        };
        break;

      case 'PaymentCapturedEvent':
        updateDoc.$set = {
          ...updateDoc.$set,
          'payment.status': 'SUCCESS',
          'payment.transactionId': payload.transactionId,
          'payment.amount': payload.amount,
          'payment.updatedAt': new Date(timestamp),
          status: 'PAYMENT_SUCCESS',
        };
        break;

      case 'PaymentFailedEvent':
        updateDoc.$set = {
          ...updateDoc.$set,
          'payment.status': 'FAILED',
          'payment.failureReason': payload.failureReason,
          'payment.updatedAt': new Date(timestamp),
          status: 'COMPENSATING_INVENTORY',
        };
        break;

      case 'InventoryReleasedEvent':
        updateDoc.$set = {
          ...updateDoc.$set,
          'inventory.status': 'RELEASED',
          'inventory.updatedAt': new Date(timestamp),
          status: 'CANCELLED',
        };
        break;

      case 'OrderConfirmedEvent':
        updateDoc.$set = {
          ...updateDoc.$set,
          status: 'CONFIRMED',
        };
        break;

      case 'OrderCancelledEvent':
        updateDoc.$set = {
          ...updateDoc.$set,
          status: 'CANCELLED',
        };
        break;

      default:
        this.logger.log(`Accumulated general event: ${eventType}`);
        break;
    }

    const updated = await this.orderDashboardModel.findByIdAndUpdate(
      orderId,
      updateDoc,
      { upsert: true, new: true },
    );

    // Broadcast live event to visualizer frontend
    this.broadcastLiveEvent('ORDER_UPDATED', {
      orderId,
      eventType,
      doc: updated,
    });

    return updated;
  }
}
