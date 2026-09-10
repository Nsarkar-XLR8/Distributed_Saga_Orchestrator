import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { Document } from 'mongoose';

export type OrderDashboardDocument = OrderDashboard & Document;

@Schema({ _id: false })
export class OrderItemEmbed {
  @Prop({ required: true })
  productId: string;

  @Prop({ required: true })
  quantity: number;

  @Prop()
  unitPrice?: number;
}

@Schema({ _id: false })
export class InventoryEmbed {
  @Prop()
  status: string; // RESERVED, RELEASED, FAILED

  @Prop()
  reservationId?: string;

  @Prop()
  warehouseId?: string;

  @Prop()
  reason?: string;

  @Prop()
  updatedAt: Date;
}

@Schema({ _id: false })
export class PaymentEmbed {
  @Prop()
  status: string; // SUCCESS, FAILED

  @Prop()
  transactionId?: string;

  @Prop()
  amount?: number;

  @Prop()
  failureReason?: string;

  @Prop()
  updatedAt: Date;
}

@Schema({ _id: false })
export class TimelineEventEmbed {
  @Prop({ required: true })
  event: string;

  @Prop({ required: true })
  timestamp: string;

  @Prop({ type: Object })
  payload?: any;
}

@Schema({ collection: 'orders_dashboard', timestamps: true })
export class OrderDashboard {
  @Prop({ required: true, type: String })
  _id: string;

  @Prop()
  customerId: string;

  @Prop({ default: 'PENDING' })
  status: string;

  @Prop()
  totalAmount: number;

  @Prop({ default: 'USD' })
  currency: string;

  @Prop({ type: [OrderItemEmbed], default: [] })
  items: OrderItemEmbed[];

  @Prop({ type: InventoryEmbed })
  inventory?: InventoryEmbed;

  @Prop({ type: PaymentEmbed })
  payment?: PaymentEmbed;

  @Prop({ type: [TimelineEventEmbed], default: [] })
  timeline: TimelineEventEmbed[];

  @Prop({ default: Date.now })
  lastUpdated: Date;
}

export const OrderDashboardSchema = SchemaFactory.createForClass(OrderDashboard);
