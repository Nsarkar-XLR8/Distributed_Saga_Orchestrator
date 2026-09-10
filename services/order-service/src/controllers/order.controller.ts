import {
  Controller,
  Post,
  Get,
  Body,
  Headers,
  Param,
  Res,
  HttpStatus,
  ValidationPipe,
  UsePipes,
} from '@nestjs/common';
import { Response } from 'express';
import { OrderService } from '../services/order.service';
import { CreateOrderDto } from '../dto/create-order.dto';

@Controller('api/v1/orders')
export class OrderController {
  constructor(private readonly orderService: OrderService) {}

  @Post()
  @UsePipes(new ValidationPipe({ transform: true, whitelist: true }))
  async createOrder(
    @Headers('Idempotency-Key') idempotencyKey: string,
    @Body() dto: CreateOrderDto,
    @Res() res: Response,
  ) {
    const result = await this.orderService.createOrder(idempotencyKey, dto);
    return res.status(result.statusCode).json(result.data);
  }

  @Get('outbox')
  async getOutbox() {
    return this.orderService.getOutboxEvents();
  }

  @Get(':id')
  async getOrder(@Param('id') id: string) {
    return this.orderService.getOrderById(id);
  }

  @Get()
  async getOrders() {
    return this.orderService.getAllOrders();
  }
}
