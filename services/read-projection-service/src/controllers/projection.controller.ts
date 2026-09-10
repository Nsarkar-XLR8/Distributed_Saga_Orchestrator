import {
  Controller,
  Get,
  Post,
  Param,
  Body,
  Sse,
  NotFoundException,
} from '@nestjs/common';
import { Observable, map } from 'rxjs';
import { ProjectionService } from '../services/projection.service';

@Controller('api/v1')
export class ProjectionController {
  constructor(private readonly projectionService: ProjectionService) {}

  @Get('orders/:id/dashboard')
  async getOrderDashboard(@Param('id') id: string) {
    const order = await this.projectionService.getOrderDashboard(id);
    if (!order) {
      throw new NotFoundException(`Order projection for ${id} not found`);
    }
    return order;
  }

  @Get('orders/dashboard')
  async getAllOrdersDashboard() {
    return this.projectionService.getAllOrders();
  }

  @Sse('stream/events')
  streamEvents(): Observable<MessageEvent> {
    return this.projectionService.getEventStream().pipe(
      map((event) => ({
        data: JSON.stringify(event),
      } as MessageEvent)),
    );
  }

  @Post('events/ingest')
  async ingestEventDirectly(@Body() envelope: any) {
    return this.projectionService.processEvent(envelope);
  }
}
