import { Controller, Get, Put, Body } from '@nestjs/common';
import { kanbanService } from './kanban.service';
import { ApiTags } from '@nestjs/swagger';

@ApiTags('Compras - Kanban')
@Controller('kanban')
export class kanbanController {
  constructor(private readonly service: kanbanService) {}

  @Get()
  async getKanban() {
    return await this.service.getKanban();
  }

  @Put()
  async updateKanban(@Body() data: any) {
    return await this.service.updateKanban(data);
  }
}
 