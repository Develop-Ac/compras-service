import { Module } from '@nestjs/common';
import { kanbanService } from './kanban.service';
import { kanbanController } from './kanban.controller';
import { kanbanRepository } from './kanban.repository';
import { PrismaModule } from '../../prisma/prisma.module';

@Module({
  imports: [PrismaModule],
  controllers: [kanbanController],
  providers: [kanbanService, kanbanRepository],
  exports: [kanbanService, kanbanRepository],
})
export class kanbanModule {}
