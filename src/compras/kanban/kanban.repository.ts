import { Injectable } from '@nestjs/common';
import { PrismaService } from '../../prisma/prisma.service';

@Injectable()
export class kanbanRepository {
  constructor(private readonly prisma: PrismaService) {}

  async findFirst() {
    return this.prisma.com_kanban.findFirst();
  }

  async update(id: string, data: any) {
      // Envia apenas o campo 'data' para o Prisma
      return this.prisma.com_kanban.update({
        where: { id },
        data: { data: data.data }
      });
  }

  async create(data: any) {
      // Envia apenas o campo 'data' para o Prisma
      return this.prisma.com_kanban.create({
        data: { data: data.data }
      });
  }
}
