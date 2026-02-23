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
      console.log('Updating kanban with id:', id, 'and data:', data);
      return this.prisma.com_kanban.update({
        where: { id },
        data: { data: data.data }
      });
  }

  async create(data: any) {
    // Gera um id automaticamente (cuid) e preenche updatedAt
    const cuid = await import('cuid');
    return this.prisma.com_kanban.create({
      data: {
        id: cuid.default(),
        data: data.data,
        updatedAt: new Date()
      }
    });
  }
}
