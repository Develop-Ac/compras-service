import { Injectable } from '@nestjs/common';
import { PrismaService } from 'src/prisma/prisma.service';

@Injectable()
export class PedidosLogsRepository {
  constructor(private readonly prisma: PrismaService) {}

  async findDetalhesPedidoLogsByPedido(pedido: string) {
    return this.prisma.sis_log.findMany({
      where: {
        tela: 'Detalhes do Pedido',
        descricao: { contains: `pedido ${pedido}` },
      },
      orderBy: { created_at: 'desc' },
      select: {
        usuario: true,
        descricao: true,
        created_at: true,
      },
    });
  }
}
