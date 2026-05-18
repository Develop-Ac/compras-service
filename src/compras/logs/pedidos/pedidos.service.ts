import { Injectable } from '@nestjs/common';
import { PedidosLogsRepository } from './pedidos.repository';
import { PrismaService } from 'src/prisma/prisma.service';

@Injectable()
export class PedidosLogsService {
  constructor(
    private readonly pedidosLogsRepository: PedidosLogsRepository,
    private readonly prisma: PrismaService,
  ) {}

  async getGerencialLogsByPedido(pedido: string) {
    const logs = await this.pedidosLogsRepository.findDetalhesPedidoLogsByPedido(pedido);
    return this.mapLogs(logs);
  }

  private async mapLogs(logs: { usuario: string; descricao: string; created_at: Date | null }[]) {
    const usuarioIds = logs.map((log) => log.usuario);
    const usuarios = await this.prisma.sis_usuarios.findMany({
      where: { id: { in: usuarioIds } },
      select: { id: true, nome: true },
    });
    const usuarioMap = Object.fromEntries(usuarios.map(u => [u.id, u.nome]));
    return logs.map((log) => ({
      usuario: usuarioMap[log.usuario] || log.usuario,
      descricao: log.descricao,
      created_at: this.formatDate(log.created_at),
    }));
  }

  private formatDate(date: Date | string | null): string {
    if (!date) return '';
    const d = new Date(date);
    const pad = (n: number) => n.toString().padStart(2, '0');
    return `${pad(d.getDate())}/${pad(d.getMonth() + 1)}/${d.getFullYear()}  ${pad(d.getHours())}:${pad(d.getMinutes())}`;
  }
}
