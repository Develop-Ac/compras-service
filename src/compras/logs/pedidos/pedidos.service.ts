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

    // Extrai todos os pedidoIds únicos encontrados nas descrições
    const pedidoIdRegex = /pedido\s+([a-z0-9]+)/gi;
    const pedidoIdSet = new Set<string>();
    for (const log of logs) {
      const matches = [...log.descricao.matchAll(pedidoIdRegex)];
      for (const match of matches) {
        pedidoIdSet.add(match[1]);
      }
    }

    const pedidoIds = [...pedidoIdSet];
    const pedidos = pedidoIds.length
      ? await this.prisma.com_pedido.findMany({
          where: { id: { in: pedidoIds } },
          select: { id: true, pedido_cotacao: true },
        })
      : [];
    const pedidoMap = Object.fromEntries(pedidos.map(p => [p.id, p.pedido_cotacao]));

    return logs.map((log) => {
      const descricao = log.descricao.replace(pedidoIdRegex, (_, id) => {
        const numero = pedidoMap[id];
        return `pedido ${numero !== undefined ? numero : id}`;
      });
      return {
        usuario: usuarioMap[log.usuario] || log.usuario,
        descricao,
        created_at: this.formatDate(log.created_at),
      };
    });
  }

  private formatDate(date: Date | string | null): string {
    if (!date) return '';
    const d = new Date(date);
    const pad = (n: number) => n.toString().padStart(2, '0');
    return `${pad(d.getDate())}/${pad(d.getMonth() + 1)}/${d.getFullYear()}  ${pad(d.getHours())}:${pad(d.getMinutes())}`;
  }
}
