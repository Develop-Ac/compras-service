// src/compras/cotacao/cotacao.repository.ts
import { Injectable } from '@nestjs/common';
import { PrismaService } from '../../prisma/prisma.service';
import { Prisma } from '@prisma/client';

@Injectable()
export class CotacaoRepository {
  constructor(private readonly prisma: PrismaService) {}

  async upsertCotacaoWithItems(
    empresa: number,
    pedido_cotacao: number,
    itensLower: Array<{
      pedido_cotacao: number;
      emissao: Date | null;
      pro_codigo: number;
      pro_descricao: string;
      mar_descricao: string | null;
      referencia: string | null;
      unidade: string | null;
      quantidade: number;
      dt_ultima_compra: Date | null;
    }>,
  ) {
    await this.prisma.$transaction(async (tx) => {
      await tx.com_cotacao.upsert({
        where: { pedido_cotacao },
        create: { empresa, pedido_cotacao },
        update: { empresa },
      });

      await tx.com_cotacao_itens.deleteMany({ where: { pedido_cotacao } });

      if (itensLower.length > 0) {
        await tx.com_cotacao_itens.createMany({ data: itensLower });
      }
    });
  }

  getCotacaoHeader(pedido: number) {
    return this.prisma.com_cotacao.findUnique({
      where: { pedido_cotacao: pedido },
      select: { empresa: true, pedido_cotacao: true },
    });
  }

  listItensByPedido(pedido: number) {
    return this.prisma.com_cotacao_itens.findMany({
      where: { pedido_cotacao: pedido },
      orderBy: { pro_codigo: 'asc' },
    });
  }

  countCotacao(where: Prisma.com_cotacaoWhereInput) {
    return this.prisma.com_cotacao.count({ where });
  }

  listHeaders(where: Prisma.com_cotacaoWhereInput, page: number, pageSize: number) {
    return this.prisma.com_cotacao.findMany({
      where,
      orderBy: { pedido_cotacao: 'desc' },
      skip: (page - 1) * pageSize,
      take: pageSize,
      select: { id: true, empresa: true, pedido_cotacao: true },
    });
  }

  groupItemCounts(pedidos: number[]) {
    if (!pedidos.length) return Promise.resolve([] as any[]);
    return this.prisma.com_cotacao_itens.groupBy({
      by: ['pedido_cotacao'],
      where: { pedido_cotacao: { in: pedidos } },
      _count: { _all: true },
    });
  }

  listItensForPedidos(pedidos: number[]) {
    if (!pedidos.length) return Promise.resolve([] as any[]);
    return this.prisma.com_cotacao_itens.findMany({
      where: { pedido_cotacao: { in: pedidos } },
      orderBy: [{ pedido_cotacao: 'desc' }, { pro_codigo: 'asc' }],
    });
  }

  findByPedidoCotacao(pedidoCotacao: number) {
    return this.prisma.com_cotacao.findUnique({
      where: { pedido_cotacao: pedidoCotacao },
      include: {
        com_cotacao_itens: true,
      },
    });
  }

  async delete(pedidoCotacao: number) {
    await this.prisma.$transaction(async (tx) => {
      await tx.com_cotacao_itens.deleteMany({ 
        where: { pedido_cotacao: pedidoCotacao } 
      });
      await tx.com_cotacao.delete({ 
        where: { pedido_cotacao: pedidoCotacao } 
      });
    });
    
    return { message: 'Cotação deletada com sucesso' };
  }
}
