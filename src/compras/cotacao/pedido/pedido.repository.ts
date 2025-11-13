// src/pedido/repositories/pedido.repository.ts
import { Injectable } from '@nestjs/common';
import { Prisma, PrismaClient } from '@prisma/client';
import { PrismaService } from '../../../prisma/prisma.service';

type Tx = PrismaService | Prisma.TransactionClient;

@Injectable()
export class PedidoRepository {
  constructor(private prisma: PrismaService) {}

  /** Retorna pedidos com contagem e itens (para listagem leve) */
  async findAllWithLightItens() {
    return this.prisma.com_pedido.findMany({
      orderBy: { created_at: 'desc' },
      include: {
        _count: { select: { itens: true } },
        itens: { select: { quantidade: true, valor_unitario: true } },
      },
    });
  }

  /** Busca um pedido por id (para PDF) */
  async findByIdWithItens(id: string) {
    return this.prisma.com_pedido.findUnique({
      where: { id },
      include: { itens: true },
    });
  }

  /** Upsert do cabeçalho por (pedido_cotacao, for_codigo) dentro de TX */
  async upsertPedidoByCotacaoFornecedor(
    tx: Tx,
    pedido_cotacao: number,
    for_codigo: number,
  ) {
    return tx.com_pedido.upsert({
      where: { pedido_cotacao_for_codigo: { pedido_cotacao, for_codigo } },
      create: { pedido_cotacao, for_codigo },
      update: {},
    });
  }

  /** Apaga itens de um pedido dentro de TX */
  async deleteItensByPedidoId(tx: Tx, pedido_id: string) {
    await tx.com_pedido_itens.deleteMany({ where: { pedido_id } });
  }

  /** Cria itens em lote dentro de TX */
  async createManyItens(tx: Tx, data: Prisma.com_pedido_itensCreateManyInput[]) {
    if (!data?.length) return;
    await tx.com_pedido_itens.createMany({ data });
  }

  /** Executa uma transação do Prisma */
  async transaction<T>(fn: (tx: Prisma.TransactionClient) => Promise<T>) {
    return this.prisma.$transaction(fn);
  }
}
