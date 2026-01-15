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

  /** Busca um pedido por id com todos os dados (para gerencial) */
  async findByIdGerencial(id: string) {
    return this.prisma.com_pedido.findUnique({
      where: { id },
      include: { 
        itens: {
          orderBy: { created_at: 'asc' }
        }
      },
    });
  }

  /**
   * Atualiza autorização de um item do pedido.
   * @param itemId ID do item
   * @param coluna 'carlos' | 'renato'
   * @param check boolean
   */
  async updateItemAutorizacao(itemId: string, coluna: 'carlos' | 'renato', check: boolean) {
      // Ajuste os nomes das colunas conforme seu schema
      const data: any = {};
      if (coluna === 'carlos') data.carlos = check;
      if (coluna === 'renato') data.renato = check;
  
      return this.prisma.com_pedido_itens.update({
        where: { id: itemId },
        data,
      });
    }
  
  // ...existing code...

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
  /** Busca um pedido e seus itens por id (para sincronização) */
  async findByIdWithAll(id: string) {
    const pedidos = await this.prisma.com_pedido.findMany({
      where: { pedido_cotacao: Number(id) },
    });
    if (!pedidos.length) return [];
    const pedidosWithItens = await Promise.all(
      pedidos.map(async (pedido) => {
        const itens = await this.prisma.com_pedido_itens.findMany({
          where: { pedido_id: pedido.id },
        });
        // Formata valor_unitario para padrão brasileiro
        const itensFormatados = itens.map((item) => ({
          ...item,
          valor_unitario: Number(item.valor_unitario).toLocaleString('pt-BR', {
            minimumFractionDigits: 2,
            maximumFractionDigits: 2,
          }),
        }));
        return { ...pedido, itens: itensFormatados };
      })
    );
    return pedidosWithItens;
  }
}
