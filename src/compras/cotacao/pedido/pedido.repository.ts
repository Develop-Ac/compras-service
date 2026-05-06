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
      include: {
      itens: {
        where: {
        OR: [
          { renato: true },
          { carlos: true },
        ],
        },
      },
      },
    });
  }

  async findByIdWithItensToAutorizar(id: string) {
    return this.prisma.com_pedido.findUnique({
      where: { id },
      include: {
        itens: true
      },
    });
  }

  /** Busca um pedido por id com todos os dados (para gerencial) */
  async findByIdGerencial(id: string) {
    const pedido = await this.prisma.com_pedido.findUnique({
      where: { id },
      include: { 
        itens: {
          orderBy: { created_at: 'asc' }
        }
      },
    });

    if (!pedido) return null;

    const dias_compra = await this.prisma.com_cotacao.findFirst({
      where: { pedido_cotacao: pedido.pedido_cotacao },
      select: { dias_compra: true },
    });

    return { ...pedido, dias_compra: dias_compra?.dias_compra ?? null };
  }

  async getMinMax(pro_codigo: number): Promise<{ min: number | null; max: number | null }> {
    const result = await this.prisma.com_fifo_completo.findFirst({
      where: { pro_codigo: String(pro_codigo) },
      select: {
        estoque_min_sugerido: true,
        estoque_max_sugerido: true,
      },
    });
    if (!result) return { min: null, max: null };
    // Ajuste os nomes dos campos conforme o seu schema
    return {
      min: result.estoque_min_sugerido ?? null,
      max: result.estoque_max_sugerido ?? null,
    };
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
    prazo: string,
  ) {
    return tx.com_pedido.upsert({
      where: { pedido_cotacao_for_codigo: { pedido_cotacao, for_codigo } },
      create: { pedido_cotacao, for_codigo, prazo },
      update: {  },
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

  /** Atualiza transportadora (nomeFrete e frete) de um pedido */
  async updateTransportadora(id: string, nomeFrete: string, frete: number) {
    return this.prisma.com_pedido.update({
      where: { id },
      data: { nomeFrete, frete },
    });
  }

  /** Busca um pedido e seus itens por id (para sincronização) */
  async findByIdWithAllForSincronizacao(id: string) {
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
  
  async findByIdWithAll(id: string) {
    const pedidos = await this.prisma.com_pedido.findMany({
      where: { pedido_cotacao: Number(id) },
    });
    if (!pedidos.length) return [];
    const pedidosWithItens = await Promise.all(
      pedidos.map(async (pedido) => {
        const itens = await this.prisma.com_pedido_itens.findMany({
          where: { pedido_id: pedido.id, renato: true, carlos: true },
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
