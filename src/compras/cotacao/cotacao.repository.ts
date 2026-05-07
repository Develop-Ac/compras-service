// Os métodos getCotacaoHeader e listItensByPedido já retornam os dados necessários para o endpoint customizado.
// src/compras/cotacao/cotacao.repository.ts
import { Injectable } from '@nestjs/common';
import { PrismaService } from '../../prisma/prisma.service';
import { Prisma } from '@prisma/client';
import { OpenQueryService } from 'src/shared/database/openquery/openquery.service';

@Injectable()
export class CotacaoRepository {
  constructor(
    private readonly prisma: PrismaService,
    private readonly mssql: OpenQueryService
  ) {}

  /** Escapa aspas simples para o literal T-SQL do OPENQUERY */
  private fbLiteral(sql: string) {
    return sql.replace(/'/g, "''");
  }

  async getInfoItens(pro_codigo: number) {
    const fbSql = `
      SELECT *
      FROM produtos pro
      WHERE pro.pro_codigo = ${pro_codigo}
    `;

    const tsql = `SELECT * FROM OPENQUERY([CONSULTA], '${this.fbLiteral(fbSql)}')`;

    try {
      const rows = await this.mssql.query<any>(tsql, {}, { timeout: 60_000, allowZeroRows: true });
      const item = rows.find(row => String(row.PRO_CODIGO) === String(pro_codigo));
      console.log(item);
      return {
        PRO_CODIGO: item?.PRO_CODIGO,
        PRO_DESCRICAO: item?.PRO_DESCRICAO,
        MAR_DESCRICAO: item?.MAR_DESCRICAO,
        UNIDADE: item?.UNIDADE,
        REFERENCIA: item?.REFERENCIA,
      }
    } catch (error) {
      console.error('Erro ao consultar informações do item:', error);
      return {
        PRO_CODIGO: null,
        PRO_DESCRICAO: null,
        MAR_DESCRICAO: null,
        UNIDADE: null,
        REFERENCIA: null,
      }
    }
  }

  async insertNewItemCotacao(
    PRO_CODIGO: number,
    PRO_DESCRICAO: string,
    MAR_DESCRICAO: string | null,
    UNIDADE: string | null,
    REFERENCIA: string | null,
    pedido_cotacao: string,
    quantidade: number,
  ) {

    // Verifica se a cotação existe antes de inserir o item
    const cotacao = await this.prisma.com_cotacao.findUnique({
      where: { pedido_cotacao: Number(pedido_cotacao) },
    });

    return this.prisma.com_cotacao_itens.create({
      data: {
        pro_codigo: PRO_CODIGO,
        pro_descricao: PRO_DESCRICAO,
        mar_descricao: MAR_DESCRICAO,
        unidade: UNIDADE,
        referencia: REFERENCIA,
        quantidade: quantidade,
        com_cotacao: { connect: { id: String(cotacao?.id) } },
      }
    });
  }

  async upsertCotacaoWithItems(
    empresa: number,
    pedido_cotacao: number,
    dias_compra: number,
    itensLower: Array<{
      pedido_cotacao: number;
      emissao: Date | null;
      pro_codigo: number;
      pro_descricao: string;
      mar_descricao: string | null;
      referencia: string | null;
      unidade: string | null;
      quantidade: number;
      qtd_sugerida: number;
      dt_ultima_compra: Date | null;
    }>,
  ) {
    await this.prisma.$transaction(async (tx) => {
      await tx.com_cotacao.upsert({
        where: { pedido_cotacao },
        create: { empresa, pedido_cotacao, dias_compra },
        update: { empresa, dias_compra },
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
      select: { empresa: true, pedido_cotacao: true, dias_compra: true },
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
      orderBy: [
        { created_at: 'desc' },
        { pedido_cotacao: 'desc' }
      ],
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

    async deleteCotacaoByPedido(pedidoCotacao: number) {
      await this.prisma.$transaction(async (tx) => {
        await tx.com_cotacao.delete({ 
            where: { pedido_cotacao: pedidoCotacao } 
          })
    
        await tx.com_cotacao_itens.deleteMany({ 
            where: { pedido_cotacao: pedidoCotacao } 
          });
      })
    
    return { message: 'Cotação deletada com sucesso' };
  }

  async getNextIndice(): Promise<number> {
    const result = await this.prisma.com_cotacao.findFirst({
      orderBy: { indice: 'desc' },
      select: { indice: true },
    });
    return (result?.indice ?? 0) + 1;
  }  
}
