// Os métodos getCotacaoHeader e listItensByPedido já retornam os dados necessários para o endpoint customizado.
// src/compras/cotacao/cotacao.repository.ts
import { Injectable } from '@nestjs/common';
import { PrismaService } from '../../prisma/prisma.service';
import { Prisma } from '@prisma/client';
import { OpenQueryService } from 'src/shared/database/openquery/openquery.service';
import { ErpApiService } from '../../shared/erp-api/erp-api.service';

// Cotação e pedido são gerenciais. PRODUTOS é chaveado por (EMPRESA, PRO_CODIGO)
// e as empresas espelham o mesmo código — sem o filtro, a leitura devolve a
// linha de uma empresa arbitrária.
const EMPRESA = 3;

@Injectable()
export class CotacaoRepository {
  constructor(
    private readonly prisma: PrismaService,
    private readonly mssql: OpenQueryService,
    private readonly erp: ErpApiService,
  ) {}

  /** Escapa aspas simples para o literal T-SQL do OPENQUERY */
  private fbLiteral(sql: string) {
    return sql.replace(/'/g, "''");
  }

  async getInfoItens(pro_codigo: number) {
    try {
      const item = await this.erp.comFallback<Record<string, any> | undefined>(
        async () => {
          const rows = await this.erp.produtosPorCodigos([pro_codigo], EMPRESA, [
            'PRO_CODIGO',
            'PRO_DESCRICAO',
            'UNIDADE',
            'REFERENCIA',
            'marca.MAR_DESCRICAO',
          ]);
          return rows[0];
        },
        async () => {
          const fbSql = `
            SELECT pro.pro_codigo, pro.pro_descricao, pro.unidade, pro.referencia,
                   mar.mar_descricao
            FROM produtos pro
            LEFT JOIN marcas mar
                   ON mar.empresa = pro.empresa
                  AND mar.mar_codigo = pro.mar_codigo
            WHERE pro.empresa = ${EMPRESA}
              AND pro.pro_codigo = ${Math.trunc(Number(pro_codigo))}
          `;
          const tsql = `SELECT * FROM OPENQUERY([CONSULTA], '${this.fbLiteral(fbSql)}')`;
          const rows = await this.mssql.query<any>(tsql, {}, { timeout: 60_000, allowZeroRows: true });
          return rows[0];
        },
      );

      return {
        PRO_CODIGO: item?.PRO_CODIGO ?? null,
        PRO_DESCRICAO: item?.PRO_DESCRICAO ?? null,
        MAR_DESCRICAO: item?.MAR_DESCRICAO ?? null,
        UNIDADE: item?.UNIDADE ?? null,
        REFERENCIA: item?.REFERENCIA ?? null,
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

  /**
   * Busca, em UMA consulta ao SQL Server BI, os dados de vários produtos de uma
   * vez, lendo direto da Stage `Stage_Produtos` (ETL ~1 min do ERP) — sem ir ao
   * Firebird via OPENQUERY. Usado para completar `referencia`/`unidade` que
   * chegam vazias das telas intranet ("Comprar Agora"/Análise). A Stage tem a
   * coluna `REFERENCIA` (mesma do ERP, e mais completa) e é `EMPRESA=3`.
   */
  async getProdutosInfo(
    empresa: number,
    codigos: number[],
  ): Promise<Map<number, { referencia: string | null; unidade: string | null }>> {
    const ids = Array.from(new Set(codigos.filter((c) => Number.isFinite(c)))).map((c) => Math.trunc(c));
    if (!ids.length) return new Map();

    const inList = ids.join(',');
    const tsql = `
      SELECT PRO_CODIGO, REFERENCIA, UNIDADE
      FROM Stage_Produtos
      WHERE EMPRESA = ${Math.trunc(empresa)}
        AND PRO_CODIGO IN (${inList})
    `;

    const rows = await this.mssql.query<any>(tsql, {}, { timeout: 30_000, allowZeroRows: true });

    const map = new Map<number, { referencia: string | null; unidade: string | null }>();
    for (const r of rows || []) {
      const code = Number(r.PRO_CODIGO ?? r.pro_codigo);
      if (!Number.isFinite(code)) continue;
      map.set(code, {
        referencia: r.REFERENCIA ?? r.referencia ?? null,
        unidade: r.UNIDADE ?? r.unidade ?? null,
      });
    }
    return map;
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

  /**
   * Próximo número de COTAÇÃO gerado NA INTRANET.
   *
   * As cotações criadas aqui (Comprar Agora / Análise / Produtos) vivem só na
   * intranet por enquanto e NÃO podem colidir com os números que o ERP mãe gera
   * na sua própria sequência (hoje na casa dos 4 mil, crescendo). Por isso a
   * intranet numera A PARTIR DE INTRANET_COTACAO_BASE (100.000): a primeira
   * cotação é 100.000, depois 100.001, 100.002… O ERP levaria ~2 séculos para
   * chegar aos 100 mil. O frontend exibe esses números como "I-100000".
   *
   * Quando existir a API do ERP, ele passará a gerar o número real e devolvê-lo;
   * até lá esta faixa mantém os dois espaços de numeração separados.
   */
  async getNextIndice(): Promise<number> {
    // Robusto ao formato do env: "100000", "100.000" e "100 000" viram 100000
    // (Number("100.000") sozinho daria 100 — ponto é decimal em JS).
    const base = Number(String(process.env.INTRANET_COTACAO_BASE ?? '').replace(/[^\d]/g, '')) || 100_000;
    const result = await this.prisma.com_cotacao.findFirst({
      where: { pedido_cotacao: { gte: base } },
      orderBy: { pedido_cotacao: 'desc' },
      select: { pedido_cotacao: true },
    });
    // primeira cotação da intranet = base (100.000); depois incrementa de 1 em 1
    return result?.pedido_cotacao != null ? result.pedido_cotacao + 1 : base;
  }
}
