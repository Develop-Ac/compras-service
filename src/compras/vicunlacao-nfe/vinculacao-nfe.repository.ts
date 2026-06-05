import { Injectable, Logger } from '@nestjs/common';
import { OpenQueryService as MssqlOpenQuery } from '../../shared/database/openquery/openquery.service';
import { PrismaService } from '../../prisma/prisma.service';

const LINKED_SERVER = 'CONSULTA';

/** Linha bruta da NFE_DISTRIBUICAO + NF_ENTRADA_XML (Firebird via OPENQUERY) */
export type NfeXmlRow = {
  EMPRESA: number | null;
  CHAVE_NFE: string | null;
  NOME_EMITENTE: string | null;
  DATA_EMISSAO: Date | string | null;
  XML_COMPLETO: string | Buffer | null;
};

/** Item de cotação vindo do Firebird (PEDIDOS_COTACOES + ITENS + PRODUTOS + MARCAS) */
export type CotacaoItemRow = {
  pedido_cotacao: number | null;
  emissao: Date | string | null;
  pro_codigo: number | string | null;
  pro_descricao: string | null;
  mar_descricao: string | null;
  referencia: string | null;
  ref_fabricante: string | null;
  ref_fornecedor: string | null;
  unidade: string | null;
  quantidade: number | string | null;
  dt_ultima_compra: Date | string | null;
};

/**
 * Repository de vinculação NFe: queries OPENQUERY ao Firebird (NF-e e cotação)
 * e leitura de com_cotacao_itens_for no Postgres (Prisma).
 */
@Injectable()
export class VinculacaoNfeRepository {
  private readonly logger = new Logger(VinculacaoNfeRepository.name);

  constructor(
    private readonly mssql: MssqlOpenQuery,
    private readonly prisma: PrismaService,
  ) {}

  /** Escapa aspas simples para o literal T-SQL do OPENQUERY */
  private fbLiteral(sql: string): string {
    return sql.replace(/'/g, "''");
  }

  /**
   * Busca o XML completo de uma NF-e pela chave de acesso.
   */
  async findXmlByChave(chaveNfe: string, empresa = 1): Promise<NfeXmlRow | null> {
    const fbSql = `
      SELECT
        NFD.EMPRESA,
        NFD.CHAVE_NFE,
        NFD.NOME_EMITENTE,
        NFD.DATA_EMISSAO,
        X.XML_COMPLETO
      FROM NFE_DISTRIBUICAO NFD
      LEFT JOIN NF_ENTRADA_XML X
             ON X.EMPRESA   = NFD.EMPRESA
            AND X.CHAVE_NFE = NFD.CHAVE_NFE
      WHERE NFD.SITUACAO_NFE = 1
        AND NFD.EMPRESA      = ${empresa}
        AND NFD.CHAVE_NFE    = '${this.fbLiteral(chaveNfe)}'
    `;

    const tsql = `SELECT * FROM OPENQUERY([${LINKED_SERVER}], '${this.fbLiteral(fbSql)}')`;

    try {
      const rows = await this.mssql.query<NfeXmlRow>(tsql, {}, { timeout: 120_000, allowZeroRows: true });
      return rows[0] || null;
    } catch (err: any) {
      this.logger.error(`[OPENQUERY nfe] ${err?.message || err}`);
      throw err;
    }
  }

  /**
   * Busca itens de um pedido de cotação no Firebird.
   */
  async findCotacaoItens(pedido: number, empresa = 1): Promise<CotacaoItemRow[]> {
    const fbSql = `
      SELECT
          orc.pedido_cotacao,
          orc.emissao,
          iorc.pro_codigo,
          pro.pro_descricao,
          mar.mar_descricao,
          pro.referencia,
          pro.ref_fabricante,
          pro.ref_fornecedor,
          pro.unidade,
          iorc.quantidade,
          pro.dt_ultima_compra
      FROM PEDIDOS_COTACOES orc
      LEFT JOIN PEDIDOS_COTACOES_ITENS iorc
             ON iorc.empresa = orc.empresa
            AND iorc.pedido_cotacao = orc.pedido_cotacao
      LEFT JOIN PRODUTOS pro
             ON pro.empresa = orc.empresa
            AND pro.pro_codigo = iorc.pro_codigo
      LEFT JOIN MARCAS mar
             ON mar.empresa = orc.empresa
            AND mar.mar_codigo = pro.mar_codigo
      WHERE orc.empresa = ${empresa}
        AND orc.pedido_cotacao = ${pedido}
    `;

    const tsql = `SELECT * FROM OPENQUERY([${LINKED_SERVER}], '${this.fbLiteral(fbSql)}')`;

    try {
      const rows = await this.mssql.query<CotacaoItemRow>(tsql, {}, { timeout: 120_000, allowZeroRows: true });
      return rows || [];
    } catch (err: any) {
      this.logger.error(`[OPENQUERY cotacao] ${err?.message || err}`);
      throw err;
    }
  }

  /**
   * Busca itens de cotação por fornecedor no Postgres (com_cotacao_itens_for).
   * O campo ref_fornecedor entra como equivalente ao da cotação Firebird.
   */
  async findCotacaoItensFor(pedido: number) {
    return this.prisma.com_cotacao_itens_for.findMany({
      where: { pedido_cotacao: pedido },
      select: {
        pedido_cotacao: true,
        for_codigo: true,
        pro_codigo: true,
        pro_descricao: true,
        mar_descricao: true,
        referencia: true,
        ref_fornecedor: true,
        unidade: true,
        quantidade: true,
        valor_unitario: true,
        emissao: true,
      },
    });
  }

  /** Retorna os ids dos pedidos (com_pedido) de uma cotação. */
  async findPedidoIds(pedido: number): Promise<string[]> {
    const rows = await this.prisma.com_pedido.findMany({
      where: { pedido_cotacao: pedido },
      select: { id: true },
    });
    return rows.map((r) => r.id);
  }

  /**
   * Busca os itens dos pedidos (com_pedido_itens) ordenados do mais recente
   * para o mais antigo (emissao DESC, id DESC).
   */
  async findPedidoItens(pedidoIds: string[]) {
    if (!pedidoIds.length) return [];
    return this.prisma.com_pedido_itens.findMany({
      where: { pedido_id: { in: pedidoIds } },
      select: {
        id: true,
        pedido_id: true,
        pro_codigo: true,
        pro_descricao: true,
        mar_descricao: true,
        referencia: true,
        unidade: true,
        emissao: true,
        valor_unitario: true,
        quantidade: true,
        for_codigo: true,
      },
      orderBy: [{ emissao: 'desc' }, { id: 'desc' }],
    });
  }
}
