import { Injectable, Logger } from '@nestjs/common';
import { Prisma } from '@prisma/client';
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

    let row: NfeXmlRow | null = null;
    try {
      const rows = await this.mssql.query<NfeXmlRow>(tsql, {}, { timeout: 120_000, allowZeroRows: true });
      row = rows[0] || null;
    } catch (err: any) {
      this.logger.error(`[OPENQUERY nfe] ${err?.message || err}`);
      throw err;
    }

    // Caminho feliz: nota ainda na NFE_DISTRIBUICAO com XML.
    if (row?.XML_COMPLETO) return row;

    // Fallback: NF já lançada pode não estar mais na NFE_DISTRIBUICAO. O XML, porém,
    // permanece em NF_ENTRADA_XML — busca direto por lá (emitente/data podem vir nulos,
    // o service completa o emitente a partir de com_nfe_conciliacao).
    const fbSql2 = `
      SELECT
        X.EMPRESA,
        X.CHAVE_NFE,
        X.XML_COMPLETO
      FROM NF_ENTRADA_XML X
      WHERE X.EMPRESA   = ${empresa}
        AND X.CHAVE_NFE = '${this.fbLiteral(chaveNfe)}'
    `;
    const tsql2 = `SELECT * FROM OPENQUERY([${LINKED_SERVER}], '${this.fbLiteral(fbSql2)}')`;

    try {
      const rows2 = await this.mssql.query<NfeXmlRow>(tsql2, {}, { timeout: 120_000, allowZeroRows: true });
      const xmlRow = rows2[0];
      if (xmlRow?.XML_COMPLETO) {
        return {
          EMPRESA: xmlRow.EMPRESA ?? row?.EMPRESA ?? empresa,
          CHAVE_NFE: xmlRow.CHAVE_NFE ?? chaveNfe,
          NOME_EMITENTE: row?.NOME_EMITENTE ?? null,
          DATA_EMISSAO: row?.DATA_EMISSAO ?? null,
          XML_COMPLETO: xmlRow.XML_COMPLETO,
        };
      }
    } catch (err: any) {
      this.logger.error(`[OPENQUERY nfe-entrada-xml] ${err?.message || err}`);
    }

    return row;
  }

  /** Lê uma linha de conciliação (Postgres) pela chave — usado p/ completar emitente de NF lançada. */
  async findConciliacaoByChave(chaveNfe: string) {
    return this.prisma.com_nfe_conciliacao.findUnique({
      where: { chave_nfe: chaveNfe },
      select: { chave_nfe: true, emitente: true, status_erp: true, dt_entrada: true },
    });
  }

  /**
   * XML completo salvo no Postgres (com_nfe_conciliacao). É a fonte confiável dos
   * itens: o OPENQUERY do Firebird trunca XML_COMPLETO num teto fixo (~11 KB),
   * cortando NF-e maiores; aqui o XML está íntegro (gzip+base64).
   */
  async findConciliacaoXmlByChave(chaveNfe: string): Promise<string | null> {
    const row = await this.prisma.com_nfe_conciliacao.findUnique({
      where: { chave_nfe: chaveNfe },
      select: { xml_completo: true },
    });
    return row?.xml_completo ?? null;
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

  // ----------------------- Persistência do vínculo NF-e ----------------------

  /**
   * Upsert do cabeçalho do vínculo (por @@unique([pedido_id, chave_nfe])) e
   * substituição dos itens (deleteMany + createMany) numa única transação.
   * Retorna o cabeçalho salvo com a contagem de itens por tipo.
   */
  async salvarVinculo(
    cabecalho: {
      pedido_id: string;
      pedido_cotacao: number;
      for_codigo?: number | null;
      chave_nfe: string;
      emitente?: string | null;
      data_emissao?: Date | null;
      valor_total?: Prisma.Decimal | number | null;
      usuario?: string | null;
    },
    itens: Prisma.com_pedido_nfe_vinculo_itemCreateManyVinculoInput[],
    opcoes?: { confirmado?: boolean; origem?: string },
  ) {
    // Default: salvamento manual pelo usuário (confirmado=true, origem='manual').
    // O job de auto-vínculo passa { confirmado: false, origem: 'auto' } para criar sugestões.
    const confirmado = opcoes?.confirmado ?? true;
    const origem = opcoes?.origem ?? 'manual';

    return this.prisma.$transaction(async (tx) => {
      const vinculo = await tx.com_pedido_nfe_vinculo.upsert({
        where: {
          pedido_id_chave_nfe: {
            pedido_id: cabecalho.pedido_id,
            chave_nfe: cabecalho.chave_nfe,
          },
        },
        create: {
          pedido_id: cabecalho.pedido_id,
          pedido_cotacao: cabecalho.pedido_cotacao,
          for_codigo: cabecalho.for_codigo ?? null,
          chave_nfe: cabecalho.chave_nfe,
          emitente: cabecalho.emitente ?? null,
          data_emissao: cabecalho.data_emissao ?? null,
          valor_total: cabecalho.valor_total ?? null,
          usuario: cabecalho.usuario ?? null,
          confirmado,
          origem_vinculo: origem,
        },
        update: {
          pedido_cotacao: cabecalho.pedido_cotacao,
          for_codigo: cabecalho.for_codigo ?? null,
          emitente: cabecalho.emitente ?? null,
          data_emissao: cabecalho.data_emissao ?? null,
          valor_total: cabecalho.valor_total ?? null,
          usuario: cabecalho.usuario ?? null,
          confirmado,
          origem_vinculo: origem,
        },
      });

      await tx.com_pedido_nfe_vinculo_item.deleteMany({
        where: { vinculo_id: vinculo.id },
      });

      if (itens.length) {
        await tx.com_pedido_nfe_vinculo_item.createMany({
          data: itens.map((it) => ({ ...it, vinculo_id: vinculo.id })),
        });
      }

      const porTipo = await tx.com_pedido_nfe_vinculo_item.groupBy({
        by: ['tipo'],
        where: { vinculo_id: vinculo.id },
        _count: { _all: true },
      });

      return { vinculo, porTipo };
    });
  }

  // ------------------------- Auto-vínculo (sugestões) ------------------------

  /**
   * Pedidos "abertos" candidatos à varredura de auto-vínculo:
   * status em ('Em analise', 'Faturado parcialmente') e SEM vínculo confirmado.
   * (Cancelado / Entregue / 'Vínculo sugerido' ficam de fora pelo filtro de status.)
   */
  async findPedidosAbertosParaAutoVinculo(limite: number) {
    return this.prisma.com_pedido.findMany({
      where: {
        status: { in: ['Em analise', 'Faturado parcialmente'] },
        nfe_vinculos: { none: { confirmado: true } },
      },
      select: {
        id: true,
        pedido_cotacao: true,
        for_codigo: true,
        status: true,
        created_at: true,
      },
      orderBy: { created_at: 'asc' },
      take: limite,
    });
  }

  /** Conta quantos itens (com_pedido_itens) distintos por pro_codigo o pedido tem. */
  async countProCodigosDoPedido(pedidoId: string): Promise<number> {
    const rows = await this.prisma.com_pedido_itens.findMany({
      where: { pedido_id: pedidoId },
      select: { pro_codigo: true },
      distinct: ['pro_codigo'],
    });
    return rows.length;
  }

  /** Verifica se já existe QUALQUER vínculo (confirmado ou não) para o par pedido_id + chave_nfe. */
  async existeVinculoParaPar(pedidoId: string, chaveNfe: string): Promise<boolean> {
    const v = await this.prisma.com_pedido_nfe_vinculo.findUnique({
      where: { pedido_id_chave_nfe: { pedido_id: pedidoId, chave_nfe: chaveNfe } },
      select: { id: true },
    });
    return v != null;
  }

  /**
   * Seta o status do pedido para 'Vínculo sugerido', SEM rebaixar
   * 'Entregue' nem 'Cancelado'. Retorna o status final.
   */
  async marcarPedidoVinculoSugerido(pedidoId: string): Promise<string | null> {
    const pedido = await this.prisma.com_pedido.findUnique({
      where: { id: pedidoId },
      select: { status: true },
    });
    if (!pedido) return null;
    const atual = pedido.status ?? null;
    if (atual === 'Entregue' || atual === 'Cancelado' || atual === 'Vínculo sugerido') {
      return atual;
    }
    await this.prisma.com_pedido.update({
      where: { id: pedidoId },
      data: { status: 'Vínculo sugerido' },
    });
    return 'Vínculo sugerido';
  }

  /** Lista os cabeçalhos (sem itens) de um pedido + contagem de itens por tipo. */
  async findVinculosByPedido(pedidoId: string) {
    const cabecalhos = await this.prisma.com_pedido_nfe_vinculo.findMany({
      where: { pedido_id: pedidoId },
      select: {
        id: true,
        chave_nfe: true,
        emitente: true,
        data_emissao: true,
        valor_total: true,
        updated_at: true,
        confirmado: true,
        origem_vinculo: true,
      },
      orderBy: { updated_at: 'desc' },
    });

    if (!cabecalhos.length) return [];

    const ids = cabecalhos.map((c) => c.id);
    const totais = await this.prisma.com_pedido_nfe_vinculo_item.groupBy({
      by: ['vinculo_id', 'tipo'],
      where: { vinculo_id: { in: ids } },
      _count: { _all: true },
    });

    const mapa = new Map<string, Record<string, number>>();
    for (const t of totais) {
      const atual = mapa.get(t.vinculo_id) ?? {};
      atual[t.tipo] = t._count._all;
      mapa.set(t.vinculo_id, atual);
    }

    return cabecalhos.map((c) => ({ ...c, totais: mapa.get(c.id) ?? {} }));
  }

  /** Carrega um vínculo (cabeçalho + itens). */
  async findVinculoById(vinculoId: string) {
    return this.prisma.com_pedido_nfe_vinculo.findUnique({
      where: { id: vinculoId },
      include: { itens: true },
    });
  }

  /** Remove um vínculo (cascade apaga os itens). */
  async deleteVinculo(vinculoId: string) {
    return this.prisma.com_pedido_nfe_vinculo.delete({
      where: { id: vinculoId },
    });
  }

  // --------------------- Conferência por item (fechamento) -------------------

  /** Lê o pedido (id, status, data_recebimento) para a conferência. */
  async findPedidoParaConferencia(pedidoId: string) {
    return this.prisma.com_pedido.findUnique({
      where: { id: pedidoId },
      select: { id: true, status: true, data_recebimento: true },
    });
  }

  /**
   * Itens do pedido (com_pedido_itens) usados na conferência:
   * pro_codigo, pro_descricao, quantidade e valor_unitario.
   */
  async findItensDoPedido(pedidoId: string) {
    return this.prisma.com_pedido_itens.findMany({
      where: { pedido_id: pedidoId },
      select: {
        pro_codigo: true,
        pro_descricao: true,
        quantidade: true,
        valor_unitario: true,
      },
      orderBy: [{ pro_codigo: 'asc' }],
    });
  }

  /**
   * Itens dos vínculos CONFIRMADOS de um pedido, já com a chave_nfe do
   * cabeçalho. Usado para agregar quantidade/valor faturado e listar os itens
   * do XML sem pedido.
   */
  async findItensVinculadosConfirmados(pedidoId: string) {
    return this.prisma.com_pedido_nfe_vinculo_item.findMany({
      where: {
        vinculo: { pedido_id: pedidoId, confirmado: true },
      },
      select: {
        tipo: true,
        produto_xml: true,
        quantidade_xml: true,
        vuncom_xml: true,
        pro_codigo: true,
        vinculo: { select: { chave_nfe: true } },
      },
    });
  }

  // ----------------------- Status automático do pedido -----------------------

  /** Lê o pedido (id + status atual). */
  async findPedidoStatus(pedidoId: string) {
    return this.prisma.com_pedido.findUnique({
      where: { id: pedidoId },
      select: { id: true, status: true },
    });
  }

  /** pro_codigo distintos dos itens do pedido (com_pedido_itens). */
  async findProCodigosDoPedido(pedidoId: string): Promise<number[]> {
    const rows = await this.prisma.com_pedido_itens.findMany({
      where: { pedido_id: pedidoId },
      select: { pro_codigo: true },
    });
    return rows.map((r) => r.pro_codigo);
  }

  /**
   * pro_codigo dos itens tipo='vinculado' pertencentes a vínculos CONFIRMADOS
   * do pedido. Usado para calcular a cobertura de faturamento.
   */
  async findProCodigosVinculadosConfirmados(pedidoId: string): Promise<number[]> {
    const rows = await this.prisma.com_pedido_nfe_vinculo_item.findMany({
      where: {
        tipo: 'vinculado',
        pro_codigo: { not: null },
        vinculo: { pedido_id: pedidoId, confirmado: true },
      },
      select: { pro_codigo: true },
    });
    return rows
      .map((r) => r.pro_codigo)
      .filter((c): c is number => c != null);
  }

  /** chave_nfe distintas dos vínculos CONFIRMADOS de um pedido. */
  async findChavesVinculadasConfirmadas(pedidoId: string): Promise<string[]> {
    const rows = await this.prisma.com_pedido_nfe_vinculo.findMany({
      where: { pedido_id: pedidoId, confirmado: true },
      select: { chave_nfe: true },
      distinct: ['chave_nfe'],
    });
    return rows.map((r) => r.chave_nfe);
  }

  /** Conciliação (status_erp + dt_entrada) das chaves informadas, do Postgres. */
  async findConciliacaoByChaves(chaves: string[]) {
    if (!chaves.length) return [];
    return this.prisma.com_nfe_conciliacao.findMany({
      where: { chave_nfe: { in: chaves } },
      select: { chave_nfe: true, status_erp: true, dt_entrada: true },
    });
  }

  /** Marca um vínculo como confirmado. */
  async setVinculoConfirmado(vinculoId: string, confirmado: boolean) {
    return this.prisma.com_pedido_nfe_vinculo.update({
      where: { id: vinculoId },
      data: { confirmado },
    });
  }

  /** Atualiza o status de um pedido. */
  async updatePedidoStatus(pedidoId: string, status: string) {
    return this.prisma.com_pedido.update({
      where: { id: pedidoId },
      data: { status },
    });
  }

  /**
   * Volta o pedido ao estado pré-vinculação ('Finalizado'), limpando a
   * data_recebimento. Não altera pedidos 'Cancelado'. Retorna o status final.
   */
  async reverterPedidoParaFinalizado(pedidoId: string): Promise<string | null> {
    const pedido = await this.prisma.com_pedido.findUnique({
      where: { id: pedidoId },
      select: { status: true },
    });
    if (!pedido) return null;
    if (pedido.status === 'Cancelado') return pedido.status;
    await this.prisma.com_pedido.update({
      where: { id: pedidoId },
      data: { status: 'Finalizado', data_recebimento: null },
    });
    return 'Finalizado';
  }

  // ----------------------- NF lançada -> Entregue ----------------------------

  /**
   * pedido_id distintos dos vínculos CONFIRMADOS de uma chave_nfe.
   * Usado para marcar os pedidos como 'Entregue' quando a NF é lançada no ERP.
   */
  async findPedidoIdsByChaveConfirmados(chaveNfe: string): Promise<string[]> {
    const rows = await this.prisma.com_pedido_nfe_vinculo.findMany({
      where: { chave_nfe: chaveNfe, confirmado: true },
      select: { pedido_id: true },
      distinct: ['pedido_id'],
    });
    return rows.map((r) => r.pedido_id);
  }

  /** Lê o pedido (id, status, data_recebimento) para a marcação de Entregue. */
  async findPedidoEntrega(pedidoId: string) {
    return this.prisma.com_pedido.findUnique({
      where: { id: pedidoId },
      select: { id: true, status: true, data_recebimento: true },
    });
  }

  /** Marca o pedido como Entregue gravando a data de recebimento. */
  async marcarPedidoEntregue(pedidoId: string, dataRecebimento: Date) {
    return this.prisma.com_pedido.update({
      where: { id: pedidoId },
      data: { status: 'Entregue', data_recebimento: dataRecebimento },
    });
  }
}
