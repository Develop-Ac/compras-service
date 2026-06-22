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

  // --------------------------- Saldo por item da NF --------------------------

  /** Normaliza o cProd para casar com o que é gravado em cprod_xml (mesma regra do service). */
  private normCprod(s: any): string {
    if (s == null) return '';
    return String(s).replace(/\s+/g, '').toUpperCase().replace(/^0+(?=.)/, '');
  }

  /**
   * Semeia/atualiza o snapshot do total de cada item da NF (com_nfe_saldo_item).
   * Só mexe em qtd_total/descricao; o consumido é sempre calculado ao vivo.
   */
  async upsertSaldoNfItens(
    chaveNfe: string,
    itens: Array<{ cprod: string; descricao?: string | null; qtd_total: number }>,
  ): Promise<void> {
    if (!itens.length) return;
    await this.prisma.$transaction(
      itens.map((it) =>
        this.prisma.com_nfe_saldo_item.upsert({
          where: { chave_nfe_cprod: { chave_nfe: chaveNfe, cprod: it.cprod } },
          create: {
            chave_nfe: chaveNfe,
            cprod: it.cprod,
            descricao: it.descricao ?? null,
            qtd_total: new Prisma.Decimal(it.qtd_total),
          },
          update: {
            descricao: it.descricao ?? null,
            qtd_total: new Prisma.Decimal(it.qtd_total),
          },
        }),
      ),
    );
  }

  /** Total por item (cProd normalizado) da NF, a partir do snapshot. */
  async totalPorNfItem(chaveNfe: string): Promise<Map<string, number>> {
    const rows = await this.prisma.com_nfe_saldo_item.findMany({
      where: { chave_nfe: chaveNfe },
      select: { cprod: true, qtd_total: true },
    });
    const mapa = new Map<string, number>();
    for (const r of rows) mapa.set(this.normCprod(r.cprod), Number(r.qtd_total));
    return mapa;
  }

  /**
   * Consumido por item (cProd normalizado) de uma NF: soma quantidade_alocada dos
   * itens tipo='vinculado' de vínculos CONFIRMADOS da chave. Opcionalmente exclui
   * um pedido (o atual, que será reescrito no upsert).
   */
  async consumidoPorNfItem(
    chaveNfe: string,
    exceptPedidoIds?: string[],
  ): Promise<Map<string, number>> {
    const rows = await this.prisma.com_pedido_nfe_vinculo_item.findMany({
      where: {
        tipo: 'vinculado',
        vinculo: {
          chave_nfe: chaveNfe,
          confirmado: true,
          ...(exceptPedidoIds?.length ? { pedido_id: { notIn: exceptPedidoIds } } : {}),
        },
      },
      select: { cprod_xml: true, quantidade_alocada: true, quantidade_xml: true },
    });
    const mapa = new Map<string, number>();
    for (const r of rows) {
      const k = this.normCprod(r.cprod_xml);
      if (!k) continue;
      // Compatibilidade: vínculos antigos sem quantidade_alocada usam quantidade_xml.
      const q = Number(r.quantidade_alocada ?? r.quantidade_xml ?? 0);
      mapa.set(k, (mapa.get(k) ?? 0) + (Number.isFinite(q) ? q : 0));
    }
    return mapa;
  }

  /**
   * Consumido por item do PEDIDO (pro_codigo): soma quantidade_alocada dos itens
   * tipo='vinculado' de vínculos CONFIRMADOS do pedido. Opcionalmente exclui uma
   * chave (a atual, que será reescrita).
   */
  async consumidoPorPedidoItem(
    pedidoIds: string[],
    exceptChave?: string,
  ): Promise<Map<number, number>> {
    if (!pedidoIds.length) return new Map();
    const rows = await this.prisma.com_pedido_nfe_vinculo_item.findMany({
      where: {
        tipo: 'vinculado',
        pro_codigo: { not: null },
        vinculo: {
          pedido_id: { in: pedidoIds },
          confirmado: true,
          ...(exceptChave ? { chave_nfe: { not: exceptChave } } : {}),
        },
      },
      select: { pro_codigo: true, quantidade_alocada: true, quantidade_xml: true },
    });
    const mapa = new Map<number, number>();
    for (const r of rows) {
      if (r.pro_codigo == null) continue;
      const q = Number(r.quantidade_alocada ?? r.quantidade_xml ?? 0);
      mapa.set(r.pro_codigo, (mapa.get(r.pro_codigo) ?? 0) + (Number.isFinite(q) ? q : 0));
    }
    return mapa;
  }

  /**
   * Dado um conjunto de chaves, retorna as que TÊM snapshot e estão TOTALMENTE
   * consumidas (Σ qtd_total − Σ alocada confirmada <= tolerância). Chaves sem
   * snapshot não entram (assume saldo cheio → continuam visíveis).
   */
  async chavesSemSaldo(chaves: string[]): Promise<Set<string>> {
    const semSaldo = new Set<string>();
    if (!chaves.length) return semSaldo;

    const totais = await this.prisma.com_nfe_saldo_item.groupBy({
      by: ['chave_nfe'],
      where: { chave_nfe: { in: chaves } },
      _sum: { qtd_total: true },
    });
    if (!totais.length) return semSaldo;

    const consumidoRows = await this.prisma.com_pedido_nfe_vinculo_item.findMany({
      where: {
        tipo: 'vinculado',
        vinculo: { chave_nfe: { in: chaves }, confirmado: true },
      },
      select: { quantidade_alocada: true, quantidade_xml: true, vinculo: { select: { chave_nfe: true } } },
    });
    const consumidoPorChave = new Map<string, number>();
    for (const r of consumidoRows) {
      const chave = r.vinculo?.chave_nfe;
      if (!chave) continue;
      const q = Number(r.quantidade_alocada ?? r.quantidade_xml ?? 0);
      consumidoPorChave.set(chave, (consumidoPorChave.get(chave) ?? 0) + (Number.isFinite(q) ? q : 0));
    }

    const TOL = 0.001;
    for (const t of totais) {
      const total = Number(t._sum.qtd_total ?? 0);
      if (total <= 0) continue;
      const consumido = consumidoPorChave.get(t.chave_nfe) ?? 0;
      if (total - consumido <= TOL) semSaldo.add(t.chave_nfe);
    }
    return semSaldo;
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

  /**
   * Vínculos CONFIRMADOS de um conjunto de chaves de NF, com os itens do lado XML
   * (vinculado / xml_sem_vinculo). Usado para o resumo da listagem e o detalhe da NF.
   */
  async findVinculosConfirmadosByChaves(chaves: string[]) {
    if (!chaves.length) return [];
    return this.prisma.com_pedido_nfe_vinculo.findMany({
      where: { chave_nfe: { in: chaves }, confirmado: true },
      select: {
        id: true,
        pedido_id: true,
        pedido_cotacao: true,
        chave_nfe: true,
        itens: {
          where: { tipo: { in: ['vinculado', 'xml_sem_vinculo'] } },
          select: {
            tipo: true,
            cprod_xml: true,
            produto_xml: true,
            pro_codigo: true,
          },
        },
      },
    });
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

  /** Lê o pedido (id, status, data_recebimento, ipi_no_valor) para a conferência. */
  async findPedidoParaConferencia(pedidoId: string) {
    return this.prisma.com_pedido.findUnique({
      where: { id: pedidoId },
      select: { id: true, status: true, data_recebimento: true, ipi_no_valor: true },
    });
  }

  /** Liga/desliga o flag "IPI incluso no valor unitário" do pedido. */
  async setPedidoIpiNoValor(pedidoId: string, valor: boolean) {
    return this.prisma.com_pedido.update({
      where: { id: pedidoId },
      data: { ipi_no_valor: valor },
      select: { id: true, ipi_no_valor: true },
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
        id: true,
        tipo: true,
        produto_xml: true,
        cprod_xml: true,
        quantidade_xml: true,
        quantidade_alocada: true,
        excede_saldo: true,
        vuncom_xml: true,
        pro_codigo: true,
        vinculo: { select: { chave_nfe: true } },
      },
    });
  }

  /**
   * Outros pedidos (≠ exceptPedidoId) que têm vínculo CONFIRMADO consumindo os
   * mesmos itens (chave_nfe + pro_codigo). Usado na conferência para mostrar, num
   * item que excede saldo, em quais outros pedidos a NF foi vinculada.
   */
  async findOutrosPedidosVinculados(
    chaves: string[],
    proCodigos: number[],
    exceptPedidoId: string,
  ) {
    if (!chaves.length || !proCodigos.length) return [];
    return this.prisma.com_pedido_nfe_vinculo_item.findMany({
      where: {
        tipo: 'vinculado',
        pro_codigo: { in: proCodigos },
        vinculo: {
          chave_nfe: { in: chaves },
          confirmado: true,
          pedido_id: { not: exceptPedidoId },
        },
      },
      select: {
        pro_codigo: true,
        quantidade_alocada: true,
        quantidade_xml: true,
        vinculo: { select: { pedido_id: true, pedido_cotacao: true, chave_nfe: true } },
      },
    });
  }

  /** Lê um item de vínculo + o pedido a que pertence (via cabeçalho). */
  async findVinculoItemComPedido(itemId: string) {
    return this.prisma.com_pedido_nfe_vinculo_item.findUnique({
      where: { id: itemId },
      select: { id: true, tipo: true, vinculo: { select: { pedido_id: true } } },
    });
  }

  /** Converte um item (xml_sem_vinculo) em 'vinculado', casando com um item do pedido. */
  async vincularItem(
    itemId: string,
    dados: {
      pro_codigo: number;
      pro_descricao?: string | null;
      quantidade_pedido?: number | null;
      valor_pedido?: number | null;
    },
  ) {
    return this.prisma.com_pedido_nfe_vinculo_item.update({
      where: { id: itemId },
      data: {
        tipo: 'vinculado',
        pro_codigo: dados.pro_codigo,
        pro_descricao: dados.pro_descricao ?? null,
        quantidade_pedido: dados.quantidade_pedido ?? null,
        valor_pedido: dados.valor_pedido ?? null,
        match_campo: 'Manual (conferência)',
        match_valor: 'Vínculo manual',
        origem: 'manual',
      },
    });
  }

  /** Desfaz o vínculo de um item: volta para 'xml_sem_vinculo', limpando o lado do pedido. */
  async desvincularItem(itemId: string) {
    return this.prisma.com_pedido_nfe_vinculo_item.update({
      where: { id: itemId },
      data: {
        tipo: 'xml_sem_vinculo',
        pro_codigo: null,
        pro_descricao: null,
        quantidade_pedido: null,
        valor_pedido: null,
        quantidade_alocada: null,
        excede_saldo: false,
        match_campo: 'Desvinculado (conferência)',
        match_valor: null,
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

  /** Conciliação (status_erp + dt_entrada + valor_total) das chaves informadas, do Postgres. */
  async findConciliacaoByChaves(chaves: string[]) {
    if (!chaves.length) return [];
    return this.prisma.com_nfe_conciliacao.findMany({
      where: { chave_nfe: { in: chaves } },
      select: { chave_nfe: true, status_erp: true, dt_entrada: true, valor_total: true },
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
