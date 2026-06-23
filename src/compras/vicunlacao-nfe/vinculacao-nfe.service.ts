import { Injectable, Logger, NotFoundException } from '@nestjs/common';
import { Prisma } from '@prisma/client';
import * as zlib from 'zlib';
import {
  CotacaoItemRow,
  VinculacaoNfeRepository,
} from './vinculacao-nfe.repository';
import { SalvarVinculoDto } from './dto/salvar-vinculo.dto';
import { FornecedorGrupoService } from '../fornecedor-grupo/fornecedor-grupo.service';

/** Item extraído do XML da NF-e */
export interface ItemXml {
  cProd: string;
  xProd: string;
  qCom: number | null;
  vUnCom: number | null;
  vProd: number | null;
}

/** Item de cotação normalizado (Firebird + Postgres unificados) */
interface ItemCotacao {
  _idx: number;
  _origem: 'firebird' | 'pg';
  pro_codigo: string | number | null;
  pro_descricao: string | null;
  mar_descricao: string | null;
  referencia: string | null;
  ref_fabricante: string | null;
  ref_fornecedor: string | null;
  unidade: string | null;
  quantidade: number | null;
  valor_unitario: number | null;
  for_codigo: number | null;
}

/** Linha vinculada (item do XML casado com item da cotação) */
export interface ItemVinculado {
  produto_xml: string;
  cprod_xml: string | null;
  quantidade_xml: number | null;
  vuncom_xml: number | null;
  pro_codigo: string | number | null;
  pro_descricao: string | null;
  quantidade_cotacao: number | null;
  quantidade_pedido: number | null;
  valor_pedido: number | null;
  // saldo: quanto deste pedido foi alocado do item da NF + saldos disponíveis
  quantidade_alocada: number | null;
  saldo_nf: number | null;
  saldo_pedido: number | null;
  excede_saldo: boolean;
  match_campo: string;
  match_valor: string | null;
  origem: 'firebird' | 'pg';
}

/** Produto do pedido (com_pedido_itens) — usado nos vinculados e nos sem vínculo. */
export interface ItemPedido {
  pro_codigo: number;
  pro_descricao: string | null;
  mar_descricao: string | null;
  referencia: string | null;
  unidade: string | null;
  for_codigo: number | null;
  quantidade: number | null;
  valor_unitario: number | null;
}

// O cProd/xProd da NF é casado APENAS contra referências externas — referencia,
// ref_fabricante e ref_fornecedor (referência grupo). NUNCA contra o pro_codigo
// (código interno), que jamais coincide com o código do fornecedor; casá-los
// causava colisões (ex.: cProd 038883 = pro_codigo 38883 de outro produto).
const COLUNAS_COTACAO = ['referencia', 'ref_fabricante', 'ref_fornecedor'] as const;
type ColunaCotacao = (typeof COLUNAS_COTACAO)[number];

// Conferência Pedido × Faturado: diferença de valor mínima (em R$) para um item
// ser considerado DIVERGENTE. Diferenças menores são ignoradas (centavos/IPI/etc.).
const TOLERANCIA_VALOR_DIVERGENTE = 1;

@Injectable()
export class VinculacaoNfeService {
  private readonly logger = new Logger(VinculacaoNfeService.name);

  constructor(
    private readonly repo: VinculacaoNfeRepository,
    private readonly grupo: FornecedorGrupoService,
  ) {}

  /**
   * Registra uma entrada no Histórico de Alterações do pedido (tela
   * 'Detalhes do Pedido'), via log-service. A descrição DEVE conter
   * "pedido <pedido_id>" para casar com o filtro do histórico (o id é depois
   * substituído pelo nº da cotação na exibição). Fire-and-forget: uma falha de
   * log nunca quebra a operação de vínculo.
   */
  private async registrarLogPedido(args: {
    usuario?: string | null;
    acao: string;
    descricao: string;
  }): Promise<void> {
    try {
      await fetch('http://log-service.acacessorios.local/log', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          usuario: args.usuario ?? null,
          setor: 'Compras',
          tela: 'Detalhes do Pedido',
          acao: args.acao,
          descricao: args.descricao,
        }),
      });
    } catch (err: any) {
      this.logger.warn(`Falha ao registrar log de vínculo de NF-e: ${err?.message || err}`);
    }
  }

  /**
   * Carrega e unifica os itens da cotação (Firebird PEDIDOS_COTACOES + Postgres
   * com_cotacao_itens_for), já com as referências de fornecedor enriquecidas pelo
   * grupo. Exposto para que o auto-vínculo/botão "Sugerir" busque UMA vez e reuse
   * em todas as NF-e candidatas do mesmo pedido (ver opts.itensCotacao de vincular).
   */
  async carregarItensCotacao(pedido: number): Promise<ItemCotacao[]> {
    const [itensFb, itensPg] = await Promise.all([
      this.repo.findCotacaoItens(pedido),
      this.repo.findCotacaoItensFor(pedido),
    ]);
    await this.grupo.enriquecerRefsEmBranco(itensPg as any);
    return this.unificarItensCotacao(itensFb, itensPg);
  }

  /**
   * Pipeline completo: busca XML da NF-e, busca itens da cotação e do pedido,
   * vincula e devolve as 3 listas (vinculados, XML sem vínculo, pedido sem vínculo).
   */
  async vincular(
    pedido: number,
    nfe: string,
    forCodigo?: number | null,
    opts?: { itensCotacao?: ItemCotacao[] },
  ) {
    // 1) XML da NF-e — Postgres PRIMEIRO.
    // O calculadora-st-service já importa o XML completo para com_nfe_conciliacao
    // (íntegro). O OPENQUERY do Firebird, por outro lado, trunca XML_COMPLETO num
    // teto fixo (~11 KB), zerando os itens de NF-e maiores. Por isso usamos o
    // Postgres como fonte primária e o Firebird apenas como fallback.
    let itensXml = [] as ReturnType<typeof this.parseItensNfe>;
    let nfeRow: Awaited<ReturnType<typeof this.repo.findXmlByChave>> = null;

    const xmlPg = await this.repo.findConciliacaoXmlByChave(nfe);
    if (xmlPg) {
      itensXml = this.parseItensNfe(xmlPg);
    }

    // Fallback: não achou no Postgres (ou veio sem itens) -> busca no Firebird.
    if (!itensXml.length) {
      nfeRow = await this.repo.findXmlByChave(nfe);
      if (nfeRow?.XML_COMPLETO != null) {
        itensXml = this.parseItensNfe(nfeRow.XML_COMPLETO);
      }
    }

    if (!xmlPg && !nfeRow) {
      throw new NotFoundException(`Nenhuma NF-e encontrada para a chave ${nfe}.`);
    }

    // 2) Itens da cotação (Firebird + com_cotacao_itens_for). Podem ser injetados
    //    já prontos (opts.itensCotacao) quando o chamador processa VÁRIAS NF-e do
    //    mesmo pedido (auto-vínculo / botão "Sugerir"), evitando refazer a busca e
    //    o enriquecimento de referências a cada NF.
    const itensCotacao = opts?.itensCotacao ?? (await this.carregarItensCotacao(pedido));

    // 3) Itens do pedido (com_pedido / com_pedido_itens) — mapa por pro_codigo
    //    (mantém o registro mais recente, já que vem ordenado por emissao DESC).
    const pedidoIds = await this.repo.findPedidoIds(pedido, forCodigo);
    const itensPedidoRows = await this.repo.findPedidoItens(pedidoIds);
    const pedidoPorCodigo = new Map<string, ItemPedido>();
    for (const r of itensPedidoRows) {
      const key = String(r.pro_codigo);
      if (pedidoPorCodigo.has(key)) continue;
      pedidoPorCodigo.set(key, {
        pro_codigo: r.pro_codigo,
        pro_descricao: r.pro_descricao,
        mar_descricao: r.mar_descricao,
        referencia: r.referencia,
        unidade: r.unidade,
        for_codigo: r.for_codigo,
        quantidade: r.quantidade == null ? null : Number(r.quantidade),
        valor_unitario: r.valor_unitario == null ? null : Number(r.valor_unitario),
      });
    }

    // 3.1) Saldo da NF: semeia o snapshot do total por item (qCom agregado por cProd)
    //      e carrega o consumido (vínculos confirmados) da NF (outros pedidos) e do
    //      pedido (outras NFs). Saldo é sempre calculado a partir de confirmados.
    const totalNf = new Map<string, number>();
    const seedItens: Array<{ cprod: string; descricao?: string | null; qtd_total: number }> = [];
    for (const item of itensXml) {
      const k = this.normRef(item.cProd);
      if (!k) continue;
      const q = item.qCom == null ? 0 : Number(item.qCom);
      totalNf.set(k, (totalNf.get(k) ?? 0) + (Number.isFinite(q) ? q : 0));
    }
    // monta seed preservando o cProd original (primeira ocorrência) e a descrição
    const cprodOriginal = new Map<string, { cprod: string; descricao: string | null }>();
    for (const item of itensXml) {
      const k = this.normRef(item.cProd);
      if (!k || cprodOriginal.has(k)) continue;
      cprodOriginal.set(k, { cprod: item.cProd ?? k, descricao: item.xProd ?? null });
    }
    for (const [k, total] of totalNf) {
      const orig = cprodOriginal.get(k);
      seedItens.push({ cprod: orig?.cprod ?? k, descricao: orig?.descricao ?? null, qtd_total: total });
    }
    await this.repo.upsertSaldoNfItens(nfe, seedItens);

    // Consumido (mutável: vai sendo decrementado conforme alocamos nesta chamada).
    const consumidoNf = await this.repo.consumidoPorNfItem(nfe, pedidoIds);
    const consumidoPedido = await this.repo.consumidoPorPedidoItem(pedidoIds, nfe);

    // 4) Vinculação XML <-> cotação (resolve o pro_codigo) + fallback semântico
    const usados = new Set<number>();
    const vinculados: ItemVinculado[] = [];
    const xmlSemVinculo: ItemXml[] = [];
    const proCodigosVinculados = new Set<string>();

    const indices = this.indexarCotacao(itensCotacao);

    // Preço por pro_codigo (do pedido) para reforçar o match semântico pelo Vlr Un.
    const precoPorCodigo = new Map<string, number>();
    for (const [codigo, p] of pedidoPorCodigo) {
      if (p.valor_unitario != null) precoPorCodigo.set(codigo, Number(p.valor_unitario));
    }

    for (const item of itensXml) {
      const match =
        this.encontrarMatch(item, indices) ??
        this.matchSemantico(item, itensCotacao, usados, precoPorCodigo);
      if (match) {
        const codigo = match.item.pro_codigo;
        const ped = codigo == null ? undefined : pedidoPorCodigo.get(String(codigo));

        // O produto casado precisa pertencer a ESTE pedido. Casamentos contra a
        // cotação (que tem itens de vários fornecedores) cujo produto não está no
        // pedido vão para "XML sem vínculo", não contam como vinculados.
        if (!ped) {
          xmlSemVinculo.push(item);
          continue;
        }

        // Saldo deste item: restante da NF (total − consumido) e do pedido.
        const cprodNorm = this.normRef(item.cProd);
        const qCom = item.qCom == null ? 0 : Number(item.qCom);
        const totalItemNf = cprodNorm ? (totalNf.get(cprodNorm) ?? qCom) : qCom;
        const jaConsumidoNf = cprodNorm ? (consumidoNf.get(cprodNorm) ?? 0) : 0;
        const saldoNf = Math.max(0, totalItemNf - jaConsumidoNf);

        const codigoNum = codigo == null ? null : Number(codigo);
        const qtdPedido = ped?.quantidade ?? null;
        const jaConsumidoPed = codigoNum == null ? 0 : (consumidoPedido.get(codigoNum) ?? 0);
        const saldoPedido = qtdPedido == null ? null : Math.max(0, qtdPedido - jaConsumidoPed);

        const limites = [qCom, saldoNf, ...(saldoPedido == null ? [] : [saldoPedido])];
        const alocada = Math.max(0, Math.min(...limites));
        const excede = qCom > saldoNf + 1e-6 || (saldoPedido != null && qCom > saldoPedido + 1e-6);

        // Vinculação automática (tokens) só ocorre se houver saldo disponível.
        // Sem saldo (NF ou pedido já totalmente consumidos), o item NÃO é vinculado
        // automaticamente — vai para "XML sem vínculo", onde pode ser vinculado
        // manualmente (com aviso de saldo) se o usuário decidir.
        if (alocada <= 1e-6) {
          xmlSemVinculo.push(item);
          continue;
        }

        usados.add(match.item._idx);
        if (codigo != null) proCodigosVinculados.add(String(codigo));

        // Reserva o alocado para os próximos itens desta mesma chamada.
        if (cprodNorm) consumidoNf.set(cprodNorm, jaConsumidoNf + alocada);
        if (codigoNum != null) consumidoPedido.set(codigoNum, jaConsumidoPed + alocada);

        vinculados.push({
          produto_xml: item.xProd,
          cprod_xml: item.cProd ?? null,
          quantidade_xml: item.qCom,
          vuncom_xml: item.vUnCom,
          pro_codigo: codigo,
          pro_descricao: match.item.pro_descricao,
          quantidade_cotacao: match.item.quantidade,
          quantidade_pedido: qtdPedido,
          valor_pedido: ped?.valor_unitario ?? null,
          quantidade_alocada: alocada,
          saldo_nf: saldoNf,
          saldo_pedido: saldoPedido,
          excede_saldo: excede,
          match_campo: match.campo,
          match_valor: match.valor,
          origem: match.item._origem,
        });
      } else {
        xmlSemVinculo.push(item);
      }
    }

    // 5) Produtos do PEDIDO que não foram vinculados (e não da cotação)
    const pedidoSemVinculo = [...pedidoPorCodigo.values()].filter(
      (p) => !proCodigosVinculados.has(String(p.pro_codigo)),
    );

    // Emitente: o caminho Postgres não traz NOME_EMITENTE (nfeRow nulo) e a NF
    // lançada via NF_ENTRADA_XML também não; completa a partir da conciliação.
    let emitente = nfeRow?.NOME_EMITENTE ?? null;
    if (!emitente) {
      const conc = await this.repo.findConciliacaoByChave(nfe);
      emitente = conc?.emitente ?? null;
    }

    return {
      pedido_cotacao: pedido,
      chave_nfe: nfe,
      emitente,
      totais: {
        itens_xml: itensXml.length,
        itens_cotacao: itensCotacao.length,
        itens_pedido: pedidoPorCodigo.size,
        vinculados: vinculados.length,
        xml_sem_vinculo: xmlSemVinculo.length,
        pedido_sem_vinculo: pedidoSemVinculo.length,
      },
      vinculados,
      xml_sem_vinculo: xmlSemVinculo,
      pedido_sem_vinculo: pedidoSemVinculo,
    };
  }

  // -------------------------- Persistência do vínculo ------------------------

  /** Converte número (ou null/undefined) para Prisma.Decimal aceito pelo client. */
  private toDecimal(v: number | null | undefined): Prisma.Decimal | null {
    if (v == null || !Number.isFinite(Number(v))) return null;
    return new Prisma.Decimal(v);
  }

  /**
   * Recalcula, no servidor, a quantidade_alocada e o flag excede_saldo de cada
   * item tipo='vinculado' a partir dos saldos ATUAIS (vínculos confirmados),
   * excluindo o próprio pedido (lado NF) e a própria chave (lado pedido) — pois
   * este vínculo será reescrito. Muta a lista de itens in-place.
   */
  private async aplicarSaldoNosItens(
    pedidoId: string,
    chaveNfe: string,
    itens: Prisma.com_pedido_nfe_vinculo_itemCreateManyVinculoInput[],
  ): Promise<void> {
    const totalNf = await this.repo.totalPorNfItem(chaveNfe);
    const consumidoNf = await this.repo.consumidoPorNfItem(chaveNfe, [pedidoId]);
    const consumidoPedido = await this.repo.consumidoPorPedidoItem([pedidoId], chaveNfe);

    for (const it of itens) {
      if (it.tipo !== 'vinculado') continue;
      const cprodNorm = this.normRef(it.cprod_xml);
      const qCom = it.quantidade_xml == null ? 0 : Number(it.quantidade_xml);
      const totalItemNf = cprodNorm ? (totalNf.get(cprodNorm) ?? qCom) : qCom;
      const jaNf = cprodNorm ? (consumidoNf.get(cprodNorm) ?? 0) : 0;
      const saldoNf = Math.max(0, totalItemNf - jaNf);

      const codigoNum = it.pro_codigo == null ? null : Number(it.pro_codigo);
      const qtdPedido = it.quantidade_pedido == null ? null : Number(it.quantidade_pedido);
      const jaPed = codigoNum == null ? 0 : (consumidoPedido.get(codigoNum) ?? 0);
      const saldoPedido = qtdPedido == null ? null : Math.max(0, qtdPedido - jaPed);

      const limites = [qCom, saldoNf, ...(saldoPedido == null ? [] : [saldoPedido])];
      const alocada = Math.max(0, Math.min(...limites));
      const excede = qCom > saldoNf + 1e-6 || (saldoPedido != null && qCom > saldoPedido + 1e-6);

      it.quantidade_alocada = this.toDecimal(alocada);
      it.excede_saldo = excede;

      if (cprodNorm) consumidoNf.set(cprodNorm, jaNf + alocada);
      if (codigoNum != null) consumidoPedido.set(codigoNum, jaPed + alocada);
    }
  }

  /**
   * Salva (upsert) a conferência de uma NF-e num pedido: cabeçalho + itens
   * tipados, substituindo o snapshot anterior (mesmo par pedido_id + chave).
   */
  async salvarVinculo(dto: SalvarVinculoDto) {
    const dataEmissao =
      dto.data_emissao && !Number.isNaN(Date.parse(dto.data_emissao))
        ? new Date(dto.data_emissao)
        : null;

    const itens: Prisma.com_pedido_nfe_vinculo_itemCreateManyVinculoInput[] = (
      dto.itens ?? []
    ).map((it) => ({
      tipo: it.tipo,
      produto_xml: it.produto_xml ?? null,
      cprod_xml: it.cprod_xml ?? null,
      quantidade_xml: this.toDecimal(it.quantidade_xml),
      vuncom_xml: this.toDecimal(it.vuncom_xml),
      pro_codigo: it.pro_codigo ?? null,
      pro_descricao: it.pro_descricao ?? null,
      quantidade_cotacao: this.toDecimal(it.quantidade_cotacao),
      quantidade_pedido: this.toDecimal(it.quantidade_pedido),
      valor_pedido: this.toDecimal(it.valor_pedido),
      match_campo: it.match_campo ?? null,
      match_valor: it.match_valor ?? null,
      origem: it.origem ?? null,
    }));

    // Recalcula quantidade_alocada / excede_saldo a partir dos saldos atuais.
    await this.aplicarSaldoNosItens(dto.pedido_id, dto.chave_nfe, itens);

    const { vinculo, porTipo } = await this.repo.salvarVinculo(
      {
        pedido_id: dto.pedido_id,
        pedido_cotacao: dto.pedido_cotacao,
        for_codigo: dto.for_codigo ?? null,
        chave_nfe: dto.chave_nfe,
        emitente: dto.emitente ?? null,
        data_emissao: dataEmissao,
        valor_total: this.toDecimal(dto.valor_total),
        usuario: dto.usuario ?? null,
      },
      itens,
    );

    const totais: Record<string, number> = {};
    for (const g of porTipo) totais[g.tipo] = g._count._all;

    // Recalcula o status do pedido a partir da cobertura de itens vinculados.
    const status = await this.recalcularStatusPedido(dto.pedido_id);

    // Histórico do pedido: registra o vínculo da NF-e.
    const nVinc = totais['vinculado'] ?? 0;
    await this.registrarLogPedido({
      usuario: dto.usuario ?? null,
      acao: 'Vínculo NF-e',
      descricao: `Vinculou a NF-e ${dto.chave_nfe} ao pedido ${dto.pedido_id} (${nVinc} item(ns) vinculado(s)).`,
    });

    return { ...vinculo, totais, status };
  }

  /**
   * Grava uma SUGESTÃO de vínculo (confirmado=false, origem='auto') a partir do
   * resultado do motor de casamento (saída de `vincular`). Reutiliza a mesma
   * gravação de itens do salvar manual, apenas trocando os flags. Não recalcula
   * status aqui (o job marca 'Vínculo sugerido' separadamente).
   */
  async salvarSugestao(args: {
    pedido_id: string;
    pedido_cotacao: number;
    for_codigo?: number | null;
    chave_nfe: string;
    emitente?: string | null;
    data_emissao?: Date | null;
    valor_total?: number | null;
    resultado: Awaited<ReturnType<VinculacaoNfeService['vincular']>>;
  }) {
    const r = args.resultado;

    const itens: Prisma.com_pedido_nfe_vinculo_itemCreateManyVinculoInput[] = [];

    for (const v of r.vinculados) {
      itens.push({
        tipo: 'vinculado',
        produto_xml: v.produto_xml ?? null,
        cprod_xml: v.cprod_xml ?? null,
        quantidade_xml: this.toDecimal(v.quantidade_xml),
        vuncom_xml: this.toDecimal(v.vuncom_xml),
        pro_codigo: v.pro_codigo == null ? null : Number(v.pro_codigo),
        pro_descricao: v.pro_descricao ?? null,
        quantidade_cotacao: this.toDecimal(v.quantidade_cotacao),
        quantidade_pedido: this.toDecimal(v.quantidade_pedido),
        valor_pedido: this.toDecimal(v.valor_pedido),
        quantidade_alocada: this.toDecimal(v.quantidade_alocada),
        excede_saldo: v.excede_saldo ?? false,
        match_campo: v.match_campo ?? null,
        match_valor: v.match_valor ?? null,
        origem: 'auto',
      });
    }

    for (const x of r.xml_sem_vinculo) {
      itens.push({
        tipo: 'xml_sem_vinculo',
        produto_xml: x.xProd ?? null,
        cprod_xml: x.cProd ?? null,
        quantidade_xml: this.toDecimal(x.qCom),
        vuncom_xml: this.toDecimal(x.vUnCom),
        pro_codigo: null,
        pro_descricao: null,
        quantidade_cotacao: null,
        quantidade_pedido: null,
        valor_pedido: null,
        match_campo: null,
        match_valor: null,
        origem: 'auto',
      });
    }

    for (const p of r.pedido_sem_vinculo) {
      itens.push({
        tipo: 'pedido_sem_vinculo',
        produto_xml: null,
        cprod_xml: null,
        quantidade_xml: null,
        vuncom_xml: null,
        pro_codigo: p.pro_codigo == null ? null : Number(p.pro_codigo),
        pro_descricao: p.pro_descricao ?? null,
        quantidade_cotacao: null,
        quantidade_pedido: this.toDecimal(p.quantidade),
        valor_pedido: this.toDecimal(p.valor_unitario),
        match_campo: null,
        match_valor: null,
        origem: 'auto',
      });
    }

    const { vinculo } = await this.repo.salvarVinculo(
      {
        pedido_id: args.pedido_id,
        pedido_cotacao: args.pedido_cotacao,
        for_codigo: args.for_codigo ?? null,
        chave_nfe: args.chave_nfe,
        emitente: args.emitente ?? null,
        data_emissao: args.data_emissao ?? null,
        valor_total: this.toDecimal(args.valor_total),
        usuario: null,
      },
      itens,
      { confirmado: false, origem: 'auto' },
    );

    return vinculo;
  }

  // --------------------- Conferência por item (fechamento) -------------------

  /**
   * Monta a conferência de fechamento de um pedido: uma linha por item do
   * pedido comparando o que foi pedido com o que foi faturado (somando todas as
   * NF-e vinculadas confirmadas), mais os itens das NF-e que não estão no
   * pedido. Somente leitura.
   */
  async conferenciaPorItem(pedidoId: string) {
    const pedido = await this.repo.findPedidoParaConferencia(pedidoId);
    if (!pedido) {
      throw new NotFoundException(`Pedido ${pedidoId} não encontrado.`);
    }

    const num = (d: Prisma.Decimal | number | null | undefined): number => {
      if (d == null) return 0;
      const n = Number(d);
      return Number.isFinite(n) ? n : 0;
    };

    const itensPedido = await this.repo.findItensDoPedido(pedidoId);
    const itensVinculo = await this.repo.findItensVinculadosConfirmados(pedidoId);

    // Agrega o faturado (itens tipo='vinculado') por pro_codigo, guardando a
    // contribuição de cada NF (qtd + chave) para depois separar entregue x pendente.
    interface AggFaturado {
      quantidade_faturada: number;
      valor_faturado: number; // última vuncom_xml
      chaves_nfe: Set<string>;
      contribs: Array<{ qtd: number; chave: string }>;
      excede_saldo: boolean;
      // Código + descrição do produto no XML da NF (pode haver mais de um casado no mesmo pro_codigo)
      xmlProds: Map<string, { cprod_xml: string | null; produto_xml: string | null }>;
      // ids dos com_pedido_nfe_vinculo_item (tipo='vinculado') deste produto — p/ desvincular
      itemIds: string[];
    }
    const faturadoPorCodigo = new Map<number, AggFaturado>();
    const chavesFaturadas = new Set<string>();
    const itensNfSemPedido: Array<{
      id: string;
      produto_xml: string;
      cprod_xml: string | null;
      quantidade_xml: number;
      vuncom_xml: number;
      chave_nfe: string;
    }> = [];

    // Ajustes por unidade da NF (lidos do XML, memoizados por chave):
    //  - desconto (vDesc) e acréscimo (vOutro): aplicados SEMPRE (valor líquido).
    //  - IPI (vIPI): somado só quando o pedido inclui IPI no valor (flag).
    const ipiNoValor = !!pedido.ipi_no_valor;
    type AjusteUnit = { fator: number; precoUnit: number; ipiUnit: number; descUnit: number; outroUnit: number };
    const ajustesCachePorChave = new Map<string, Map<string, AjusteUnit>>();
    const getAjustesMap = async (chave: string): Promise<Map<string, AjusteUnit>> => {
      const cached = ajustesCachePorChave.get(chave);
      if (cached) return cached;
      let map = new Map<string, AjusteUnit>();
      try {
        // Postgres (conciliação) primeiro; fallback Firebird, igual ao vincular().
        let xml: string | Buffer | null = await this.repo.findConciliacaoXmlByChave(chave);
        if (!xml) {
          const row = await this.repo.findXmlByChave(chave);
          xml = row?.XML_COMPLETO ?? null;
        }
        if (xml) map = this.parseAjustesUnitPorCprod(xml);
      } catch {
        // sem ajustes se não for possível ler/parsear o XML
      }
      ajustesCachePorChave.set(chave, map);
      return map;
    };

    for (const it of itensVinculo) {
      const chave = it.vinculo?.chave_nfe ?? '';
      if (chave) chavesFaturadas.add(chave);
      if (it.tipo === 'vinculado') {
        if (it.pro_codigo == null) continue;
        const cod = Number(it.pro_codigo);
        const atual: AggFaturado =
          faturadoPorCodigo.get(cod) ??
          { quantidade_faturada: 0, valor_faturado: 0, chaves_nfe: new Set<string>(), contribs: [], excede_saldo: false, xmlProds: new Map(), itemIds: [] };
        // Ajustes da NF (já na unidade base): fator de conversão de unidade,
        // desconto/acréscimo e IPI por unidade.
        const aj = chave
          ? (await getAjustesMap(chave)).get(this.normRef(it.cprod_xml))
          : undefined;
        const fator = aj?.fator ?? 1;

        // Base do faturado = quantidade_alocada (quanto deste item da NF foi para
        // ESTE pedido), evitando dobrar quando a NF é repartida entre pedidos.
        // Fallback p/ quantidade_xml em vínculos antigos sem alocação gravada.
        // Convertida p/ a unidade base quando a NF é em outra unidade (ex.: 3 CJ
        // * 100 = 300 UN), para casar com a quantidade do pedido.
        const q = num(it.quantidade_alocada ?? it.quantidade_xml) * fator;
        atual.quantidade_faturada += q;
        // Unitário faturado na unidade base. Com conversão (fator != 1) usa o preço
        // tributável (vUnTrib/UN); senão mantém o vUnCom do snapshot ("última"
        // sobrescreve). Depois aplica desconto (−), acréscimo (+) e IPI (+ se o
        // pedido inclui IPI no valor).
        let unitFaturado = num(it.vuncom_xml);
        if (aj) {
          if (fator !== 1 && aj.precoUnit > 0) unitFaturado = aj.precoUnit;
          unitFaturado += -aj.descUnit + aj.outroUnit;
          if (ipiNoValor) unitFaturado += aj.ipiUnit;
        }
        atual.valor_faturado = Math.max(0, unitFaturado);
        if (it.excede_saldo) atual.excede_saldo = true;
        if (it.id) atual.itemIds.push(it.id);
        // Guarda o código/descrição do produto no XML (dedup por cprod_xml).
        const xmlKey = String(it.cprod_xml ?? it.produto_xml ?? '');
        if (xmlKey && !atual.xmlProds.has(xmlKey)) {
          atual.xmlProds.set(xmlKey, {
            cprod_xml: it.cprod_xml ?? null,
            produto_xml: it.produto_xml ?? null,
          });
        }
        if (chave) {
          atual.chaves_nfe.add(chave);
          atual.contribs.push({ qtd: q, chave });
        }
        faturadoPorCodigo.set(cod, atual);
      } else if (it.tipo === 'xml_sem_vinculo') {
        itensNfSemPedido.push({
          id: it.id,
          produto_xml: it.produto_xml ?? '',
          cprod_xml: it.cprod_xml ?? null,
          quantidade_xml: num(it.quantidade_xml),
          vuncom_xml: num(it.vuncom_xml),
          chave_nfe: chave,
        });
      }
    }

    // Conciliação das chaves: status lançada (entregue) + valor total da NF.
    const concChaves = await this.repo.findConciliacaoByChaves([...chavesFaturadas]);
    const chaveLancada = new Map<string, boolean>(
      concChaves.map((c) => [c.chave_nfe, c.status_erp === 'LANCADA']),
    );
    const valorFaturadoTotal = concChaves.reduce((acc, c) => acc + num(c.valor_total), 0);

    // Outros pedidos que compartilham as mesmas NF-e/produtos (p/ os itens que
    // excedem saldo): mapa pro_codigo -> [{ pedido_id, pedido_cotacao, chave_nfe }].
    const proCodigosPedido = itensPedido
      .map((p) => Number(p.pro_codigo))
      .filter((c) => Number.isFinite(c));
    const outrosRows = await this.repo.findOutrosPedidosVinculados(
      [...chavesFaturadas],
      proCodigosPedido,
      pedidoId,
    );
    const outrosPorCodigo = new Map<
      number,
      Array<{ pedido_id: string; pedido_cotacao: number; chave_nfe: string }>
    >();
    for (const r of outrosRows) {
      if (r.pro_codigo == null || !r.vinculo) continue;
      const cod = Number(r.pro_codigo);
      const lista = outrosPorCodigo.get(cod) ?? [];
      // dedup por pedido_id + chave_nfe
      const ja = lista.some(
        (x) => x.pedido_id === r.vinculo!.pedido_id && x.chave_nfe === r.vinculo!.chave_nfe,
      );
      if (!ja) {
        lista.push({
          pedido_id: r.vinculo.pedido_id,
          pedido_cotacao: r.vinculo.pedido_cotacao,
          chave_nfe: r.vinculo.chave_nfe,
        });
      }
      outrosPorCodigo.set(cod, lista);
    }

    let itensCompletos = 0;
    let itensParciais = 0;
    let itensNaoFaturados = 0;
    let itensDivergentes = 0;
    let valorPedidoTotal = 0;

    const itens = itensPedido.map((p) => {
      const cod = Number(p.pro_codigo);
      const quantidadePedido = num(p.quantidade);
      const valorPedido = num(p.valor_unitario);
      const agg = faturadoPorCodigo.get(cod);
      const quantidadeFaturada = agg?.quantidade_faturada ?? 0;
      const valorFaturado = agg?.valor_faturado ?? 0;
      const chavesNfe = agg ? [...agg.chaves_nfe] : [];

      // Separa o que já foi ENTREGUE (NF lançada) do que está apenas FATURADO
      // (NF vinculada mas ainda não lançada).
      let qEntregue = 0;
      let qFaturadoPendente = 0;
      for (const c of agg?.contribs ?? []) {
        if (chaveLancada.get(c.chave)) qEntregue += c.qtd;
        else qFaturadoPendente += c.qtd;
      }

      // Status do produto (acumula quando há entrega parcial + faturamento).
      const statusProduto: string[] = [];
      if (quantidadeFaturada === 0) {
        statusProduto.push('Não faturado');
      } else if (qEntregue >= quantidadePedido) {
        statusProduto.push('Entregue');
      } else {
        if (qEntregue > 0) statusProduto.push('Entregue parcial');
        if (qFaturadoPendente > 0) {
          statusProduto.push(
            qEntregue + qFaturadoPendente >= quantidadePedido ? 'Faturado' : 'Faturado parcial',
          );
        }
        if (statusProduto.length === 0) statusProduto.push('Não faturado');
      }

      // Divergência de valor: só conta como divergência quando a diferença é de
      // R$ 1,00 ou mais. Diferenças menores (centavos/arredondamento) são ignoradas.
      const valorDiverge =
        Math.abs(valorFaturado - valorPedido) >= TOLERANCIA_VALOR_DIVERGENTE;

      let situacao: 'completo' | 'parcial' | 'nao_faturado' | 'divergente';
      if (quantidadeFaturada === 0) {
        situacao = 'nao_faturado';
        itensNaoFaturados++;
      } else if (valorDiverge || quantidadeFaturada > quantidadePedido) {
        // Valor diferente OU quantidade faturada a mais -> divergente.
        situacao = 'divergente';
        itensDivergentes++;
      } else if (quantidadeFaturada < quantidadePedido) {
        // Faturado a menos (valor ok) -> parcial (ainda pendente).
        situacao = 'parcial';
        itensParciais++;
      } else {
        // Quantidade exata e valor ok -> completo.
        situacao = 'completo';
        itensCompletos++;
      }

      valorPedidoTotal += valorPedido * quantidadePedido;

      return {
        pro_codigo: cod,
        pro_descricao: p.pro_descricao ?? '',
        quantidade_pedido: quantidadePedido,
        quantidade_faturada: quantidadeFaturada,
        quantidade_entregue: qEntregue,
        saldo: quantidadePedido - quantidadeFaturada,
        valor_pedido: valorPedido,
        valor_faturado: valorFaturado,
        diferenca_valor: valorFaturado - valorPedido,
        situacao,
        status_produto: statusProduto,
        excede_saldo: agg?.excede_saldo ?? false,
        produtos_xml: agg ? [...agg.xmlProds.values()] : [],
        outros_pedidos: outrosPorCodigo.get(cod) ?? [],
        vinculo_item_ids: agg?.itemIds ?? [],
        chaves_nfe: chavesNfe,
      };
    });

    // "XML sem vínculo": só mantém itens da NF que AINDA têm saldo GLOBAL para
    // vincular (qtd total do item da NF − consumido em vínculos confirmados de
    // QUALQUER pedido). A mesma NF pode ir para vários pedidos, então o saldo é
    // global, não só deste pedido.
    const chavesSemPedido = [...new Set(itensNfSemPedido.map((i) => i.chave_nfe).filter(Boolean))];
    const totalPorChave = new Map<string, Map<string, number>>();
    const consumidoGlobalPorChave = new Map<string, Map<string, number>>();
    for (const ch of chavesSemPedido) {
      totalPorChave.set(ch, await this.repo.totalPorNfItem(ch));
      consumidoGlobalPorChave.set(ch, await this.repo.consumidoPorNfItem(ch));
    }
    const itensNfComSaldo = itensNfSemPedido.filter((it) => {
      const norm = this.normRef(it.cprod_xml);
      const total = totalPorChave.get(it.chave_nfe)?.get(norm) ?? num(it.quantidade_xml);
      const consumido = consumidoGlobalPorChave.get(it.chave_nfe)?.get(norm) ?? 0;
      return total - consumido > 0.001;
    });

    return {
      pedido_id: pedido.id,
      status: pedido.status ?? '',
      ipi_no_valor: ipiNoValor,
      data_recebimento: pedido.data_recebimento
        ? pedido.data_recebimento.toISOString()
        : null,
      totais: {
        itens_pedido: itens.length,
        itens_completos: itensCompletos,
        itens_parciais: itensParciais,
        itens_nao_faturados: itensNaoFaturados,
        itens_divergentes: itensDivergentes,
        valor_pedido: valorPedidoTotal,
        valor_faturado: valorFaturadoTotal,
      },
      itens,
      itens_nf_sem_pedido: itensNfComSaldo,
    };
  }

  /**
   * Liga/desliga o flag "IPI incluso no valor unitário" do pedido. Quando ligado,
   * a conferência soma o IPI por unidade da NF ao valor faturado antes de comparar.
   */
  async setIpiNoValor(pedidoId: string, valor: boolean) {
    const pedido = await this.repo.findPedidoParaConferencia(pedidoId);
    if (!pedido) {
      throw new NotFoundException(`Pedido ${pedidoId} não encontrado.`);
    }
    const atualizado = await this.repo.setPedidoIpiNoValor(pedidoId, !!valor);
    return { pedido_id: atualizado.id, ipi_no_valor: atualizado.ipi_no_valor };
  }

  /**
   * Vincula manualmente, pela tela de conferência, um item da NF que estava sem
   * pedido (tipo='xml_sem_vinculo') a um produto do pedido. Recalcula o status.
   */
  async vincularItemConferencia(
    itemId: string,
    dados: {
      pro_codigo: number;
      pro_descricao?: string | null;
      quantidade_pedido?: number | null;
      valor_pedido?: number | null;
    },
  ) {
    const item = await this.repo.findVinculoItemComPedido(itemId);
    if (!item) {
      throw new NotFoundException(`Item de vínculo ${itemId} não encontrado.`);
    }
    await this.repo.vincularItem(itemId, dados);
    const status = await this.recalcularStatusPedido(item.vinculo.pedido_id);
    return { id: itemId, vinculado: true, status };
  }

  /**
   * Desfaz o vínculo de um item na conferência: volta o item para
   * tipo='xml_sem_vinculo' (mantém o lado da NF, limpa o lado do pedido) e
   * recalcula o status. O item da NF reaparece em "XML sem vínculo" e o produto
   * do pedido, se ficar sem cobertura, volta para "Pedido sem vínculo".
   */
  async desvincularItemConferencia(itemId: string) {
    const item = await this.repo.findVinculoItemComPedido(itemId);
    if (!item) {
      throw new NotFoundException(`Item de vínculo ${itemId} não encontrado.`);
    }
    await this.repo.desvincularItem(itemId);
    const status = await this.recalcularStatusPedido(item.vinculo.pedido_id);
    return { id: itemId, desvinculado: true, status };
  }

  // ----------------------- Status automático do pedido -----------------------

  /**
   * Recalcula o status do pedido a partir da cobertura dos seus itens
   * (com_pedido_itens) por itens tipo='vinculado' de vínculos confirmados,
   * cruzando com o status no ERP de cada NF (LANCADA = entregue).
   *
   * Por pro_codigo do pedido:
   *  - "faturado" = coberto por ≥1 vínculo confirmado;
   *  - "entregue" = coberto e TODAS as NFs que o cobrem estão LANCADA no ERP.
   *
   * Status resultante:
   *  - Todos os itens entregues               -> 'Entregue' (grava data_recebimento)
   *  - Algum item entregue, mas não todos      -> 'Entregue parcialmente'
   *    (inclui: nem todos os itens vinculados; ou itens vinculados em NFs lançadas
   *     e não lançadas — entrega parcial)
   *  - Nenhuma NF lançada, todos os itens cobertos   -> 'Faturado'
   *  - Nenhuma NF lançada, só parte coberta          -> 'Faturado parcialmente'
   *  - Nenhum item coberto                           -> mantém o status atual
   *
   * Nunca rebaixa 'Entregue' nem mexe em 'Cancelado'. Retorna o status final.
   */
  async recalcularStatusPedido(pedidoId: string): Promise<string | null> {
    const pedido = await this.repo.findPedidoStatus(pedidoId);
    if (!pedido) return null;

    const statusAtual = pedido.status ?? null;

    // Status que nunca devem ser rebaixados/alterados automaticamente.
    if (statusAtual === 'Entregue' || statusAtual === 'Cancelado') {
      return statusAtual;
    }

    const proCodigosPedido = await this.repo.findProCodigosDoPedido(pedidoId);

    // Sem itens no pedido: nada a calcular, mantém o status atual.
    if (!proCodigosPedido.length) return statusAtual;

    const codigosPedido = new Set(proCodigosPedido.map((c) => Number(c)));

    // Mapa pro_codigo -> chaves de NF que o cobrem (vínculos confirmados, tipo='vinculado').
    const itensVinc = await this.repo.findItensVinculadosConfirmados(pedidoId);
    const codigoParaChaves = new Map<number, Set<string>>();
    for (const it of itensVinc) {
      if (it.tipo !== 'vinculado' || it.pro_codigo == null) continue;
      const cod = Number(it.pro_codigo);
      if (!codigosPedido.has(cod)) continue; // ignora o que não é deste pedido
      const chave = it.vinculo?.chave_nfe;
      if (!chave) continue;
      if (!codigoParaChaves.has(cod)) codigoParaChaves.set(cod, new Set());
      codigoParaChaves.get(cod)!.add(chave);
    }

    // Nenhum item contemplado: não mexe no status atual.
    if (codigoParaChaves.size === 0) return statusAtual;

    // Status no ERP de cada chave envolvida (LANCADA = entregue).
    const chaves = [
      ...new Set([...codigoParaChaves.values()].flatMap((s) => [...s])),
    ];
    const concs = await this.repo.findConciliacaoByChaves(chaves);
    const porChave = new Map(concs.map((c) => [c.chave_nfe, c]));
    const lancada = (chave: string) => porChave.get(chave)?.status_erp === 'LANCADA';

    let todosEntregues = true; // todo item coberto e 100% das suas NFs lançadas
    let algumEntregue = false; // existe item com ao menos uma NF lançada
    let todosFaturados = true; // todo item coberto (lançado ou não)
    for (const cod of codigosPedido) {
      const chavesDoCod = codigoParaChaves.get(cod);
      if (!chavesDoCod || chavesDoCod.size === 0) {
        // Item sem nenhuma NF: não é faturado nem entregue.
        todosFaturados = false;
        todosEntregues = false;
        continue;
      }
      const arr = [...chavesDoCod];
      if (!arr.every(lancada)) todosEntregues = false;
      if (arr.some(lancada)) algumEntregue = true;
    }

    let novoStatus = statusAtual;
    if (todosEntregues) {
      // Tudo entregue -> 'Entregue' (data_recebimento = maior dt_entrada das NFs).
      const datas = chaves
        .filter(lancada)
        .map((c) => porChave.get(c)?.dt_entrada)
        .filter((d): d is Date => d instanceof Date);
      const dataRecebimento = datas.length
        ? new Date(Math.max(...datas.map((d) => d.getTime())))
        : new Date();
      await this.repo.marcarPedidoEntregue(pedidoId, dataRecebimento);
      return 'Entregue';
    } else if (algumEntregue) {
      // Parte entregue (NF lançada), mas não tudo: itens faltando vincular OU
      // misto de NFs lançadas/não lançadas.
      novoStatus = 'Entregue parcialmente';
    } else if (todosFaturados) {
      novoStatus = 'Faturado';
    } else {
      novoStatus = 'Faturado parcialmente';
    }

    if (novoStatus && novoStatus !== statusAtual) {
      await this.repo.updatePedidoStatus(pedidoId, novoStatus);
    }

    return novoStatus;
  }

  /** Confirma um vínculo (confirmado=true) e recalcula o status do pedido. */
  async confirmarVinculo(vinculoId: string, usuario?: string | null) {
    const v = await this.repo.findVinculoById(vinculoId);
    if (!v) {
      throw new NotFoundException(`Vínculo ${vinculoId} não encontrado.`);
    }
    // Reaplica o escopo por fornecedor ANTES de confirmar: snapshots antigos (de
    // antes do filtro por fornecedor) podem ter vinculados casados contra item de
    // OUTRO fornecedor da mesma cotação. Esses voltam a 'xml_sem_vinculo' e a lista
    // 'pedido_sem_vinculo' é reconstruída a partir dos itens reais do pedido, para
    // nunca confirmar/consumir saldo de NF num produto que não é deste pedido.
    await this.repo.reescoparVinculoItens(vinculoId, v.pedido_id);
    await this.repo.setVinculoConfirmado(vinculoId, true);
    const status = await this.recalcularStatusPedido(v.pedido_id);

    await this.registrarLogPedido({
      usuario: usuario ?? null,
      acao: 'Confirmar vínculo NF-e',
      descricao: `Confirmou o vínculo da NF-e ${v.chave_nfe} ao pedido ${v.pedido_id}.`,
    });

    return { id: vinculoId, confirmado: true, status };
  }

  /**
   * Rejeita uma sugestão de vínculo: marca como rejeitado (NÃO apaga, para que o
   * auto-vínculo não sugira a mesma NF p/ este pedido de novo) e reverte o status
   * do pedido (volta ao status anterior à sugestão, ficando elegível p/ outras NFs).
   */
  async rejeitarVinculo(vinculoId: string, usuario?: string | null) {
    const v = await this.repo.findVinculoById(vinculoId);
    if (!v) {
      throw new NotFoundException(`Vínculo ${vinculoId} não encontrado.`);
    }
    await this.repo.marcarVinculoRejeitado(vinculoId);
    // Recalcula pelo lado confirmado; se continuar 'Vínculo sugerido' (sem
    // confirmados), reverte ao status anterior à sugestão.
    let status = await this.recalcularStatusPedido(v.pedido_id);
    if (status === 'Vínculo sugerido') {
      status = await this.repo.reverterStatusPedido(v.pedido_id);
    }

    await this.registrarLogPedido({
      usuario: usuario ?? null,
      acao: 'Rejeitar vínculo NF-e',
      descricao: `Rejeitou a sugestão de vínculo da NF-e ${v.chave_nfe} no pedido ${v.pedido_id}.`,
    });

    return { id: vinculoId, rejeitado: true, status };
  }

  /**
   * Reage às NF-e que viraram LANCADA no ERP recalculando o status dos pedidos
   * vinculados. Como a conciliação (com_nfe_conciliacao) já foi marcada LANCADA
   * antes desta chamada, recalcularStatusPedido decide corretamente entre:
   *   - 'Entregue'              (todos os itens do pedido cobertos por NF lançada)
   *   - 'Entregue parcialmente' (parte entregue; itens faltando vincular ou em NF
   *                              ainda não lançada)
   * Nunca rebaixa 'Entregue' nem altera 'Cancelado'. Idempotente.
   */
  async nfLancada(
    lancadas: Array<{ chave_nfe: string; dt_entrada?: string | null }>,
  ): Promise<{ atualizados: number; pedidos: string[] }> {
    const pedidosAtualizados = new Set<string>();
    let atualizados = 0;

    for (const { chave_nfe } of lancadas ?? []) {
      const chave = String(chave_nfe ?? '').trim();
      if (!chave) continue;

      const pedidoIds = await this.repo.findPedidoIdsByChaveConfirmados(chave);
      if (!pedidoIds.length) {
        this.logger.log(`NF lançada ${chave} sem pedido vinculado (confirmado).`);
        continue;
      }

      for (const pedidoId of pedidoIds) {
        const pedido = await this.repo.findPedidoEntrega(pedidoId);
        if (!pedido) continue;

        // Nunca rebaixa/altera pedido Cancelado (recalcular também protege).
        if (pedido.status === 'Cancelado') continue;

        const statusAntes = pedido.status;
        const statusNovo = await this.recalcularStatusPedido(pedidoId);
        if (statusNovo !== statusAntes) {
          atualizados++;
          pedidosAtualizados.add(pedidoId);
        }
      }
    }

    return { atualizados, pedidos: [...pedidosAtualizados] };
  }

  /** Lista as NF-e já salvas de um pedido (cabeçalhos + totais por tipo). */
  async listarVinculosDoPedido(pedidoId: string) {
    return this.repo.findVinculosByPedido(pedidoId);
  }

  /**
   * Carrega uma conferência salva e remonta o MESMO shape de VinculacaoResponse
   * (totais + listas vinculados / xml_sem_vinculo / pedido_sem_vinculo).
   */
  async carregarVinculo(vinculoId: string) {
    const v = await this.repo.findVinculoById(vinculoId);
    if (!v) {
      throw new NotFoundException(`Vínculo ${vinculoId} não encontrado.`);
    }

    const num = (d: Prisma.Decimal | null): number | null =>
      d == null ? null : Number(d);

    const vinculados: ItemVinculado[] = [];
    const xmlSemVinculo: ItemXml[] = [];

    for (const it of v.itens) {
      if (it.tipo === 'vinculado') {
        vinculados.push({
          produto_xml: it.produto_xml ?? '',
          cprod_xml: it.cprod_xml ?? null,
          quantidade_xml: num(it.quantidade_xml),
          vuncom_xml: num(it.vuncom_xml),
          pro_codigo: it.pro_codigo,
          pro_descricao: it.pro_descricao,
          quantidade_cotacao: num(it.quantidade_cotacao),
          quantidade_pedido: num(it.quantidade_pedido),
          valor_pedido: num(it.valor_pedido),
          quantidade_alocada: num(it.quantidade_alocada),
          saldo_nf: null,
          saldo_pedido: null,
          excede_saldo: it.excede_saldo ?? false,
          match_campo: it.match_campo ?? '',
          match_valor: it.match_valor,
          origem: (it.origem as any) ?? 'firebird',
        });
      } else if (it.tipo === 'xml_sem_vinculo') {
        xmlSemVinculo.push({
          cProd: it.cprod_xml ?? '',
          xProd: it.produto_xml ?? '',
          qCom: num(it.quantidade_xml),
          vUnCom: num(it.vuncom_xml),
          vProd: null,
        });
      }
      // Itens tipo 'pedido_sem_vinculo' do snapshot são IGNORADOS de propósito:
      // a lista é RECALCULADA abaixo a partir dos itens REAIS do pedido deste
      // fornecedor (com_pedido_itens). Snapshots antigos podem ter sido gerados
      // sem o escopo por fornecedor e conter itens de toda a cotação.
    }

    // ----------------------------------------------------------------------
    // FILTRO FINAL POR PEDIDO/FORNECEDOR
    // ----------------------------------------------------------------------
    // O pedido_id deste vínculo é, por construção (com_pedido @@unique
    // [pedido_cotacao, for_codigo]), de UM único fornecedor. Logo, os
    // com_pedido_itens desse pedido_id são exatamente "o que de fato foi para o
    // pedido daquele fornecedor". Reaplicamos esse escopo ao snapshot salvo:
    //  - vinculados cujo produto NÃO está no pedido (casados contra a cotação de
    //    outro fornecedor) voltam para "XML sem vínculo";
    //  - "Pedido sem vínculo" passa a ser derivado dos itens reais do pedido.
    const itensPedidoRows = await this.repo.findPedidoItens([v.pedido_id]);
    const pedidoPorCodigo = new Map<string, ItemPedido>();
    for (const r of itensPedidoRows) {
      const key = String(r.pro_codigo);
      if (pedidoPorCodigo.has(key)) continue;
      pedidoPorCodigo.set(key, {
        pro_codigo: r.pro_codigo,
        pro_descricao: r.pro_descricao,
        mar_descricao: r.mar_descricao,
        referencia: r.referencia,
        unidade: r.unidade,
        for_codigo: r.for_codigo,
        quantidade: r.quantidade == null ? null : Number(r.quantidade),
        valor_unitario: r.valor_unitario == null ? null : Number(r.valor_unitario),
      });
    }

    const vinculadosNoPedido: ItemVinculado[] = [];
    for (const vinc of vinculados) {
      const cod = vinc.pro_codigo == null ? null : String(vinc.pro_codigo);
      if (cod != null && pedidoPorCodigo.has(cod)) {
        vinculadosNoPedido.push(vinc);
      } else {
        // Produto não pertence a este pedido: o item da NF volta a ficar sem vínculo.
        xmlSemVinculo.push({
          cProd: vinc.cprod_xml ?? '',
          xProd: vinc.produto_xml ?? '',
          qCom: vinc.quantidade_xml ?? null,
          vUnCom: vinc.vuncom_xml ?? null,
          vProd: null,
        });
      }
    }

    const proCodigosVinculados = new Set(
      vinculadosNoPedido
        .map((x) => x.pro_codigo)
        .filter((c) => c != null)
        .map((c) => String(c)),
    );

    const pedidoSemVinculo = [...pedidoPorCodigo.values()]
      .filter((p) => !proCodigosVinculados.has(String(p.pro_codigo)))
      .map((p) => ({
        pro_codigo: p.pro_codigo,
        pro_descricao: p.pro_descricao,
        mar_descricao: p.mar_descricao,
        referencia: p.referencia,
        unidade: p.unidade,
        for_codigo: p.for_codigo,
        quantidade: p.quantidade,
        valor_unitario: p.valor_unitario,
      }));

    const itensXml = vinculadosNoPedido.length + xmlSemVinculo.length;
    const itensPedido = pedidoPorCodigo.size;

    // Itens da cotação não ficam no snapshot — conta ao vivo (best-effort; se
    // falhar, fica 0 sem quebrar o carregamento da conferência).
    let itensCotacao = 0;
    try {
      const [fb, pg] = await Promise.all([
        this.repo.findCotacaoItens(v.pedido_cotacao),
        this.repo.findCotacaoItensFor(v.pedido_cotacao),
      ]);
      itensCotacao = fb.length + pg.length;
    } catch {
      itensCotacao = 0;
    }

    return {
      vinculo_id: v.id,
      pedido_cotacao: v.pedido_cotacao,
      chave_nfe: v.chave_nfe,
      emitente: v.emitente,
      totais: {
        itens_xml: itensXml,
        itens_cotacao: itensCotacao,
        itens_pedido: itensPedido,
        vinculados: vinculadosNoPedido.length,
        xml_sem_vinculo: xmlSemVinculo.length,
        pedido_sem_vinculo: pedidoSemVinculo.length,
      },
      vinculados: vinculadosNoPedido,
      xml_sem_vinculo: xmlSemVinculo,
      pedido_sem_vinculo: pedidoSemVinculo,
    };
  }

  /** Normaliza o cProd da NF para comparação (trim + remove zeros à esquerda). */
  private normCprod(value?: string | null): string {
    return String(value ?? '').trim().replace(/^0+/, '');
  }

  /**
   * Resumo da vinculação CONFIRMADA por chave de NF (para a listagem de NF):
   * conta os itens do XML que foram vinculados a um pedido x o total de itens
   * do XML, devolvendo o percentual de conclusão e os pedidos envolvidos.
   * Apenas vínculos confirmados entram na conta.
   */
  async resumoVinculacaoPorChaves(chaves: string[]) {
    const limpas = [
      ...new Set((chaves ?? []).map((c) => String(c ?? '').trim()).filter(Boolean)),
    ];
    const out: Record<
      string,
      { total_itens: number; vinculados: number; percentual: number; pedidos: number[] }
    > = {};
    if (!limpas.length) return out;

    const vinculos = await this.repo.findVinculosConfirmadosByChaves(limpas);

    const porChave = new Map<
      string,
      { total: Set<string>; vinc: Set<string>; pedidos: Set<number> }
    >();

    for (const v of vinculos) {
      const acc =
        porChave.get(v.chave_nfe) ??
        { total: new Set<string>(), vinc: new Set<string>(), pedidos: new Set<number>() };
      if (v.pedido_cotacao != null) acc.pedidos.add(Number(v.pedido_cotacao));
      for (const it of v.itens) {
        const key = this.normCprod(it.cprod_xml);
        if (!key) continue;
        acc.total.add(key);
        if (it.tipo === 'vinculado') acc.vinc.add(key);
      }
      porChave.set(v.chave_nfe, acc);
    }

    for (const [chave, acc] of porChave) {
      const total = acc.total.size;
      const vinculados = acc.vinc.size;
      out[chave] = {
        total_itens: total,
        vinculados,
        percentual: total > 0 ? Math.round((vinculados / total) * 100) : 0,
        pedidos: [...acc.pedidos].sort((a, b) => a - b),
      };
    }

    return out;
  }

  /**
   * Detalhe da vinculação CONFIRMADA de uma NF: para cada item do XML que foi
   * vinculado, devolve a qual pedido (pedido_id + pedido_cotacao) ele pertence.
   * Usado na tela de detalhe da NF para mostrar/abrir o pedido de cada item.
   */
  async vinculacaoPorChave(chave: string) {
    const chaveNfe = String(chave ?? '').trim();
    const out = {
      chave_nfe: chaveNfe,
      resumo: { total_itens: 0, vinculados: 0, percentual: 0 },
      pedidos: [] as Array<{ pedido_id: string; pedido_cotacao: number }>,
      itens: [] as Array<{
        cprod_xml: string;
        cprod_norm: string;
        produto_xml: string | null;
        pro_codigo: number | null;
        pedido_id: string;
        pedido_cotacao: number;
      }>,
    };
    if (!chaveNfe) return out;

    const vinculos = await this.repo.findVinculosConfirmadosByChaves([chaveNfe]);

    const totalSet = new Set<string>();
    const vincSet = new Set<string>();
    const pedidosMap = new Map<string, number>();

    for (const v of vinculos) {
      if (v.pedido_cotacao != null) pedidosMap.set(v.pedido_id, Number(v.pedido_cotacao));
      for (const it of v.itens) {
        const norm = this.normCprod(it.cprod_xml);
        if (!norm) continue;
        totalSet.add(norm);
        if (it.tipo === 'vinculado') {
          vincSet.add(norm);
          out.itens.push({
            cprod_xml: it.cprod_xml ?? '',
            cprod_norm: norm,
            produto_xml: it.produto_xml ?? null,
            pro_codigo: it.pro_codigo,
            pedido_id: v.pedido_id,
            pedido_cotacao: Number(v.pedido_cotacao ?? 0),
          });
        }
      }
    }

    out.pedidos = [...pedidosMap.entries()]
      .map(([pedido_id, pedido_cotacao]) => ({ pedido_id, pedido_cotacao }))
      .sort((a, b) => a.pedido_cotacao - b.pedido_cotacao);
    out.resumo = {
      total_itens: totalSet.size,
      vinculados: vincSet.size,
      percentual: totalSet.size > 0 ? Math.round((vincSet.size / totalSet.size) * 100) : 0,
    };

    return out;
  }

  /** Remove um vínculo salvo (cascade apaga os itens). */
  async removerVinculo(vinculoId: string, usuario?: string | null) {
    const v = await this.repo.findVinculoById(vinculoId);
    if (!v) {
      throw new NotFoundException(`Vínculo ${vinculoId} não encontrado.`);
    }
    const pedidoId = v.pedido_id;
    const chaveNfe = v.chave_nfe;
    await this.repo.deleteVinculo(vinculoId);

    // Se não restou nenhum vínculo confirmado, o pedido volta ao estado
    // pré-vinculação: 'Liberado' (mesmo que estivesse 'Entregue'/'Faturado').
    // Restando outros vínculos, recalcula normalmente (Faturado/parcial/Entregue).
    const restantes = await this.repo.findChavesVinculadasConfirmadas(pedidoId);
    const status = restantes.length
      ? await this.recalcularStatusPedido(pedidoId)
      : await this.repo.reverterPedidoParaLiberado(pedidoId);

    await this.registrarLogPedido({
      usuario: usuario ?? null,
      acao: 'Remover vínculo NF-e',
      descricao: `Removeu o vínculo da NF-e ${chaveNfe} do pedido ${pedidoId}.`,
    });

    return { id: vinculoId, removido: true, status };
  }

  // ----------------------------- XML da NF-e --------------------------------

  /** Normaliza o XML vindo do banco (base64+gzip, BLOB gzip, entidades escapadas, BOM). */
  private sanitizeXml(raw: string | Buffer): string {
    let data: Buffer | string = raw;

    // 1) String que parece base64+gzip ("H4sI...") -> bytes comprimidos
    if (typeof data === 'string') {
      const s = data.trim();
      if (s.startsWith('H4sI') && /^[A-Za-z0-9+/=\s]+$/.test(s)) {
        try {
          data = Buffer.from(s, 'base64');
        } catch {
          /* mantém string */
        }
      }
    }

    // 2) Buffer: tenta descompactar gzip/deflate, depois decodifica
    if (Buffer.isBuffer(data)) {
      const b = data;
      const isGzip = b[0] === 0x1f && b[1] === 0x8b;
      const isZlib = b[0] === 0x78 && [0x9c, 0xda, 0x01].includes(b[1]);
      if (isGzip || isZlib) {
        const dec = this.tryDecompress(b);
        if (dec) data = dec;
      }
      data = Buffer.isBuffer(data) ? data.toString('utf-8') : data;
    }

    let s = String(data);
    s = s.replace(/^﻿/, '').replace(/^￾/, '').replace(/\x00/g, '');

    // XML escapado (&lt;nfeProc...)
    if (!s.includes('<') && s.includes('&lt;')) {
      s = s
        .replace(/&lt;/g, '<')
        .replace(/&gt;/g, '>')
        .replace(/&quot;/g, '"')
        .replace(/&apos;/g, "'")
        .replace(/&amp;/g, '&');
    }

    const idx = s.indexOf('<');
    if (idx > 0) s = s.slice(idx);
    return s.trim();
  }

  private tryDecompress(b: Buffer): Buffer | null {
    const tentativas = [
      () => zlib.gunzipSync(b),
      () => zlib.inflateSync(b),
      () => zlib.inflateRawSync(b),
    ];
    for (const fn of tentativas) {
      try {
        return fn();
      } catch {
        /* tenta o próximo */
      }
    }
    return null;
  }

  /** Extrai os itens (cProd, xProd, qCom, vUnCom, vProd) do XML via regex. */
  private parseItensNfe(raw: string | Buffer): ItemXml[] {
    const xml = this.sanitizeXml(raw);

    // Cada <prod>...</prod> é um item (aceita prefixo de namespace, ex.: <ns:prod>).
    const prodRe = /<(?:\w+:)?prod\b[^>]*>([\s\S]*?)<\/(?:\w+:)?prod>/gi;
    const itens: ItemXml[] = [];

    let m: RegExpExecArray | null;
    while ((m = prodRe.exec(xml)) !== null) {
      const bloco = m[1];
      itens.push({
        cProd: this.tagText(bloco, 'cProd'),
        xProd: this.tagText(bloco, 'xProd'),
        qCom: this.toNumber(this.tagText(bloco, 'qCom')),
        vUnCom: this.toNumber(this.tagText(bloco, 'vUnCom')),
        vProd: this.toNumber(this.tagText(bloco, 'vProd')),
      });
    }
    return itens;
  }

  /**
   * Mapa cProd (normalizado) -> ajustes POR UNIDADE da NF, já na UNIDADE BASE:
   * fator de conversão, preço unitário, IPI, desconto e acréscimo.
   *
   * Iteramos por <det> (que contém <prod> + <imposto>). O <prod> traz a unidade
   * comercial (uCom/qCom/vUnCom) e a tributável (uTrib/qTrib/vUnTrib); vDesc/vOutro
   * estão no <prod> e vIPI em <imposto><IPI>. Os três (desc/acrésc/IPI) são TOTAIS
   * da linha.
   *
   * Conversão de unidade: quando uCom != uTrib (ex.: NF em CJ e estoque em UN, com
   * uTrib=UN/qTrib=300/vUnTrib=preço por UN), passamos a medir na unidade tributável
   * (base): fator = qTrib/qCom, preço = vUnTrib, e os totais são divididos por qTrib.
   * Quando as unidades coincidem, fica tudo na comercial (fator=1, /qCom), como antes.
   */
  private parseAjustesUnitPorCprod(
    raw: string | Buffer,
  ): Map<string, { fator: number; precoUnit: number; ipiUnit: number; descUnit: number; outroUnit: number }> {
    const xml = this.sanitizeXml(raw);
    const detRe = /<(?:\w+:)?det\b[^>]*>([\s\S]*?)<\/(?:\w+:)?det>/gi;
    const normUnit = (s: string) => String(s || '').trim().toUpperCase();

    interface Acc {
      vipi: number; vdesc: number; voutro: number;
      qcom: number; qtrib: number;
      uCom: string; uTrib: string;
      vUnCom: number; vUnTrib: number;
    }
    const acc = new Map<string, Acc>();

    let m: RegExpExecArray | null;
    while ((m = detRe.exec(xml)) !== null) {
      const bloco = m[1];
      const cprod = this.normRef(this.tagText(bloco, 'cProd'));
      if (!cprod) continue;
      const qcom = this.toNumber(this.tagText(bloco, 'qCom')) ?? 0;
      const qtrib = this.toNumber(this.tagText(bloco, 'qTrib')) ?? 0;
      const vipi = this.toNumber(this.tagText(bloco, 'vIPI')) ?? 0;
      const vdesc = this.toNumber(this.tagText(bloco, 'vDesc')) ?? 0;
      const voutro = this.toNumber(this.tagText(bloco, 'vOutro')) ?? 0;
      const vUnCom = this.toNumber(this.tagText(bloco, 'vUnCom')) ?? 0;
      const vUnTrib = this.toNumber(this.tagText(bloco, 'vUnTrib')) ?? 0;
      const uCom = normUnit(this.tagText(bloco, 'uCom'));
      const uTrib = normUnit(this.tagText(bloco, 'uTrib'));

      const a = acc.get(cprod) ?? {
        vipi: 0, vdesc: 0, voutro: 0, qcom: 0, qtrib: 0,
        uCom: '', uTrib: '', vUnCom: 0, vUnTrib: 0,
      };
      a.vipi += Number.isFinite(vipi) ? vipi : 0;
      a.vdesc += Number.isFinite(vdesc) ? vdesc : 0;
      a.voutro += Number.isFinite(voutro) ? voutro : 0;
      a.qcom += Number.isFinite(qcom) ? qcom : 0;
      a.qtrib += Number.isFinite(qtrib) ? qtrib : 0;
      if (uCom) a.uCom = uCom;
      if (uTrib) a.uTrib = uTrib;
      if (vUnCom > 0) a.vUnCom = vUnCom;
      if (vUnTrib > 0) a.vUnTrib = vUnTrib;
      acc.set(cprod, a);
    }

    const out = new Map<string, { fator: number; precoUnit: number; ipiUnit: number; descUnit: number; outroUnit: number }>();
    for (const [cprod, v] of acc) {
      const converter = !!v.uCom && !!v.uTrib && v.uCom !== v.uTrib && v.qcom > 0 && v.qtrib > 0;
      const fator = converter ? v.qtrib / v.qcom : 1;
      const denom = (converter ? v.qtrib : v.qcom) > 0 ? (converter ? v.qtrib : v.qcom) : 1;
      const precoUnit = converter ? v.vUnTrib : v.vUnCom;
      out.set(cprod, {
        fator,
        precoUnit,
        ipiUnit: v.vipi / denom,
        descUnit: v.vdesc / denom,
        outroUnit: v.voutro / denom,
      });
    }
    return out;
  }

  /** Extrai o texto de uma tag dentro de um trecho de XML (aceita prefixo de namespace). */
  private tagText(xml: string, tag: string): string {
    const re = new RegExp(`<(?:\\w+:)?${tag}\\b[^>]*>([\\s\\S]*?)</(?:\\w+:)?${tag}>`, 'i');
    const m = re.exec(xml);
    if (!m) return '';
    return this.desescapar(m[1]).trim();
  }

  /** Desescapa entidades XML básicas. */
  private desescapar(s: string): string {
    return s
      .replace(/&lt;/g, '<')
      .replace(/&gt;/g, '>')
      .replace(/&quot;/g, '"')
      .replace(/&apos;/g, "'")
      .replace(/&amp;/g, '&');
  }

  private toNumber(v: any): number | null {
    if (v == null) return null;
    const s = String(v).trim();
    if (!s) return null;
    const n = Number(s.replace(',', '.'));
    return Number.isFinite(n) ? n : null;
  }

  // --------------------------- Itens da cotação -----------------------------

  /** Unifica itens do Firebird e do Postgres numa única lista normalizada. */
  private unificarItensCotacao(
    fb: CotacaoItemRow[],
    pg: Array<Record<string, any>>,
  ): ItemCotacao[] {
    let idx = 0;
    const out: ItemCotacao[] = [];

    for (const it of fb) {
      out.push({
        _idx: idx++,
        _origem: 'firebird',
        pro_codigo: it.pro_codigo,
        pro_descricao: it.pro_descricao,
        mar_descricao: it.mar_descricao,
        referencia: it.referencia,
        ref_fabricante: it.ref_fabricante,
        ref_fornecedor: it.ref_fornecedor,
        unidade: it.unidade,
        quantidade: this.toNumber(it.quantidade),
        valor_unitario: null,
        for_codigo: null,
      });
    }

    for (const it of pg) {
      out.push({
        _idx: idx++,
        _origem: 'pg',
        pro_codigo: it.pro_codigo ?? null,
        pro_descricao: it.pro_descricao ?? null,
        mar_descricao: it.mar_descricao ?? null,
        referencia: it.referencia ?? null,
        ref_fabricante: null,
        ref_fornecedor: it.ref_fornecedor ?? null,
        unidade: it.unidade ?? null,
        quantidade: this.toNumber(it.quantidade),
        valor_unitario: it.valor_unitario == null ? null : Number(it.valor_unitario),
        for_codigo: it.for_codigo ?? null,
      });
    }

    return out;
  }

  // ------------------------------ Vinculação --------------------------------

  private normRef(s: any): string {
    if (s == null) return '';
    // Remove espaços, padroniza maiúsculas e ignora zeros à esquerda — o cProd da NF
    // costuma vir preenchido com zeros (ex.: 000000000004904246) enquanto a referência
    // do produto é 4904246. Mantém ao menos 1 caractere (ex.: '0000' -> '0').
    return String(s).replace(/\s+/g, '').toUpperCase().replace(/^0+(?=.)/, '');
  }

  /** Extrai o "código" do início do xProd, ex.: 'FCA1130DS PARA-BRISAS ...' -> 'FCA1130DS'. */
  private refDoXprod(xprod: string): string {
    if (!xprod) return '';
    let token = xprod.trim().split(/\s+/)[0] ?? '';
    token = token.replace(/^[^\w]+|[^\w]+$/g, '');
    if (token.length < 3) return '';
    const temDigito = /\d/.test(token);
    const todoUpper = token === token.toUpperCase() && /[A-Za-z]/.test(token);
    return temDigito || todoUpper ? token : '';
  }

  /** Indexa os itens da cotação por coluna -> valor normalizado -> itens. */
  private indexarCotacao(itens: ItemCotacao[]): Record<ColunaCotacao, Map<string, ItemCotacao[]>> {
    const idx = {} as Record<ColunaCotacao, Map<string, ItemCotacao[]>>;
    for (const col of COLUNAS_COTACAO) idx[col] = new Map();
    for (const it of itens) {
      for (const col of COLUNAS_COTACAO) {
        const k = this.normRef((it as any)[col]);
        if (!k) continue;
        const arr = idx[col].get(k);
        if (arr) arr.push(it);
        else idx[col].set(k, [it]);
      }
    }
    return idx;
  }

  /** Tenta casar um item do XML contra as colunas da cotação (cProd e xProd[0]). */
  private encontrarMatch(
    item: ItemXml,
    indices: Record<ColunaCotacao, Map<string, ItemCotacao[]>>,
  ): { item: ItemCotacao; campo: string; valor: string | null } | null {
    const chaves: Record<string, string> = {
      cProd: this.normRef(item.cProd),
      'xProd[0]': this.normRef(this.refDoXprod(item.xProd)),
    };

    for (const chave of Object.values(chaves)) {
      if (!chave) continue;
      for (const col of COLUNAS_COTACAO) {
        const cands = indices[col].get(chave);
        if (!cands || !cands.length) continue;
        for (const alvo of cands) {
          // Mesmo casando por código, barra modelo/cor/lado/presença divergentes —
          // protege contra colisão do cProd do fornecedor com o nosso pro_codigo.
          if (this.conflitaAtributos(item.xProd, this.cotacaoDesc(alvo))) continue;
          const campo =
            col === 'ref_fornecedor' && alvo._origem === 'pg' ? 'ref_fornecedor_pg' : col;
          return { item: alvo, campo, valor: this.normRef((alvo as any)[col]) };
        }
      }
    }
    return null;
  }

  // -------------------------- Análise semântica -----------------------------

  private static readonly ABREV: Record<string, string> = {
    'P/BRISA': 'PARABRISA',
    'PARA-BRISAS': 'PARABRISA',
    'PARA-BRISA': 'PARABRISA',
    PARABRISAS: 'PARABRISA',
    'DIANT.': 'DIANTEIRO',
    'TRAS.': 'TRASEIRO',
    'DIR.': 'DIREITO',
    'ESQ.': 'ESQUERDO',
    // C/P e C/PISCA = COM PISCA (chaves mais longas são processadas primeiro,
    // então 'C/PISCA' é resolvida antes de 'C/P' e ambas antes de 'C/').
    'C/PISCA': ' COM PISCA ',
    'C/P': ' COM PISCA ',
    'C/': ' COM ',
    'S/': ' SEM ',
    'P/': ' PARA ',
  };

  private static readonly STOP = new Set([
    'DE', 'DA', 'DO', 'DAS', 'DOS', 'PARA', 'COM', 'SEM', 'E', 'OU',
    'A', 'O', 'AS', 'OS', 'UM', 'UMA', 'EM', 'POR',
  ]);

  private normalizarTexto(s: any): string {
    if (!s) return '';
    let t = String(s).normalize('NFKD').replace(/[̀-ͯ]/g, '').toUpperCase();
    for (const k of Object.keys(VinculacaoNfeService.ABREV).sort((a, b) => b.length - a.length)) {
      // Pad com espaços: a abreviação costuma consumir o separador (ex.: 'TRAS.'),
      // então sem o espaço a expansão gruda na palavra seguinte
      // ('LANT.TRAS.SANDERO' -> 'LANT.TRASEIROSANDERO', perdendo o token SANDERO).
      t = t.split(k).join(` ${VinculacaoNfeService.ABREV[k]} `);
    }
    // Cola hífen entre letra e dígito (nome de modelo): 'S-10' -> 'S10'. NÃO toca
    // dígito-dígito ('11-14'), preservando faixas de ano.
    t = t.replace(/([A-Z])-(\d)/g, '$1$2').replace(/(\d)-([A-Z])/g, '$1$2');
    t = t.replace(/[^A-Z0-9 ]/g, ' ').replace(/\s+/g, ' ').trim();
    return t;
  }

  /**
   * Sinônimos/abreviações de catálogo, canonizados por TOKEN (não por substring,
   * p/ não corromper palavras). Ex.: LANT/LAN -> LANTERNA, TRAS -> TRASEIRA,
   * LE/LD -> ESQUERDO/DIREITO. Faz o nome do produto ("Lanterna Traseira") e o
   * lado casarem mesmo escritos de formas diferentes nos dois lados.
   */
  private static readonly SINONIMOS: Record<string, string> = {
    LANT: 'LANTERNA', LAN: 'LANTERNA', LANTERNA: 'LANTERNA',
    TRAS: 'TRASEIRA', TRASEIRO: 'TRASEIRA', TRASEIRA: 'TRASEIRA',
    DIANT: 'DIANTEIRA', DIANTEIRO: 'DIANTEIRA', DIANTEIRA: 'DIANTEIRA',
    RETR: 'RETROVISOR', RETROVISOR: 'RETROVISOR',
    CROM: 'CROMADO', CROMADO: 'CROMADO',
    // 'PB' (abreviação do fornecedor) = parabrisa. P/BRISA, PARA-BRISA, PARA-BRISAS
    // e PARABRISAS já viram PARABRISA na ABREV; aqui canonizamos o token 'PB' para
    // o mesmo valor, casando por TOKEN (não corrompe códigos como 'ABPB12').
    PB: 'PARABRISA', PARABRISA: 'PARABRISA',
    // Para-lama / para-barro / protetor de lama = mesmo produto (canônico PARALAMA).
    // 'P/LAMA' e 'P/BARRO' viram 'PARA LAMA'/'PARA BARRO' (PARA é stopword) -> LAMA/BARRO.
    PARALAMA: 'PARALAMA', PARALAMAS: 'PARALAMA', PARABARRO: 'PARALAMA', PARABARROS: 'PARALAMA',
    LAMA: 'PARALAMA', BARRO: 'PARALAMA',
    LE: 'ESQUERDO', LD: 'DIREITO', ESQ: 'ESQUERDO', DIR: 'DIREITO',
    ESQUERDA: 'ESQUERDO', ESQUERDO: 'ESQUERDO', DIREITA: 'DIREITO', DIREITO: 'DIREITO',
  };

  private tokensSemanticos(s: any): Set<string> {
    return new Set(
      this.normalizarTexto(s)
        .split(' ')
        .filter((tk) => tk.length >= 2 && !VinculacaoNfeService.STOP.has(tk))
        .map((tk) => VinculacaoNfeService.SINONIMOS[tk] ?? tk),
    );
  }

  /** Bônus de score quando o valor unitário da NF bate com o do pedido/cotação. */
  private static readonly VALOR_BONUS = 0.25;

  /** Bônus de score quando as faixas de ano da NF e do pedido se sobrepõem. */
  private static readonly ANO_BONUS = 0.15;

  /** Valor unitário bate (mesmo preço negociado): tolerância de 1% ou 2 centavos. */
  private valorUnitarioBate(a: number | null, b: number | null): boolean {
    if (a == null || b == null) return false;
    if (!Number.isFinite(a) || !Number.isFinite(b) || a <= 0 || b <= 0) return false;
    return Math.abs(a - b) <= Math.max(0.02, 0.01 * Math.max(a, b));
  }

  /** Expande um ano (2 ou 4 dígitos) para 4 dígitos. Ex.: '11' -> 2011, '98' -> 1998. */
  private expandAno(n: string): number | null {
    const d = String(n).replace(/\D/g, '');
    if (d.length === 4) {
      const y = Number(d);
      return y >= 1900 && y <= 2099 ? y : null;
    }
    if (d.length === 2) {
      const y = Number(d);
      return y <= 50 ? 2000 + y : 1900 + y;
    }
    return null;
  }

  /**
   * Extrai a faixa de anos de uma descrição (ex.: '11/14' -> 2011..2014,
   * '2012/2014' -> 2012..2014, ou um ano 4 dígitos solto). Retorna a faixa e os
   * tokens crus de ano encontrados (para poder removê-los da comparação textual).
   */
  private extrairAnos(text: string): { min: number; max: number; tokens: Set<string> } | null {
    if (!text) return null;
    // Cola modelo 'S-10' -> 'S10' p/ o '10' não ser lido como ano. Preserva faixas
    // dígito-dígito ('11-14').
    const s = String(text)
      .toUpperCase()
      .replace(/([A-Z])-(\d)/g, '$1$2')
      .replace(/(\d)-([A-Z])/g, '$1$2');
    const anos: number[] = [];
    const tokens = new Set<string>();

    // Faixas NN/NN, NNNN/NNNN, NN-NN, etc.
    for (const m of s.matchAll(/(\d{2,4})\s*[\/\-]\s*(\d{2,4})/g)) {
      const a = this.expandAno(m[1]);
      const b = this.expandAno(m[2]);
      if (a != null) { anos.push(a); tokens.add(m[1]); }
      if (b != null) { anos.push(b); tokens.add(m[2]); }
    }
    // Anos de 4 dígitos soltos (19xx / 20xx).
    for (const m of s.matchAll(/\b(?:19|20)\d{2}\b/g)) {
      anos.push(Number(m[0]));
      tokens.add(m[0]);
    }
    // Anos de 2 dígitos soltos (ex.: 'UNO 04' -> 2004). Só afeta o bônus/strip de
    // ano (nunca bloqueia o match), então o risco de confundir com medida é baixo.
    for (const m of s.matchAll(/\b\d{2}\b/g)) {
      const y = this.expandAno(m[0]);
      if (y != null) { anos.push(y); tokens.add(m[0]); }
    }
    if (!anos.length) return null;
    return { min: Math.min(...anos), max: Math.max(...anos), tokens };
  }

  /** Duas faixas de anos se sobrepõem? Ex.: [2011,2014] e [2012,2014] -> true. */
  private anosSobrepoe(
    a: { min: number; max: number } | null,
    b: { min: number; max: number } | null,
  ): boolean {
    if (!a || !b) return false;
    return a.min <= b.max && b.min <= a.max;
  }

  /** Léxico de cores (variações de gênero/número -> cor canônica). */
  // Inclui abreviações comuns do fornecedor/cadastro (ex.: AMAR=amarelo, VERM=vermelho,
  // PTO/PRET=preto, BCO/BRAN=branco). Assim cores abreviadas também são reconhecidas e
  // continuam bloqueando combinações divergentes (amarelo x rosa).
  private static readonly CORES: Record<string, string> = {
    PRETA: 'PRETO', PRETO: 'PRETO', PRET: 'PRETO', PTO: 'PRETO', NEGRA: 'PRETO', NEGRO: 'PRETO',
    BRANCA: 'BRANCO', BRANCO: 'BRANCO', BRAN: 'BRANCO', BCO: 'BRANCO',
    VERMELHA: 'VERMELHO', VERMELHO: 'VERMELHO', VERM: 'VERMELHO',
    AZUL: 'AZUL', AZULADO: 'AZUL',
    VERDE: 'VERDE', VERD: 'VERDE',
    AMARELA: 'AMARELO', AMARELO: 'AMARELO', AMAR: 'AMARELO',
    CINZA: 'CINZA', CINZ: 'CINZA', GRAFITE: 'CINZA',
    PRATA: 'PRATA', PRATEADO: 'PRATA', PRAT: 'PRATA',
    DOURADA: 'DOURADO', DOURADO: 'DOURADO', DOUR: 'DOURADO',
    FUME: 'FUME', FUMACE: 'FUME',
    CRISTAL: 'CRISTAL',
    ROSA: 'ROSA',
    LARANJA: 'LARANJA', LARAN: 'LARANJA', LRJ: 'LARANJA',
    MARROM: 'MARROM', MARR: 'MARROM',
    BEGE: 'BEGE', VINHO: 'VINHO', CHAMPANHE: 'CHAMPANHE',
  };

  /** Extrai as cores canônicas presentes numa descrição (ex.: 'BORDA PRETA' -> {PRETO}). */
  private extrairCores(text: string): Set<string> {
    const out = new Set<string>();
    if (!text) return out;
    for (const tk of this.normalizarTexto(text).split(' ')) {
      const c = VinculacaoNfeService.CORES[tk];
      if (c) out.add(c);
    }
    return out;
  }

  /**
   * Extrai recursos marcados como COM (presente) ou SEM (ausente). normalizarTexto
   * já expande 'C/' -> 'COM' e 'S/' -> 'SEM'. Ex.: 'C/LED' -> com={LED};
   * 'S/LED' -> sem={LED}. Usado para barrar 'com LED' x 'sem LED'.
   */
  private extrairPresenca(text: string): { com: Set<string>; sem: Set<string> } {
    const com = new Set<string>();
    const sem = new Set<string>();
    if (!text) return { com, sem };
    const toks = this.normalizarTexto(text).split(' ');
    for (let i = 0; i < toks.length - 1; i++) {
      const prox = toks[i + 1];
      if (!prox) continue;
      if (toks[i] === 'COM') com.add(prox);
      else if (toks[i] === 'SEM') sem.add(prox);
    }
    return { com, sem };
  }

  /** Conflito de presença: um lado diz COM X e o outro SEM X (ex.: com LED x sem LED). */
  private presencaConflita(
    a: { com: Set<string>; sem: Set<string> },
    b: { com: Set<string>; sem: Set<string> },
  ): boolean {
    for (const f of a.com) if (b.sem.has(f)) return true;
    for (const f of a.sem) if (b.com.has(f)) return true;
    return false;
  }

  /** Lado (esquerdo/direito): LE/ESQ -> ESQUERDO, LD/DIR -> DIREITO. */
  private static readonly LADOS: Record<string, string> = {
    LE: 'ESQUERDO', ESQ: 'ESQUERDO', ESQUERDA: 'ESQUERDO', ESQUERDO: 'ESQUERDO',
    LD: 'DIREITO', DIR: 'DIREITO', DIREITA: 'DIREITO', DIREITO: 'DIREITO',
  };

  /** Extrai os lados (ESQUERDO/DIREITO) citados numa descrição. */
  private extrairLado(text: string): Set<string> {
    const out = new Set<string>();
    if (!text) return out;
    for (const tk of this.normalizarTexto(text).split(' ')) {
      const l = VinculacaoNfeService.LADOS[tk];
      if (l) out.add(l);
    }
    return out;
  }

  /**
   * Modelos de veículo (token único, MAIÚSCULO, sem acento). Lista extensível —
   * inclua novos modelos aqui. 'S-10' já é normalizado p/ 'S10' antes de chegar aqui.
   */
  private static readonly MODELOS = new Set<string>([
    // VW
    'GOL', 'VOYAGE', 'SAVEIRO', 'PARATI', 'SANTANA', 'FOX', 'CROSSFOX', 'SPACEFOX',
    'POLO', 'VIRTUS', 'NIVUS', 'TCROSS', 'UP', 'FUSCA', 'KOMBI', 'JETTA', 'BORA',
    'GOLF', 'PASSAT', 'TIGUAN', 'AMAROK',
    // Fiat
    'UNO', 'MILLE', 'PALIO', 'SIENA', 'STRADA', 'WEEKEND', 'IDEA', 'PUNTO', 'LINEA',
    'BRAVO', 'TIPO', 'ARGO', 'CRONOS', 'MOBI', 'TORO', 'FIORINO', 'DOBLO', 'PULSE',
    'FASTBACK',
    // GM / Chevrolet
    'ONIX', 'PRISMA', 'CELTA', 'CORSA', 'CLASSIC', 'COBALT', 'AGILE', 'MONTANA',
    'S10', 'BLAZER', 'SPIN', 'TRACKER', 'CRUZE', 'ASTRA', 'VECTRA', 'ZAFIRA',
    'MERIVA', 'MONZA', 'KADETT', 'OMEGA', 'CAMARO', 'EQUINOX',
    // Ford
    'KA', 'FIESTA', 'FOCUS', 'FUSION', 'ECOSPORT', 'ESCORT', 'RANGER', 'EDGE',
    'TERRITORY', 'MAVERICK', 'COURIER', 'BELINA',
    // Toyota
    'COROLLA', 'ETIOS', 'YARIS', 'HILUX', 'SW4', 'RAV4', 'CAMRY',
    // Honda
    'CIVIC', 'FIT', 'CITY', 'HRV', 'WRV', 'ACCORD', 'CRV',
    // Renault
    'SANDERO', 'LOGAN', 'DUSTER', 'KWID', 'CAPTUR', 'OROCH', 'STEPWAY', 'CLIO',
    'MEGANE', 'SCENIC', 'KANGOO', 'SYMBOL', 'FLUENCE',
    // Nissan
    'MARCH', 'VERSA', 'KICKS', 'SENTRA', 'FRONTIER', 'LIVINA', 'TIIDA',
    // Hyundai
    'HB20', 'HB20S', 'CRETA', 'TUCSON', 'IX35', 'AZERA', 'SANTAFE', 'I30', 'ELANTRA',
    // Kia
    'CERATO', 'SPORTAGE', 'SORENTO', 'PICANTO', 'BONGO', 'SOUL',
    // Mitsubishi
    'L200', 'PAJERO', 'OUTLANDER', 'ASX', 'LANCER', 'TRITON', 'ECLIPSE',
    // Jeep / outros
    'RENEGADE', 'COMPASS', 'COMMANDER', 'CHEROKEE',
  ]);

  /** Extrai os modelos de veículo reconhecidos numa descrição. */
  private extrairModelos(text: string): Set<string> {
    const out = new Set<string>();
    if (!text) return out;
    for (const tk of this.normalizarTexto(text).split(' ')) {
      if (VinculacaoNfeService.MODELOS.has(tk)) out.add(tk);
    }
    return out;
  }

  /**
   * Conflito de atributo distintivo entre duas descrições: modelo de veículo, cor,
   * lado ou presença (com/sem). Cada checagem só "trava" quando AMBOS os lados
   * declaram o atributo e nenhum coincide. Usado tanto no match por código quanto
   * no semântico — um produto de modelo/cor/lado diferente nunca deve casar, mesmo
   * que o código do fornecedor colida com o nosso (ex.: cProd 038883 = código 38883).
   */
  private conflitaAtributos(xmlDesc: string, cotDesc: string): boolean {
    const disjunto = (a: Set<string>, b: Set<string>) =>
      a.size > 0 && b.size > 0 && ![...a].some((x) => b.has(x));

    if (disjunto(this.extrairModelos(xmlDesc), this.extrairModelos(cotDesc))) return true;
    if (disjunto(this.extrairCores(xmlDesc), this.extrairCores(cotDesc))) return true;
    if (disjunto(this.extrairLado(xmlDesc), this.extrairLado(cotDesc))) return true;
    if (this.presencaConflita(this.extrairPresenca(xmlDesc), this.extrairPresenca(cotDesc))) {
      return true;
    }
    return false;
  }

  /** Junta os campos textuais de um item de cotação para análise de atributos. */
  private cotacaoDesc(it: ItemCotacao): string {
    return [it.pro_descricao, it.referencia, it.ref_fabricante, it.ref_fornecedor]
      .filter(Boolean)
      .join(' ');
  }

  /**
   * Fallback: maior sobreposição de tokens entre xProd e descrição da cotação.
   * O valor unitário da NF (vUnCom) é usado como reforço: quando bate com o preço
   * do item (cotação pg ou pedido), soma um bônus ao score e relaxa o mínimo de
   * tokens p/ 2 — é o mesmo preço negociado, evidência forte de ser o mesmo item.
   */
  private matchSemantico(
    item: ItemXml,
    itens: ItemCotacao[],
    usados: Set<number>,
    precoPorCodigo: Map<string, number> = new Map(),
    threshold = 0.7,
    minIntersec = 3,
  ): { item: ItemCotacao; campo: string; valor: string | null } | null {
    const toksXml = this.tokensSemanticos(item.xProd);
    if (toksXml.size < 2) return null;

    const vXml = item.vUnCom == null ? null : Number(item.vUnCom);
    const anosXml = this.extrairAnos(item.xProd);

    let best: ItemCotacao | null = null;
    let bestInter: string[] = [];
    let bestScore = 0;
    let bestVlrBate = false;
    let bestAnoBate = false;

    for (const it of itens) {
      if (usados.has(it._idx)) continue;
      const haystack = [it.pro_descricao, it.referencia, it.ref_fabricante, it.ref_fornecedor]
        .filter(Boolean)
        .join(' ');
      let toksCot = this.tokensSemanticos(haystack);
      if (!toksCot.size) continue;

      // Atributo distintivo divergente (modelo/cor/lado/presença) => não casa.
      if (this.conflitaAtributos(item.xProd, haystack)) continue;

      // Faixa de anos: faixas que se sobrepõem são compatíveis (ex.: NF 2012/2014
      // x pedido 11/14 = 2011..2014). Quando sobrepõem, removemos os tokens de ano
      // da comparação (param de diluir) e damos um bônus. Quando conflitam, mantemos
      // os tokens p/ diferenciar produtos que só mudam de ano.
      const anosCot = this.extrairAnos(haystack);
      const anoBate = this.anosSobrepoe(anosXml, anosCot);

      let toksXmlCmp = toksXml;
      if (anoBate) {
        const anoTokens = new Set([
          ...(anosXml?.tokens ?? []),
          ...(anosCot?.tokens ?? []),
        ]);
        toksXmlCmp = new Set([...toksXml].filter((tk) => !anoTokens.has(tk)));
        toksCot = new Set([...toksCot].filter((tk) => !anoTokens.has(tk)));
      }
      if (!toksXmlCmp.size || !toksCot.size) continue;

      const inter = [...toksXmlCmp].filter((tk) => toksCot.has(tk));

      // Preço do candidato: cotação pg traz valor_unitario; senão usa o do pedido.
      const precoCand =
        it.valor_unitario ?? precoPorCodigo.get(String(it.pro_codigo)) ?? null;
      const vlrBate = this.valorUnitarioBate(vXml, precoCand);

      // Com valor ou ano batendo, basta sobreposição de 2 tokens; senão, o mínimo padrão.
      const minReq = vlrBate || anoBate ? 2 : minIntersec;
      if (inter.length < minReq) continue;

      const base = inter.length / Math.min(toksXmlCmp.size, toksCot.size);
      const score = Math.min(
        1,
        base +
          (vlrBate ? VinculacaoNfeService.VALOR_BONUS : 0) +
          (anoBate ? VinculacaoNfeService.ANO_BONUS : 0),
      );
      if (score > bestScore) {
        bestScore = score;
        best = it;
        bestInter = inter;
        bestVlrBate = vlrBate;
        bestAnoBate = anoBate;
      }
    }

    if (best && bestScore >= threshold) {
      const extras = [
        bestVlrBate ? 'Vlr igual' : null,
        bestAnoBate ? 'Ano compat.' : null,
      ].filter(Boolean);
      return {
        item: best,
        campo: 'Analise semantica',
        valor: `${Math.round(bestScore * 100)}% | ${bestInter.sort().join(' ')}${extras.length ? ' | ' + extras.join(' ') : ''}`,
      };
    }
    return null;
  }
}
