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

const COLUNAS_COTACAO = ['pro_codigo', 'referencia', 'ref_fabricante', 'ref_fornecedor'] as const;
type ColunaCotacao = (typeof COLUNAS_COTACAO)[number];

@Injectable()
export class VinculacaoNfeService {
  private readonly logger = new Logger(VinculacaoNfeService.name);

  constructor(
    private readonly repo: VinculacaoNfeRepository,
    private readonly grupo: FornecedorGrupoService,
  ) {}

  /**
   * Pipeline completo: busca XML da NF-e, busca itens da cotação e do pedido,
   * vincula e devolve as 3 listas (vinculados, XML sem vínculo, pedido sem vínculo).
   */
  async vincular(pedido: number, nfe: string) {
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

    // 2) Itens da cotação (Firebird) + com_cotacao_itens_for (Postgres)
    const [itensFb, itensPg] = await Promise.all([
      this.repo.findCotacaoItens(pedido),
      this.repo.findCotacaoItensFor(pedido),
    ]);
    // Preenche ref_fornecedor em branco usando a referência do grupo (matriz/filiais),
    // para que a NF-e de um relacionado case mesmo sem a referência gravada nesta cotação.
    await this.grupo.enriquecerRefsEmBranco(itensPg as any);
    const itensCotacao = this.unificarItensCotacao(itensFb, itensPg);

    // 3) Itens do pedido (com_pedido / com_pedido_itens) — mapa por pro_codigo
    //    (mantém o registro mais recente, já que vem ordenado por emissao DESC).
    const pedidoIds = await this.repo.findPedidoIds(pedido);
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

    for (const it of itensVinculo) {
      const chave = it.vinculo?.chave_nfe ?? '';
      if (chave) chavesFaturadas.add(chave);
      if (it.tipo === 'vinculado') {
        if (it.pro_codigo == null) continue;
        const cod = Number(it.pro_codigo);
        const atual: AggFaturado =
          faturadoPorCodigo.get(cod) ??
          { quantidade_faturada: 0, valor_faturado: 0, chaves_nfe: new Set<string>(), contribs: [], excede_saldo: false, xmlProds: new Map(), itemIds: [] };
        // Base do faturado = quantidade_alocada (quanto deste item da NF foi para
        // ESTE pedido), evitando dobrar quando a NF é repartida entre pedidos.
        // Fallback p/ quantidade_xml em vínculos antigos sem alocação gravada.
        const q = num(it.quantidade_alocada ?? it.quantidade_xml);
        atual.quantidade_faturada += q;
        // "última" vuncom_xml: sobrescreve com o valor mais recente encontrado.
        atual.valor_faturado = num(it.vuncom_xml);
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

      // Divergência de valor (com tolerância p/ ponto flutuante / centavos).
      const valorDiverge = Math.abs(valorFaturado - valorPedido) > 0.005;

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

    return {
      pedido_id: pedido.id,
      status: pedido.status ?? '',
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
      itens_nf_sem_pedido: itensNfSemPedido,
    };
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
   * (com_pedido_itens) por itens tipo='vinculado' de vínculos confirmados.
   *
   * - Todos os pro_codigo do pedido contemplados  -> 'Faturado'
   * - Parte contemplada                            -> 'Faturado parcialmente'
   * - Nenhum                                       -> mantém o status atual
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

    const vinculados = await this.repo.findProCodigosVinculadosConfirmados(pedidoId);
    const setVinculados = new Set(vinculados.map((c) => Number(c)));

    const codigosPedido = new Set(proCodigosPedido.map((c) => Number(c)));
    let cobertos = 0;
    for (const c of codigosPedido) {
      if (setVinculados.has(c)) cobertos++;
    }

    let novoStatus = statusAtual;
    if (cobertos === 0) {
      // Nenhum item contemplado: não mexe no status atual.
      novoStatus = statusAtual;
    } else if (cobertos >= codigosPedido.size) {
      // 100% coberto. Se TODAS as notas vinculadas confirmadas já estão lançadas
      // no ERP -> 'Entregue' (data_recebimento = maior dt_entrada). Senão -> 'Faturado'.
      const chaves = await this.repo.findChavesVinculadasConfirmadas(pedidoId);
      const concs = await this.repo.findConciliacaoByChaves(chaves);
      const porChave = new Map(concs.map((c) => [c.chave_nfe, c]));
      const todasLancadas =
        chaves.length > 0 &&
        chaves.every((c) => porChave.get(c)?.status_erp === 'LANCADA');

      if (todasLancadas) {
        const datas = chaves
          .map((c) => porChave.get(c)?.dt_entrada)
          .filter((d): d is Date => d instanceof Date);
        const dataRecebimento = datas.length
          ? new Date(Math.max(...datas.map((d) => d.getTime())))
          : new Date();
        await this.repo.marcarPedidoEntregue(pedidoId, dataRecebimento);
        return 'Entregue';
      }

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
  async confirmarVinculo(vinculoId: string) {
    const v = await this.repo.findVinculoById(vinculoId);
    if (!v) {
      throw new NotFoundException(`Vínculo ${vinculoId} não encontrado.`);
    }
    await this.repo.setVinculoConfirmado(vinculoId, true);
    const status = await this.recalcularStatusPedido(v.pedido_id);
    return { id: vinculoId, confirmado: true, status };
  }

  /**
   * Marca como 'Entregue' (gravando data_recebimento) os pedidos vinculados às
   * NF-e que viraram LANCADA no ERP.
   *
   * Para cada chave: acha os vínculos CONFIRMADOS dessa chave e, para cada
   * pedido_id, atualiza com_pedido (status='Entregue', a menos que já esteja
   * 'Cancelado'; data_recebimento = dt_entrada se vier, senão now()).
   * Idempotente: não reescreve se já estiver 'Entregue' com a mesma data.
   */
  async nfLancada(
    lancadas: Array<{ chave_nfe: string; dt_entrada?: string | null }>,
  ): Promise<{ atualizados: number; pedidos: string[] }> {
    const pedidosAtualizados = new Set<string>();
    let atualizados = 0;

    for (const { chave_nfe, dt_entrada } of lancadas ?? []) {
      const chave = String(chave_nfe ?? '').trim();
      if (!chave) continue;

      const dataRecebimento =
        dt_entrada && !Number.isNaN(Date.parse(dt_entrada))
          ? new Date(dt_entrada)
          : new Date();

      const pedidoIds = await this.repo.findPedidoIdsByChaveConfirmados(chave);
      if (!pedidoIds.length) {
        this.logger.log(`NF lançada ${chave} sem pedido vinculado (confirmado).`);
        continue;
      }

      for (const pedidoId of pedidoIds) {
        const pedido = await this.repo.findPedidoEntrega(pedidoId);
        if (!pedido) continue;

        // Nunca rebaixa/altera pedido Cancelado.
        if (pedido.status === 'Cancelado') continue;

        // Idempotência: já Entregue com a mesma data -> não reescreve.
        const jaEntregue = pedido.status === 'Entregue';
        const mesmaData =
          pedido.data_recebimento != null &&
          pedido.data_recebimento.getTime() === dataRecebimento.getTime();
        if (jaEntregue && mesmaData) continue;

        await this.repo.marcarPedidoEntregue(pedidoId, dataRecebimento);
        atualizados++;
        pedidosAtualizados.add(pedidoId);
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
    const pedidoSemVinculo: any[] = [];

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
      } else {
        pedidoSemVinculo.push({
          pro_codigo: it.pro_codigo,
          pro_descricao: it.pro_descricao,
          quantidade: num(it.quantidade_pedido),
          valor_unitario: num(it.valor_pedido),
        });
      }
    }

    return {
      vinculo_id: v.id,
      pedido_cotacao: v.pedido_cotacao,
      chave_nfe: v.chave_nfe,
      emitente: v.emitente,
      totais: {
        vinculados: vinculados.length,
        xml_sem_vinculo: xmlSemVinculo.length,
        pedido_sem_vinculo: pedidoSemVinculo.length,
      },
      vinculados,
      xml_sem_vinculo: xmlSemVinculo,
      pedido_sem_vinculo: pedidoSemVinculo,
    };
  }

  /** Remove um vínculo salvo (cascade apaga os itens). */
  async removerVinculo(vinculoId: string) {
    const v = await this.repo.findVinculoById(vinculoId);
    if (!v) {
      throw new NotFoundException(`Vínculo ${vinculoId} não encontrado.`);
    }
    const pedidoId = v.pedido_id;
    await this.repo.deleteVinculo(vinculoId);

    // Se não restou nenhum vínculo confirmado, o pedido volta ao estado
    // pré-vinculação: 'Finalizado' (mesmo que estivesse 'Entregue'/'Faturado').
    // Restando outros vínculos, recalcula normalmente (Faturado/parcial/Entregue).
    const restantes = await this.repo.findChavesVinculadasConfirmadas(pedidoId);
    const status = restantes.length
      ? await this.recalcularStatusPedido(pedidoId)
      : await this.repo.reverterPedidoParaFinalizado(pedidoId);

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
        if (cands && cands.length) {
          const alvo = cands[0];
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
    t = t.replace(/[^A-Z0-9 ]/g, ' ').replace(/\s+/g, ' ').trim();
    return t;
  }

  private tokensSemanticos(s: any): Set<string> {
    return new Set(
      this.normalizarTexto(s)
        .split(' ')
        .filter((tk) => tk.length >= 2 && !VinculacaoNfeService.STOP.has(tk)),
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
    const s = String(text).toUpperCase();
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
  private static readonly CORES: Record<string, string> = {
    PRETA: 'PRETO', PRETO: 'PRETO',
    BRANCA: 'BRANCO', BRANCO: 'BRANCO',
    VERMELHA: 'VERMELHO', VERMELHO: 'VERMELHO',
    AZUL: 'AZUL', AZULADO: 'AZUL',
    VERDE: 'VERDE',
    AMARELA: 'AMARELO', AMARELO: 'AMARELO',
    CINZA: 'CINZA', GRAFITE: 'CINZA',
    PRATA: 'PRATA', PRATEADO: 'PRATA',
    DOURADA: 'DOURADO', DOURADO: 'DOURADO',
    FUME: 'FUME', FUMACE: 'FUME',
    CRISTAL: 'CRISTAL',
    ROSA: 'ROSA', LARANJA: 'LARANJA', MARROM: 'MARROM',
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
    threshold = 0.5,
    minIntersec = 3,
  ): { item: ItemCotacao; campo: string; valor: string | null } | null {
    const toksXml = this.tokensSemanticos(item.xProd);
    if (toksXml.size < 2) return null;

    const vXml = item.vUnCom == null ? null : Number(item.vUnCom);
    const anosXml = this.extrairAnos(item.xProd);
    const coresXml = this.extrairCores(item.xProd);

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

      // Cor divergente => produtos diferentes (ex.: BORDA PRETA x BORDA VERMELHA).
      // Se os dois lados têm cor e não compartilham nenhuma, NÃO casa.
      const coresCot = this.extrairCores(haystack);
      if (coresXml.size && coresCot.size) {
        const compartilhaCor = [...coresXml].some((c) => coresCot.has(c));
        if (!compartilhaCor) continue;
      }

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
