import { Injectable, Logger, NotFoundException } from '@nestjs/common';
import { Prisma } from '@prisma/client';
import * as zlib from 'zlib';
import {
  CotacaoItemRow,
  VinculacaoNfeRepository,
} from './vinculacao-nfe.repository';
import { SalvarVinculoDto } from './dto/salvar-vinculo.dto';

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

  constructor(private readonly repo: VinculacaoNfeRepository) {}

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

    // 4) Vinculação XML <-> cotação (resolve o pro_codigo) + fallback semântico
    const usados = new Set<number>();
    const vinculados: ItemVinculado[] = [];
    const xmlSemVinculo: ItemXml[] = [];
    const proCodigosVinculados = new Set<string>();

    const indices = this.indexarCotacao(itensCotacao);

    for (const item of itensXml) {
      const match = this.encontrarMatch(item, indices) ?? this.matchSemantico(item, itensCotacao, usados);
      if (match) {
        usados.add(match.item._idx);
        const codigo = match.item.pro_codigo;
        const ped = codigo == null ? undefined : pedidoPorCodigo.get(String(codigo));
        if (codigo != null) proCodigosVinculados.add(String(codigo));
        vinculados.push({
          produto_xml: item.xProd,
          cprod_xml: item.cProd ?? null,
          quantidade_xml: item.qCom,
          vuncom_xml: item.vUnCom,
          pro_codigo: codigo,
          pro_descricao: match.item.pro_descricao,
          quantidade_cotacao: match.item.quantidade,
          quantidade_pedido: ped?.quantidade ?? null,
          valor_pedido: ped?.valor_unitario ?? null,
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

    // Agrega o faturado (itens tipo='vinculado') por pro_codigo.
    interface AggFaturado {
      quantidade_faturada: number;
      valor_faturado: number; // última vuncom_xml
      chaves_nfe: Set<string>;
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
        const atual =
          faturadoPorCodigo.get(cod) ??
          { quantidade_faturada: 0, valor_faturado: 0, chaves_nfe: new Set<string>() };
        atual.quantidade_faturada += num(it.quantidade_xml);
        // "última" vuncom_xml: sobrescreve com o valor mais recente encontrado.
        atual.valor_faturado = num(it.vuncom_xml);
        if (chave) atual.chaves_nfe.add(chave);
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

    let itensCompletos = 0;
    let itensParciais = 0;
    let itensNaoFaturados = 0;
    let itensDivergentes = 0;
    let valorPedidoTotal = 0;
    let valorFaturadoTotal = 0;

    const itens = itensPedido.map((p) => {
      const cod = Number(p.pro_codigo);
      const quantidadePedido = num(p.quantidade);
      const valorPedido = num(p.valor_unitario);
      const agg = faturadoPorCodigo.get(cod);
      const quantidadeFaturada = agg?.quantidade_faturada ?? 0;
      const valorFaturado = agg?.valor_faturado ?? 0;
      const chavesNfe = agg ? [...agg.chaves_nfe] : [];

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
        saldo: quantidadePedido - quantidadeFaturada,
        valor_pedido: valorPedido,
        valor_faturado: valorFaturado,
        diferenca_valor: valorFaturado - valorPedido,
        situacao,
        chaves_nfe: chavesNfe,
      };
    });

    // "Valor Faturado" do resumo (cards superiores) = valor TOTAL da(s) NF(s)
    // vinculada(s) (vNF real, de com_nfe_conciliacao), e não a soma dos itens
    // casados. A coluna por item continua sendo o valor unitário da NF.
    const concChaves = await this.repo.findConciliacaoByChaves([...chavesFaturadas]);
    valorFaturadoTotal = concChaves.reduce((acc, c) => acc + num(c.valor_total), 0);

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
      t = t.split(k).join(VinculacaoNfeService.ABREV[k]);
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

  /** Fallback: maior sobreposição de tokens entre xProd e descrição da cotação. */
  private matchSemantico(
    item: ItemXml,
    itens: ItemCotacao[],
    usados: Set<number>,
    threshold = 0.5,
    minIntersec = 3,
  ): { item: ItemCotacao; campo: string; valor: string | null } | null {
    const toksXml = this.tokensSemanticos(item.xProd);
    if (toksXml.size < minIntersec) return null;

    let best: ItemCotacao | null = null;
    let bestInter: string[] = [];
    let bestScore = 0;

    for (const it of itens) {
      if (usados.has(it._idx)) continue;
      const haystack = [it.pro_descricao, it.referencia, it.ref_fabricante, it.ref_fornecedor]
        .filter(Boolean)
        .join(' ');
      const toksCot = this.tokensSemanticos(haystack);
      if (!toksCot.size) continue;

      const inter = [...toksXml].filter((tk) => toksCot.has(tk));
      if (inter.length < minIntersec) continue;
      const cont = inter.length / Math.min(toksXml.size, toksCot.size);
      if (cont > bestScore) {
        bestScore = cont;
        best = it;
        bestInter = inter;
      }
    }

    if (best && bestScore >= threshold) {
      return {
        item: best,
        campo: 'Analise semantica',
        valor: `${Math.round(bestScore * 100)}% | ${bestInter.sort().join(' ')}`,
      };
    }
    return null;
  }
}
