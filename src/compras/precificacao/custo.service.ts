import { Injectable, Logger, NotFoundException } from '@nestjs/common';
import * as zlib from 'zlib';
import { PrismaService } from '../../prisma/prisma.service';
import {
  CustoComposicaoDto,
  CustoItemDto,
  OrigemParcela,
  ParcelaCustoDto,
} from './dto/custo-item.dto';

/**
 * Dados por unidade BASE extraídos de uma linha (<det>) do XML da NF-e.
 * "Unidade base" = tributável quando uCom != uTrib (ex.: NF em CJ, estoque em UN),
 * comercial caso contrário — mesma regra já aplicada na conferência por item
 * (`VinculacaoNfeService.conferenciaPorItem`), para que custo e conferência falem
 * a mesma língua.
 */
interface LinhaXml {
  /** qTrib/qCom quando há conversão de unidade; 1 caso contrário. */
  fator: number;
  /** Preço unitário líquido: preço da unidade base − desconto + acréscimo. NÃO inclui IPI. */
  unitarioLiquido: number;
  /** IPI (vIPI) por unidade base. */
  ipiUnit: number;
  /** ICMS ST destacado (vICMSST + vFCPST) por unidade base. */
  stUnit: number;
  /** Valor líquido de mercadoria da linha (unitarioLiquido × quantidade na unidade base). */
  valorLinha: number;
}

/** Agregados da NF inteira — denominadores dos rateios. */
interface DadosNf {
  linhas: Map<string, LinhaXml>;
  /** Σ valorLinha de TODAS as linhas do XML (não só as deste pedido) — denominador do frete. */
  totalMercadoria: number;
  /** Σ valorLinha das linhas com ICMS ST destacado — denominador da guia de ST. */
  baseSt: number;
  /** Guia de ST efetivamente recolhida para esta chave (pagamento; fallback valor_guia). */
  valorGuiaSt: number;
  /** Frete por nossa conta atribuído a esta NF (soma dos CT-es com tomador_nos). */
  freteNossaConta: number;
  /** Chaves de CT-e que originaram o frete — vai no `detalhe` da parcela. */
  ctes: string[];
  /** true quando existe CT-e vinculado, mas nenhum por nossa conta. */
  temCteFornecedor: boolean;
}

/** Contribuição de UMA NF para UM item do pedido. */
interface Contribuicao {
  chave: string;
  quantidade: number;
  unitario: number;
  origemUnitario: OrigemParcela;
  ipi: number;
  st: number;
  stComplementar: number;
  frete: number;
  origemFrete: OrigemParcela;
  detalheFrete?: string;
}

/** Casas decimais das parcelas — alinhado a `Decimal(15, 4)` de `com_pedido_nfe_vinculo_item`. */
const CASAS_PARCELA = 4;

/**
 * Compõe o custo real do item de NF conferida a partir de 5 parcelas discriminadas:
 * unitário, ICMS ST, ICMS ST complementar, IPI e frete (só quando por nossa conta).
 *
 * REUSO: o unitário líquido e o IPI por unidade seguem exatamente a regra já
 * praticada em `conferenciaPorItem` (desconto/acréscimo sempre; conversão de unidade
 * por uTrib). NOVO: o rateio por item de frete e de guia de ST — o serviço só tinha
 * o total por pedido (`sincronizarTransportePedido`).
 *
 * ATENÇÃO — `ipi_no_valor` NÃO altera o custo composto: o IPI pago é custo sempre e
 * aqui é parcela própria. O flag descreve o PEDIDO (se o valor negociado já embutia
 * IPI) e é devolvido apenas como metadado de conferência.
 */
@Injectable()
export class CustoService {
  private readonly logger = new Logger(CustoService.name);

  constructor(private readonly prisma: PrismaService) {}

  /** Custo composto de todos os itens de um pedido — 1 requisição, N itens. */
  async custoDoPedido(pedidoId: string): Promise<CustoComposicaoDto> {
    return this.compor(String(pedidoId ?? '').trim(), null);
  }

  /**
   * Custo composto dos itens cobertos por UMA nota. Como a mesma NF pode ser
   * repartida entre pedidos, devolve uma composição por pedido vinculado.
   */
  async custoDaNota(chaveNfe: string): Promise<CustoComposicaoDto[]> {
    const chave = this.normChave(chaveNfe);
    if (!chave) {
      throw new NotFoundException('Chave de NF-e inválida (esperados 44 dígitos).');
    }
    const vinculos = await this.prisma.com_pedido_nfe_vinculo.findMany({
      where: { chave_nfe: chave, confirmado: true, rejeitado: false },
      select: { pedido_id: true },
    });
    const pedidos = [...new Set(vinculos.map((v) => v.pedido_id))];
    if (!pedidos.length) {
      throw new NotFoundException(
        `Nenhum pedido com vínculo confirmado para a NF-e ${chave}.`,
      );
    }
    const out: CustoComposicaoDto[] = [];
    for (const p of pedidos) out.push(await this.compor(p, chave));
    return out;
  }

  // --------------------------------------------------------------------------
  // Composição
  // --------------------------------------------------------------------------

  private async compor(
    pedidoId: string,
    chaveFiltro: string | null,
  ): Promise<CustoComposicaoDto> {
    const pedido = await this.prisma.com_pedido.findUnique({
      where: { id: pedidoId },
      select: { id: true, for_codigo: true, ipi_no_valor: true },
    });
    if (!pedido) {
      throw new NotFoundException(`Pedido ${pedidoId} não encontrado.`);
    }

    const itensPedido = await this.prisma.com_pedido_itens.findMany({
      where: { pedido_id: pedidoId },
      select: {
        pro_codigo: true,
        pro_descricao: true,
        unidade: true,
        quantidade: true,
        valor_unitario: true,
      },
      orderBy: { pro_codigo: 'asc' },
    });

    const vinculos = await this.prisma.com_pedido_nfe_vinculo.findMany({
      where: {
        pedido_id: pedidoId,
        confirmado: true,
        rejeitado: false,
        ...(chaveFiltro ? { chave_nfe: chaveFiltro } : {}),
      },
      select: {
        chave_nfe: true,
        itens: {
          where: { tipo: 'vinculado' },
          select: {
            pro_codigo: true,
            cprod_xml: true,
            quantidade_xml: true,
            quantidade_alocada: true,
            vuncom_xml: true,
          },
        },
      },
    });

    const chaves = [...new Set(vinculos.map((v) => v.chave_nfe).filter(Boolean))];
    const dadosPorChave = await this.carregarDadosNf(chaves);
    const stPorChaveProduto = await this.carregarFlagSt(chaves);

    // Contribuições por pro_codigo (um item pode vir de mais de uma NF).
    const contribs = new Map<number, Contribuicao[]>();
    for (const v of vinculos) {
      const dados = dadosPorChave.get(v.chave_nfe);
      for (const it of v.itens) {
        if (it.pro_codigo == null) continue;
        const cod = Number(it.pro_codigo);
        const linha = dados?.linhas.get(this.normRef(it.cprod_xml));
        const fator = linha?.fator ?? 1;
        const quantidade = this.num(it.quantidade_alocada ?? it.quantidade_xml) * fator;

        // Unitário: preferência para o XML (líquido, na unidade base); fallback para
        // o snapshot vuncom_xml gravado no vínculo quando o XML não está disponível.
        let unitario = linha?.unitarioLiquido ?? this.num(it.vuncom_xml);
        let origemUnitario: OrigemParcela = linha ? 'xml' : 'pedido';
        if (!Number.isFinite(unitario) || unitario < 0) {
          unitario = 0;
          origemUnitario = 'ausente';
        }

        const ipi = linha?.ipiUnit ?? 0;
        const st = linha?.stUnit ?? 0;

        // ST complementar: guia recolhida pela empresa, rateada entre os itens COM ST
        // (proporcional ao valor líquido do item). Sem marcação de ST na conciliação,
        // a base é a nota inteira.
        let stComplementar = 0;
        if (dados && dados.valorGuiaSt > 0) {
          const temSt =
            st > 0 || stPorChaveProduto.get(`${v.chave_nfe}|${cod}`) === true;
          const base = dados.baseSt > 0 ? dados.baseSt : dados.totalMercadoria;
          const aplica = dados.baseSt > 0 ? temSt : true;
          if (aplica && base > 0) {
            stComplementar = (dados.valorGuiaSt * unitario) / base;
          }
        }

        // Frete: só o que é por nossa conta, rateado pelo valor líquido do item
        // sobre o valor de mercadoria da NF inteira.
        let frete = 0;
        let origemFrete: OrigemParcela = 'ausente';
        let detalheFrete: string | undefined;
        if (dados) {
          if (dados.freteNossaConta > 0 && dados.totalMercadoria > 0) {
            frete = (dados.freteNossaConta * unitario) / dados.totalMercadoria;
            origemFrete = 'cte';
            detalheFrete = `CT-e ${dados.ctes.join(', ')} (tomador_nos) rateado por valor do item`;
          } else if (dados.temCteFornecedor) {
            origemFrete = 'cte';
            detalheFrete = 'CT-e com tomador_nos = false — frete do fornecedor, não entra no custo';
          } else {
            detalheFrete = 'sem CT-e vinculado à NF';
          }
        }

        const lista = contribs.get(cod) ?? [];
        lista.push({
          chave: v.chave_nfe,
          quantidade,
          unitario,
          origemUnitario,
          ipi,
          st,
          stComplementar,
          frete,
          origemFrete,
          detalheFrete,
        });
        contribs.set(cod, lista);
      }
    }

    const itens: CustoItemDto[] = itensPedido.map((p) =>
      this.comporItem(p, contribs.get(Number(p.pro_codigo)) ?? []),
    );

    return {
      pedido_id: pedido.id,
      for_codigo: pedido.for_codigo == null ? null : Number(pedido.for_codigo),
      ipi_no_valor: !!pedido.ipi_no_valor,
      chave_nfe: chaveFiltro,
      totais: this.totalizar(itens),
      itens,
    };
  }

  /**
   * Consolida as contribuições de N notas num único custo unitário: cada parcela é a
   * média ponderada pela quantidade faturada. Sem nenhuma NF, cai no pedido
   * (`valor_unitario`) — a base de custo decidida para `com_pedido_itens`.
   */
  private comporItem(
    p: {
      pro_codigo: number;
      pro_descricao: string;
      unidade: string | null;
      quantidade: unknown;
      valor_unitario: unknown;
    },
    lista: Contribuicao[],
  ): CustoItemDto {
    const qtdTotal = lista.reduce((acc, c) => acc + c.quantidade, 0);

    let unitario: number;
    let origemUnitario: OrigemParcela;
    let ipi = 0;
    let st = 0;
    let stComplementar = 0;
    let frete = 0;
    let origemFrete: OrigemParcela = 'ausente';
    let detalheFrete: string | undefined;
    let quantidade = qtdTotal;

    if (!lista.length || qtdTotal <= 0) {
      // Item ainda não faturado: custo = valor negociado no pedido, sem parcelas.
      unitario = this.num(p.valor_unitario);
      origemUnitario = 'pedido';
      quantidade = this.num(p.quantidade);
      detalheFrete = 'item sem NF vinculada';
    } else {
      const pond = (sel: (c: Contribuicao) => number) =>
        lista.reduce((acc, c) => acc + sel(c) * c.quantidade, 0) / qtdTotal;
      unitario = pond((c) => c.unitario);
      ipi = pond((c) => c.ipi);
      st = pond((c) => c.st);
      stComplementar = pond((c) => c.stComplementar);
      frete = pond((c) => c.frete);
      origemUnitario = lista.every((c) => c.origemUnitario === 'xml') ? 'xml' : 'pedido';
      const comFrete = lista.find((c) => c.frete > 0) ?? lista[0];
      origemFrete = comFrete.origemFrete;
      detalheFrete = comFrete.detalheFrete;
    }

    const parcelas = {
      unitario: this.parcela(unitario, origemUnitario, 'preço líquido da NF (desconto/acréscimo aplicados, sem IPI)'),
      icms_st: this.parcela(st, st > 0 ? 'xml' : 'ausente', 'vICMSST + vFCPST destacados na linha'),
      icms_st_complementar: this.parcela(
        stComplementar,
        stComplementar > 0 ? 'guia' : 'ausente',
        'guia de ST da chave rateada entre os itens com ST',
      ),
      ipi: this.parcela(ipi, ipi > 0 ? 'xml' : 'ausente', 'vIPI da linha por unidade'),
      frete: this.parcela(frete, origemFrete, detalheFrete),
    };

    // O custo é a SOMA das parcelas já arredondadas — garante diferença zero entre
    // total e parcelas (o AC pede ≤ R$ 0,01).
    const custoUnitario = this.round(
      parcelas.unitario.valor +
        parcelas.icms_st.valor +
        parcelas.icms_st_complementar.valor +
        parcelas.ipi.valor +
        parcelas.frete.valor,
      CASAS_PARCELA,
    );

    return {
      pro_codigo: Number(p.pro_codigo),
      pro_descricao: p.pro_descricao ?? '',
      unidade: p.unidade ?? null,
      quantidade: this.round(quantidade, CASAS_PARCELA),
      chaves_nfe: [...new Set(lista.map((c) => c.chave))],
      custo_unitario: custoUnitario,
      custo_total: this.round(custoUnitario * quantidade, 2),
      parcelas,
    };
  }

  private totalizar(itens: CustoItemDto[]) {
    const soma = (sel: (i: CustoItemDto) => number) =>
      this.round(
        itens.reduce((acc, i) => acc + sel(i) * i.quantidade, 0),
        2,
      );
    return {
      itens: itens.length,
      custo_total: soma((i) => i.custo_unitario),
      unitario: soma((i) => i.parcelas.unitario.valor),
      icms_st: soma((i) => i.parcelas.icms_st.valor),
      icms_st_complementar: soma((i) => i.parcelas.icms_st_complementar.valor),
      ipi: soma((i) => i.parcelas.ipi.valor),
      frete: soma((i) => i.parcelas.frete.valor),
    };
  }

  private parcela(valor: number, origem: OrigemParcela, detalhe?: string): ParcelaCustoDto {
    const v = this.round(Number.isFinite(valor) ? valor : 0, CASAS_PARCELA);
    return { valor: v, origem, detalhe };
  }

  // --------------------------------------------------------------------------
  // Carga de dados por chave de NF
  // --------------------------------------------------------------------------

  private async carregarDadosNf(chaves: string[]): Promise<Map<string, DadosNf>> {
    const out = new Map<string, DadosNf>();
    if (!chaves.length) return out;

    const [concs, guias, ctes] = await Promise.all([
      this.prisma.com_nfe_conciliacao.findMany({
        where: { chave_nfe: { in: chaves } },
        select: { chave_nfe: true, xml_completo: true, valor_guia: true, valor_total: true },
      }),
      this.prisma.com_pagamento_guia.findMany({
        where: { chave_nfe: { in: chaves } },
        select: { chave_nfe: true, valor: true },
      }),
      this.buscarFretePorChave(chaves),
    ]);

    const guiaPaga = new Map(guias.map((g) => [g.chave_nfe, this.num(g.valor)]));

    for (const chave of chaves) {
      const conc = concs.find((c) => c.chave_nfe === chave);
      const linhas = conc?.xml_completo
        ? this.parseLinhasXml(conc.xml_completo)
        : new Map<string, LinhaXml>();

      let totalMercadoria = 0;
      let baseSt = 0;
      for (const l of linhas.values()) {
        totalMercadoria += l.valorLinha;
        if (l.stUnit > 0) baseSt += l.valorLinha;
      }
      if (totalMercadoria <= 0) totalMercadoria = this.num(conc?.valor_total);

      const frete = ctes.get(chave);
      out.set(chave, {
        linhas,
        totalMercadoria,
        baseSt,
        // A guia efetivamente paga manda; o valor apurado na conciliação é fallback.
        valorGuiaSt: guiaPaga.get(chave) ?? this.num(conc?.valor_guia),
        freteNossaConta: frete?.valor ?? 0,
        ctes: frete?.ctes ?? [],
        temCteFornecedor: !!frete?.temCteFornecedor,
      });
    }
    return out;
  }

  /** `com_nfe_conciliacao_item.possui_icms_st` por chave + pro_codigo. */
  private async carregarFlagSt(chaves: string[]): Promise<Map<string, boolean>> {
    const out = new Map<string, boolean>();
    if (!chaves.length) return out;
    const rows = await this.prisma.com_nfe_conciliacao_item.findMany({
      where: { chave_nfe: { in: chaves } },
      select: { chave_nfe: true, pro_codigo: true, possui_icms_st: true },
    });
    for (const r of rows) {
      if (r.pro_codigo == null) continue;
      const cod = Number(r.pro_codigo);
      if (!Number.isFinite(cod)) continue;
      const k = `${r.chave_nfe}|${cod}`;
      out.set(k, (out.get(k) ?? false) || !!r.possui_icms_st);
    }
    return out;
  }

  /**
   * Frete POR NOSSA CONTA atribuído a cada NF. Um CT-e pode cobrir várias NFs
   * (`dados_json -> documentosNFe`): o valor é rateado entre elas na proporção do
   * valor total de cada nota (divisão igual quando os valores não são conhecidos).
   * CT-e com `tomador_nos = false` é frete do fornecedor: não entra no custo.
   */
  private async buscarFretePorChave(
    chaves: string[],
  ): Promise<Map<string, { valor: number; ctes: string[]; temCteFornecedor: boolean }>> {
    const out = new Map<string, { valor: number; ctes: string[]; temCteFornecedor: boolean }>();
    const limpas = [...new Set(chaves.map((c) => this.normChave(c)).filter(Boolean))];
    if (!limpas.length) return out;

    // Lista literal segura: só chaves de 44 dígitos (mesma sanitização do repositório
    // de vinculação, que também usa $queryRawUnsafe para o JSONB do CT-e).
    const inList = limpas.map((c) => `'${c}'`).join(',');
    const rows = await this.prisma.$queryRawUnsafe<
      Array<{
        cte_chave: string;
        tomador_nos: boolean | null;
        valor_frete: unknown;
        chaves_nfe: string[];
      }>
    >(
      `SELECT c.chave_acesso AS cte_chave,
              c.tomador_nos  AS tomador_nos,
              c.valor_total  AS valor_frete,
              ARRAY(SELECT jsonb_array_elements_text(c.dados_json -> 'documentosNFe')) AS chaves_nfe
         FROM com_cte_documento c
        WHERE EXISTS (
                SELECT 1
                  FROM jsonb_array_elements_text(c.dados_json -> 'documentosNFe') AS j(chave)
                 WHERE j.chave IN (${inList})
              )`,
    );
    if (!rows.length) return out;

    // Valor de cada NF coberta pelos CT-es (inclui notas fora do pedido) — peso do rateio.
    const todasChaves = [...new Set(rows.flatMap((r) => r.chaves_nfe ?? []))];
    const valores = todasChaves.length
      ? await this.prisma.com_nfe_conciliacao.findMany({
          where: { chave_nfe: { in: todasChaves } },
          select: { chave_nfe: true, valor_total: true },
        })
      : [];
    const valorNf = new Map(valores.map((v) => [v.chave_nfe, this.num(v.valor_total)]));

    const alvo = new Set(limpas);
    for (const r of rows) {
      const cobertas = [...new Set((r.chaves_nfe ?? []).filter(Boolean))];
      if (!cobertas.length) continue;
      const nossaConta = r.tomador_nos === true;
      const valorCte = this.num(r.valor_frete);
      const somaValores = cobertas.reduce((acc, c) => acc + (valorNf.get(c) ?? 0), 0);

      for (const chave of cobertas) {
        if (!alvo.has(chave)) continue;
        const atual = out.get(chave) ?? { valor: 0, ctes: [], temCteFornecedor: false };
        if (nossaConta && valorCte > 0) {
          const peso =
            somaValores > 0 ? (valorNf.get(chave) ?? 0) / somaValores : 1 / cobertas.length;
          atual.valor += valorCte * peso;
          if (!atual.ctes.includes(r.cte_chave)) atual.ctes.push(r.cte_chave);
        } else if (!nossaConta) {
          atual.temCteFornecedor = true;
        }
        out.set(chave, atual);
      }
    }
    return out;
  }

  // --------------------------------------------------------------------------
  // XML
  // --------------------------------------------------------------------------

  /**
   * Mapa cProd (normalizado) -> valores POR UNIDADE BASE da linha do XML.
   * Espelha a normalização de `parseAjustesUnitPorCprod` (conversão uCom/uTrib,
   * desconto e acréscimo) e acrescenta o ICMS ST destacado, que a conferência não usa.
   */
  private parseLinhasXml(raw: string | Buffer): Map<string, LinhaXml> {
    const xml = this.sanitizeXml(raw);
    const detRe = /<(?:\w+:)?det\b[^>]*>([\s\S]*?)<\/(?:\w+:)?det>/gi;
    const normUnit = (s: string) => String(s || '').trim().toUpperCase();

    interface Acc {
      vipi: number;
      vdesc: number;
      voutro: number;
      vst: number;
      qcom: number;
      qtrib: number;
      uCom: string;
      uTrib: string;
      vUnCom: number;
      vUnTrib: number;
    }
    const acc = new Map<string, Acc>();

    let m: RegExpExecArray | null;
    while ((m = detRe.exec(xml)) !== null) {
      const bloco = m[1];
      const cprod = this.normRef(this.tagText(bloco, 'cProd'));
      if (!cprod) continue;
      const a =
        acc.get(cprod) ??
        {
          vipi: 0, vdesc: 0, voutro: 0, vst: 0,
          qcom: 0, qtrib: 0, uCom: '', uTrib: '', vUnCom: 0, vUnTrib: 0,
        };
      a.vipi += this.toNumber(this.tagText(bloco, 'vIPI')) ?? 0;
      a.vdesc += this.toNumber(this.tagText(bloco, 'vDesc')) ?? 0;
      a.voutro += this.toNumber(this.tagText(bloco, 'vOutro')) ?? 0;
      a.vst +=
        (this.toNumber(this.tagText(bloco, 'vICMSST')) ?? 0) +
        (this.toNumber(this.tagText(bloco, 'vFCPST')) ?? 0);
      a.qcom += this.toNumber(this.tagText(bloco, 'qCom')) ?? 0;
      a.qtrib += this.toNumber(this.tagText(bloco, 'qTrib')) ?? 0;
      const uCom = normUnit(this.tagText(bloco, 'uCom'));
      const uTrib = normUnit(this.tagText(bloco, 'uTrib'));
      const vUnCom = this.toNumber(this.tagText(bloco, 'vUnCom')) ?? 0;
      const vUnTrib = this.toNumber(this.tagText(bloco, 'vUnTrib')) ?? 0;
      if (uCom) a.uCom = uCom;
      if (uTrib) a.uTrib = uTrib;
      if (vUnCom > 0) a.vUnCom = vUnCom;
      if (vUnTrib > 0) a.vUnTrib = vUnTrib;
      acc.set(cprod, a);
    }

    const out = new Map<string, LinhaXml>();
    for (const [cprod, v] of acc) {
      const converter =
        !!v.uCom && !!v.uTrib && v.uCom !== v.uTrib && v.qcom > 0 && v.qtrib > 0;
      const fator = converter ? v.qtrib / v.qcom : 1;
      const qtdBase = converter ? v.qtrib : v.qcom;
      const denom = qtdBase > 0 ? qtdBase : 1;
      const precoUnit = converter ? v.vUnTrib : v.vUnCom;
      const unitarioLiquido = Math.max(0, precoUnit - v.vdesc / denom + v.voutro / denom);
      out.set(cprod, {
        fator,
        unitarioLiquido,
        ipiUnit: v.vipi / denom,
        stUnit: v.vst / denom,
        valorLinha: unitarioLiquido * qtdBase,
      });
    }
    return out;
  }

  // --------------------------------------------------------------------------
  // Utilitários
  // --------------------------------------------------------------------------

  /** Normaliza XML vindo do banco: base64+gzip, gzip/zlib cru, BOM ou escapado. */
  private sanitizeXml(raw: string | Buffer): string {
    let data: Buffer | string = raw;
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
    if (Buffer.isBuffer(data)) {
      const b = data;
      const isGzip = b[0] === 0x1f && b[1] === 0x8b;
      const isZlib = b[0] === 0x78 && [0x9c, 0xda, 0x01].includes(b[1]);
      if (isGzip || isZlib) {
        try {
          data = isGzip ? zlib.gunzipSync(b) : zlib.inflateSync(b);
        } catch {
          /* mantém buffer original */
        }
      }
      data = Buffer.isBuffer(data) ? data.toString('utf-8') : data;
    }
    let s = String(data);
    s = s.replace(/^﻿/, '').replace(/\x00/g, '');
    if (!s.includes('<') && s.includes('&lt;')) s = this.desescapar(s);
    const idx = s.indexOf('<');
    if (idx > 0) s = s.slice(idx);
    return s.trim();
  }

  private tagText(xml: string, tag: string): string {
    const re = new RegExp(
      `<(?:\\w+:)?${tag}\\b[^>]*>([\\s\\S]*?)</(?:\\w+:)?${tag}>`,
      'i',
    );
    const m = re.exec(xml);
    return m ? this.desescapar(m[1]).trim() : '';
  }

  private desescapar(s: string): string {
    return s
      .replace(/&lt;/g, '<')
      .replace(/&gt;/g, '>')
      .replace(/&quot;/g, '"')
      .replace(/&apos;/g, "'")
      .replace(/&amp;/g, '&');
  }

  private toNumber(v: unknown): number | null {
    if (v == null) return null;
    const s = String(v).trim();
    if (!s) return null;
    const n = Number(s.replace(',', '.'));
    return Number.isFinite(n) ? n : null;
  }

  /** Mesma normalização de referência da vinculação (zeros à esquerda, caixa alta). */
  private normRef(s: unknown): string {
    if (s == null) return '';
    return String(s).replace(/\s+/g, '').toUpperCase().replace(/^0+(?=.)/, '');
  }

  private normChave(s: unknown): string {
    const c = String(s ?? '').replace(/\D/g, '');
    return c.length === 44 ? c : '';
  }

  private num(d: unknown): number {
    if (d == null) return 0;
    const n = Number(d);
    return Number.isFinite(n) ? n : 0;
  }

  private round(n: number, casas: number): number {
    if (!Number.isFinite(n)) return 0;
    const f = Math.pow(10, casas);
    return Math.round((n + Number.EPSILON) * f) / f;
  }
}
