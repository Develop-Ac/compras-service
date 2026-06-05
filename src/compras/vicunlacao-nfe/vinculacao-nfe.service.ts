import { Injectable, Logger, NotFoundException } from '@nestjs/common';
import * as zlib from 'zlib';
import {
  CotacaoItemRow,
  VinculacaoNfeRepository,
} from './vinculacao-nfe.repository';

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
    // 1) XML da NF-e
    const nfeRow = await this.repo.findXmlByChave(nfe);
    if (!nfeRow) {
      throw new NotFoundException(`Nenhuma NF-e encontrada para a chave ${nfe}.`);
    }
    if (nfeRow.XML_COMPLETO == null) {
      throw new NotFoundException('XML_COMPLETO veio nulo para a chave informada.');
    }
    const itensXml = this.parseItensNfe(nfeRow.XML_COMPLETO);

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

    return {
      pedido_cotacao: pedido,
      chave_nfe: nfe,
      emitente: nfeRow.NOME_EMITENTE,
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
    return String(s).replace(/\s+/g, '').toUpperCase();
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
