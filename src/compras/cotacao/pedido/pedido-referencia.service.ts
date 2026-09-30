import { Injectable, Logger } from '@nestjs/common';
import { PedidoRepository } from './pedido.repository';
import { ErpApiService } from '../../../shared/erp-api/erp-api.service';

/* =============================================================================
   TROCA DE ITEM PELA REFERÊNCIA DO FORNECEDOR
   -----------------------------------------------------------------------------
   Quando o fornecedor responde a cotação, a referência que ele digitou para
   cada produto fica em com_produto_fornecedor_referencia (fornecedor + codigo
   nosso -> referência dele). Na hora de virar pedido, essa referência é
   consultada no cadastro do ERP (PRODUTOS.REF_FORNECEDOR, rota
   /erp/produtos/referencia) e, se ela apontar para OUTRO produto, o item do
   pedido é trocado por esse produto — mantendo quantidade, preço e o restante.

   Regras:
   - sem referência gravada, ou referência vazia -> item fica como está;
   - API fora, erro ou lista vazia -> item fica como está;
   - a lista já contém o próprio produto -> item fica como está (já é o certo);
   - mais de um candidato ativo e sem como desempatar -> item fica como está;
   - o produto destino já é outro item do mesmo fornecedor no pedido -> fica
     como está (o pedido tem unique por pedido/produto/fornecedor).
   ============================================================================= */

/** Campos mínimos que a troca lê/escreve; o resto do item passa intacto. */
export interface ItemReferenciavel {
  pro_codigo: number;
  pro_descricao: string;
  mar_descricao?: string | null;
  referencia?: string | null;
  unidade?: string | null;
  for_codigo: number;
}

export interface Substituicao {
  for_codigo: number;
  referencia: string;
  de: { pro_codigo: number; pro_descricao: string };
  para: { pro_codigo: number; pro_descricao: string };
}

export interface AvisoReferencia {
  for_codigo: number;
  pro_codigo: number;
  referencia: string;
  motivo: string;
}

export interface ResultadoSubstituicao<T> {
  itens: T[];
  substituicoes: Substituicao[];
  avisos: AvisoReferencia[];
}

type ProdutoErp = {
  PRO_CODIGO: number;
  PRO_DESCRICAO: string;
  REF_FORNECEDOR?: string | null;
  REFERENCIA?: string | null;
  UNIDADE?: string | null;
  MAR_DESCRICAO?: string | null;
  INATIVO?: string | null;
};

/** Chamadas simultâneas à API: produto a produto abre uma conexão por item no Firebird. */
const CONCORRENCIA = 4;

@Injectable()
export class PedidoReferenciaService {
  private readonly logger = new Logger(PedidoReferenciaService.name);

  constructor(
    private readonly repo: PedidoRepository,
    private readonly erpApi: ErpApiService,
  ) {}

  async substituirPorReferencia<T extends ItemReferenciavel>(
    itens: T[],
    empresa: number,
  ): Promise<ResultadoSubstituicao<T>> {
    const substituicoes: Substituicao[] = [];
    const avisos: AvisoReferencia[] = [];

    if (!itens.length) return { itens, substituicoes, avisos };

    if (!this.erpApi.habilitado) {
      this.logger.warn(
        '[REFERENCIA] ERP_API_URL não configurada (ou em cooldown) — itens mantidos sem troca por referência.',
      );
      return { itens, substituicoes, avisos };
    }

    // 1) referência do fornecedor para cada (produto, fornecedor) — mais recente.
    const pares = itens
      .map((i) => ({ pro_codigo: Number(i.pro_codigo), for_codigo: Number(i.for_codigo) }))
      .filter((p) => Number.isFinite(p.pro_codigo) && Number.isFinite(p.for_codigo));
    const referencias = await this.repo.findReferenciasEmLote(pares);
    if (!referencias.size) return { itens, substituicoes, avisos };

    // 2) uma consulta por referência distinta, com concorrência limitada.
    const distintas = [...new Set(
      itens
        .map((i) => this.limpar(referencias.get(`${i.pro_codigo}_${i.for_codigo}`)))
        .filter((r): r is string => !!r),
    )];
    const produtosPorRef = await this.consultarReferencias(distintas, empresa);

    // 3) decide item a item; códigos já usados por fornecedor evitam colisão de unique.
    const usadosPorFor = new Map<number, Set<number>>();
    for (const i of itens) {
      const f = Number(i.for_codigo);
      (usadosPorFor.get(f) ?? usadosPorFor.set(f, new Set()).get(f)!).add(Number(i.pro_codigo));
    }

    const saida = itens.map((item) => {
      const for_codigo = Number(item.for_codigo);
      const pro_codigo = Number(item.pro_codigo);
      const referencia = this.limpar(referencias.get(`${pro_codigo}_${for_codigo}`));
      if (!referencia) return item;

      const produtos = produtosPorRef.get(referencia);
      if (produtos === undefined) {
        avisos.push({ for_codigo, pro_codigo, referencia, motivo: 'API do ERP indisponível; item mantido' });
        return item;
      }
      if (!produtos.length) {
        avisos.push({ for_codigo, pro_codigo, referencia, motivo: 'Referência não encontrada no ERP; item mantido' });
        return item;
      }

      // O próprio produto já responde por essa referência: nada a trocar.
      if (produtos.some((p) => Number(p.PRO_CODIGO) === pro_codigo)) return item;

      const escolhido = this.escolher(produtos, item);
      if (!escolhido) {
        avisos.push({
          for_codigo,
          pro_codigo,
          referencia,
          motivo: `Referência aponta para mais de um produto (${produtos.map((p) => p.PRO_CODIGO).join(', ')}); item mantido`,
        });
        return item;
      }

      const novoCodigo = Number(escolhido.PRO_CODIGO);
      const usados = usadosPorFor.get(for_codigo)!;
      if (usados.has(novoCodigo)) {
        avisos.push({
          for_codigo,
          pro_codigo,
          referencia,
          motivo: `Produto ${novoCodigo} já está no pedido deste fornecedor; item mantido`,
        });
        return item;
      }
      usados.add(novoCodigo);

      substituicoes.push({
        for_codigo,
        referencia,
        de: { pro_codigo, pro_descricao: item.pro_descricao },
        para: { pro_codigo: novoCodigo, pro_descricao: String(escolhido.PRO_DESCRICAO ?? '').trim() },
      });

      return {
        ...item,
        pro_codigo: novoCodigo,
        pro_descricao: String(escolhido.PRO_DESCRICAO ?? item.pro_descricao).trim(),
        mar_descricao: this.limpar(escolhido.MAR_DESCRICAO) ?? item.mar_descricao ?? null,
        referencia: this.limpar(escolhido.REFERENCIA) ?? this.limpar(escolhido.REF_FORNECEDOR) ?? referencia,
        unidade: this.limpar(escolhido.UNIDADE) ?? item.unidade ?? null,
      };
    });

    if (substituicoes.length) {
      this.logger.log(
        `[REFERENCIA] ${substituicoes.length} item(ns) trocado(s) pela referência do fornecedor: ` +
          substituicoes.map((s) => `${s.de.pro_codigo}->${s.para.pro_codigo} (${s.referencia}, forn ${s.for_codigo})`).join('; '),
      );
    }

    return { itens: saida, substituicoes, avisos };
  }

  /* ------------------------------ auxiliares ------------------------------ */

  private limpar(v: unknown): string | undefined {
    const s = v == null ? '' : String(v).trim();
    return s ? s : undefined;
  }

  /**
   * Entre os produtos que respondem pela referência: só os ativos; se sobrar
   * mais de um, tenta a marca do item; sem desempate, não troca.
   */
  private escolher(produtos: ProdutoErp[], item: ItemReferenciavel): ProdutoErp | null {
    const ativos = produtos.filter((p) => String(p.INATIVO ?? '').trim().toUpperCase() !== 'S');
    if (ativos.length === 1) return ativos[0];
    if (!ativos.length) return null;

    const marca = this.limpar(item.mar_descricao)?.toUpperCase();
    if (marca) {
      const mesmaMarca = ativos.filter((p) => this.limpar(p.MAR_DESCRICAO)?.toUpperCase() === marca);
      if (mesmaMarca.length === 1) return mesmaMarca[0];
    }
    return null;
  }

  /**
   * Consulta cada referência na API. Referência que falhou fica FORA do mapa
   * (undefined = indisponível); referência sem produto fica com lista vazia.
   */
  private async consultarReferencias(
    referencias: string[],
    empresa: number,
  ): Promise<Map<string, ProdutoErp[]>> {
    const resultado = new Map<string, ProdutoErp[]>();
    let cursor = 0;

    const trabalhador = async () => {
      while (cursor < referencias.length) {
        const ref = referencias[cursor++];
        try {
          const dados = await this.erpApi.produtoPorReferencia(ref, empresa);
          resultado.set(ref, Array.isArray(dados) ? (dados as ProdutoErp[]) : []);
        } catch (e: any) {
          this.logger.warn(`[REFERENCIA] falha ao consultar "${ref}": ${e?.message ?? e}`);
        }
      }
    };

    await Promise.all(
      Array.from({ length: Math.min(CONCORRENCIA, referencias.length) }, () => trabalhador()),
    );
    return resultado;
  }
}
