// src/compras/cotacao.service.ts
import { Injectable, NotFoundException } from '@nestjs/common';
import { CotacaoRepository } from './cotacao.repository';
import { CreateCotacaoDto } from './cotacao.dto';

type ListAllParams = {
  empresa?: number;
  page: number;
  pageSize: number;
  includeItems: boolean;
};

@Injectable()
export class CotacaoService {
  constructor(private readonly repo: CotacaoRepository) {}

  async upsertCotacao(dto: CreateCotacaoDto) {
    const { empresa, pedido_cotacao, itens } = dto;

    const itensLower = (itens || []).map((i) => ({
      pedido_cotacao: i.PEDIDO_COTACAO,
      emissao: i.EMISSAO ? new Date(i.EMISSAO) : null,
      pro_codigo: Number(i.PRO_CODIGO),
      pro_descricao: i.PRO_DESCRICAO,
      mar_descricao: i.MAR_DESCRICAO ?? null,
      referencia: i.REFERENCIA ?? null,
      unidade: i.UNIDADE ?? null,
      quantidade: Number(i.QUANTIDADE),
      dt_ultima_compra: i.DT_ULTIMA_COMPRA ? new Date(i.DT_ULTIMA_COMPRA) : null,
    }));

    await this.repo.upsertCotacaoWithItems(empresa, pedido_cotacao, itensLower);

    return { ok: true, empresa, pedido_cotacao, total_itens: itensLower.length };
  }

  async getCotacao(empresa: number, pedido: number) {
    const header = await this.repo.getCotacaoHeader(pedido);

    if (!header || header.empresa !== empresa) {
      throw new NotFoundException('Pedido de cotação não encontrado.');
    }

    const itens = await this.repo.listItensByPedido(pedido);

    const itensUpper = itens.map((r) => ({
      PEDIDO_COTACAO: r.pedido_cotacao,
      EMISSAO: r.emissao ? r.emissao.toISOString() : null,
      PRO_CODIGO: r.pro_codigo,
      PRO_DESCRICAO: r.pro_descricao,
      MAR_DESCRICAO: r.mar_descricao,
      REFERENCIA: r.referencia,
      UNIDADE: r.unidade,
      QUANTIDADE: r.quantidade,
      DT_ULTIMA_COMPRA: (r as any).dt_ultima_compra ? (r as any).dt_ultima_compra.toISOString() : null,
    }));

    return {
      empresa: header.empresa,
      pedido_cotacao: header.pedido_cotacao,
      total_itens: itensUpper.length,
      itens: itensUpper,
    };
  }

  // <<< REESCRITO: sem relações >>>
  async listAll({ empresa, page, pageSize, includeItems }: ListAllParams) {
    const where = empresa != null ? { empresa } : {};

  const total = await this.repo.countCotacao(where);

    const headers = await this.repo.listHeaders(where, page, pageSize);

    const pedidos = headers.map((h) => h.pedido_cotacao);

    // count por pedido via groupBy
    const counts = pedidos.length ? await this.repo.groupItemCounts(pedidos) : [];

    const countMap = new Map<number, number>(
      counts.map((c) => [c.pedido_cotacao, c._count._all]),
    );

    // itens (opcional) em uma query única e agrupados em memória
    let itensMap = new Map<
      number,
      Array<{
        PEDIDO_COTACAO: number;
        EMISSAO: string | null;
        PRO_CODIGO: number;
        PRO_DESCRICAO: string;
        MAR_DESCRICAO: string | null;
        REFERENCIA: string | null;
        UNIDADE: string | null;
        QUANTIDADE: number;
        DT_ULTIMA_COMPRA: string | null;
      }>
    >();

    if (includeItems && pedidos.length) {
      const itens = await this.repo.listItensForPedidos(pedidos);

      itensMap = itens.reduce((map, r) => {
        const arr = map.get(r.pedido_cotacao) ?? [];
        arr.push({
          PEDIDO_COTACAO: r.pedido_cotacao,
          EMISSAO: r.emissao ? r.emissao.toISOString() : null,
          PRO_CODIGO: r.pro_codigo,
          PRO_DESCRICAO: r.pro_descricao,
          MAR_DESCRICAO: r.mar_descricao,
          REFERENCIA: r.referencia,
          UNIDADE: r.unidade,
          QUANTIDADE: r.quantidade,
          DT_ULTIMA_COMPRA: r.dt_ultima_compra ? r.dt_ultima_compra.toISOString() : null,
        });
        map.set(r.pedido_cotacao, arr);
        return map;
      }, itensMap);
    }

    const data = headers.map((h) => {
      const base = {
        empresa: h.empresa,
        pedido_cotacao: h.pedido_cotacao,
        total_itens: countMap.get(h.pedido_cotacao) ?? 0,
      };
      if (!includeItems) return base;
      return { ...base, itens: itensMap.get(h.pedido_cotacao) ?? [] };
    });

    return { total, page, pageSize, data };
  }

  async findByPedidoCotacao(pedidoCotacao: number) {
    const cotacao = await this.repo.findByPedidoCotacao(pedidoCotacao);
    
    if (!cotacao) {
      throw new NotFoundException(`Cotação com pedido_cotacao ${pedidoCotacao} não encontrada`);
    }
    
    return cotacao;
  }

  async delete(pedidoCotacao: number) {
    return this.repo.delete(pedidoCotacao);
  }
}
