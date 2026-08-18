import { Injectable, NotFoundException } from '@nestjs/common';
import { CompraCasadaFornecedoresRepository } from './compra-casada.repository';
import {
  FornecedorSubgrupoDto,
  FornecedoresPorProdutoDto,
} from './dto/fornecedor-subgrupo.dto';

function num(v: unknown): number {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}

/** Campos texto da staging vêm com padding/vazio — normaliza para string ou null. */
function text(v: unknown): string | null {
  if (v == null) return null;
  const s = String(v).trim();
  return s.length ? s : null;
}

@Injectable()
export class CompraCasadaFornecedoresService {
  constructor(private readonly repository: CompraCasadaFornecedoresRepository) {}

  /**
   * Fornecedores que abastecem o subgrupo do produto informado.
   * Fluxo: pro_codigo -> subgrp_codigo (Stage_Produtos) -> subgrp_descricao
   * (Stage_ProdutosSubgrupos) -> fornecedores (Stage_FornecedorSubgrupos),
   * enriquecidos com a última compra do produto no ERP (OPENQUERY).
   */
  async porProduto(proCodigo: number): Promise<FornecedoresPorProdutoDto> {
    const subgrpCodigo = await this.repository.subgrupoDoProduto(proCodigo);
    if (subgrpCodigo == null) {
      throw new NotFoundException(
        `Produto ${proCodigo} não encontrado ou sem subgrupo cadastrado`,
      );
    }

    const subgrpDescricao = await this.repository.descricaoDoSubgrupo(subgrpCodigo);
    if (!subgrpDescricao) {
      throw new NotFoundException(
        `Subgrupo ${subgrpCodigo} do produto ${proCodigo} não encontrado`,
      );
    }

    // Staging (fornecedores do subgrupo) + ERP ao vivo (última compra do produto).
    const [rows, ultimas] = await Promise.all([
      this.repository.fornecedoresPorSubgrupo(subgrpDescricao),
      this.repository.ultimaCompraProdutoPorFornecedor(proCodigo),
    ]);

    // rn = 1 no OPENQUERY garante uma linha por fornecedor para este produto.
    const ultimaPorFornecedor = new Map(
      ultimas.map((u) => [num(u.FOR_CODIGO), u]),
    );

    const fornecedores: FornecedorSubgrupoDto[] = rows.map((r) => {
      const forCodigo = num(r.FOR_CODIGO);
      const ultima = ultimaPorFornecedor.get(forCodigo);

      return {
        for_codigo: forCodigo,
        for_nome: text(r.FOR_NOME) ?? '',
        for_fone: text(r.FOR_FONE),
        for_celular: text(r.FOR_CELULAR),
        for_obs: text(r.FOR_OBS),
        for_uf: text(r.FOR_UF),
        ultima_compra: r.ULTIMA_COMPRA ?? null,
        ultima_compra_produto: ultima?.ULTIMA_COMPRA ?? null,
        unitario_produto: ultima?.UNITARIO == null ? null : num(ultima.UNITARIO),
        data_carga: r.DATA_CARGA ?? null,
        carga_em: r.CARGA_EM ?? null,
      };
    });

    return {
      pro_codigo: proCodigo,
      subgrp_codigo: subgrpCodigo,
      subgrp_descricao: subgrpDescricao,
      total: fornecedores.length,
      fornecedores,
    };
  }
}
