import { Injectable, NotFoundException } from '@nestjs/common';
import { CompraCasadaFornecedoresRepository } from './compra-casada.repository';
import {
  FornecedorSubgrupoDto,
  FornecedoresPorProdutoDto,
} from './dto/fornecedor-subgrupo.dto';

/** O linked server devolve COUNT/SUM como string — normaliza para número. */
function num(v: unknown): number {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}

@Injectable()
export class CompraCasadaFornecedoresService {
  constructor(private readonly repository: CompraCasadaFornecedoresRepository) {}

  /**
   * Fornecedores que abastecem o subgrupo do produto informado.
   * Fluxo: pro_codigo -> subgrp_codigo (Stage_Produtos) -> subgrp_descricao
   * (Stage_ProdutosSubgrupos) -> compras por fornecedor no ERP (OPENQUERY).
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

    const rows = await this.repository.fornecedoresPorSubgrupo(subgrpDescricao);

    const fornecedores: FornecedorSubgrupoDto[] = rows.map((r) => ({
      for_codigo: num(r.FOR_CODIGO),
      for_nome: r.FOR_NOME,
      qtd_produtos: num(r.QTD_PRODUTOS),
      qtd_notas: num(r.QTD_NOTAS),
      qtd_comprada: num(r.QTD_COMPRADA),
      valor_total: num(r.VALOR_TOTAL),
      primeira_compra: r.PRIMEIRA_COMPRA ?? null,
      ultima_compra: r.ULTIMA_COMPRA ?? null,
    }));

    return {
      pro_codigo: proCodigo,
      subgrp_codigo: subgrpCodigo,
      subgrp_descricao: subgrpDescricao,
      total: fornecedores.length,
      fornecedores,
    };
  }
}
