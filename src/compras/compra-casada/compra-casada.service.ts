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
   * (Stage_ProdutosSubgrupos) -> fornecedores (Stage_FornecedorSubgrupos).
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
      for_nome: text(r.FOR_NOME) ?? '',
      for_fone: text(r.FOR_FONE),
      for_celular: text(r.FOR_CELULAR),
      for_obs: text(r.FOR_OBS),
      for_uf: text(r.FOR_UF),
      ultima_compra: r.ULTIMA_COMPRA ?? null,
      data_carga: r.DATA_CARGA ?? null,
      carga_em: r.CARGA_EM ?? null,
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
