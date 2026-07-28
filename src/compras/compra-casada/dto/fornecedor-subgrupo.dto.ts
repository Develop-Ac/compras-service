import { ApiProperty } from '@nestjs/swagger';

export class FornecedorSubgrupoDto {
  @ApiProperty({ description: 'Código do fornecedor', example: 1042 })
  for_codigo!: number;

  @ApiProperty({ description: 'Nome do fornecedor', example: 'BOSCH DO BRASIL LTDA' })
  for_nome!: string;

  @ApiProperty({
    description: 'Produtos distintos do subgrupo já comprados deste fornecedor',
    example: 37,
  })
  qtd_produtos!: number;

  @ApiProperty({ description: 'Notas de entrada distintas', example: 12 })
  qtd_notas!: number;

  @ApiProperty({ description: 'Quantidade total comprada', example: 1540 })
  qtd_comprada!: number;

  @ApiProperty({ description: 'Valor total comprado (R$)', example: 87540.35 })
  valor_total!: number;

  @ApiProperty({
    description: 'Data da primeira compra',
    type: String,
    format: 'date-time',
    nullable: true,
    example: '2021-03-14T00:00:00.000Z',
  })
  primeira_compra!: Date | null;

  @ApiProperty({
    description: 'Data da última compra',
    type: String,
    format: 'date-time',
    nullable: true,
    example: '2026-05-02T00:00:00.000Z',
  })
  ultima_compra!: Date | null;
}

export class FornecedoresPorProdutoDto {
  @ApiProperty({ description: 'Código do produto consultado', example: 51234 })
  pro_codigo!: number;

  @ApiProperty({ description: 'Código do subgrupo do produto', example: 118 })
  subgrp_codigo!: number;

  @ApiProperty({ description: 'Descrição do subgrupo do produto', example: 'PASTILHA DE FREIO' })
  subgrp_descricao!: string;

  @ApiProperty({ description: 'Quantidade de fornecedores retornados', example: 8 })
  total!: number;

  @ApiProperty({
    description: 'Fornecedores do subgrupo, do maior para o menor valor comprado',
    type: [FornecedorSubgrupoDto],
  })
  fornecedores!: FornecedorSubgrupoDto[];
}
