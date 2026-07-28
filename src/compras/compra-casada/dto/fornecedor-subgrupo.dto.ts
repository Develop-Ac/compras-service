import { ApiProperty } from '@nestjs/swagger';

export class FornecedorSubgrupoDto {
  @ApiProperty({ description: 'Código do fornecedor', example: 1042 })
  for_codigo!: number;

  @ApiProperty({ description: 'Nome do fornecedor', example: 'BOSCH DO BRASIL LTDA' })
  for_nome!: string;

  @ApiProperty({
    description: 'Telefone do fornecedor',
    nullable: true,
    example: '(11) 3434-1200',
  })
  for_fone!: string | null;

  @ApiProperty({
    description: 'Celular do fornecedor',
    nullable: true,
    example: '(11) 99876-5432',
  })
  for_celular!: string | null;

  @ApiProperty({
    description: 'Observações cadastradas para o fornecedor',
    nullable: true,
    example: 'Pedido mínimo R$ 2.000,00',
  })
  for_obs!: string | null;

  @ApiProperty({ description: 'UF do fornecedor', nullable: true, example: 'SP' })
  for_uf!: string | null;

  @ApiProperty({
    description: 'Data da última compra',
    type: String,
    format: 'date-time',
    nullable: true,
    example: '2026-05-02T00:00:00.000Z',
  })
  ultima_compra!: Date | null;

  @ApiProperty({
    description: 'Data de referência da carga no BI',
    type: String,
    format: 'date-time',
    nullable: true,
    example: '2026-07-28T00:00:00.000Z',
  })
  data_carga!: Date | null;

  @ApiProperty({
    description: 'Momento em que a linha foi carregada no BI',
    type: String,
    format: 'date-time',
    nullable: true,
    example: '2026-07-28T03:15:00.000Z',
  })
  carga_em!: Date | null;
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
    description: 'Fornecedores do subgrupo, da compra mais recente para a mais antiga',
    type: [FornecedorSubgrupoDto],
  })
  fornecedores!: FornecedorSubgrupoDto[];
}
