import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';

/**
 * As três tabelas de preço vigentes do negócio (US-038). O rótulo é de NEGÓCIO,
 * nunca a nomenclatura técnica do espelho (`preco_venda_1/2`): o comprador fala
 * varejo / atacado especial / atacado.
 */
export type TabelaPreco = 'varejo' | 'atacado_especial' | 'atacado';

/**
 * De onde saiu o valor:
 * - `erp`     — lido ao vivo do ERP (`produtos` via OPENQUERY [CONSULTA], read-only);
 * - `espelho` — veio de `com_fifo_completo` (fallback quando o ERP não respondeu);
 * - `ausente` — o produto não tem valor cadastrado em nenhuma das fontes.
 */
export type OrigemPreco = 'erp' | 'espelho' | 'ausente';

/** Metadados fixos de cada tabela — mapa único do domínio, sem duplicação por camada. */
export const TABELAS_PRECO: ReadonlyArray<{
  tabela: TabelaPreco;
  rotulo: string;
  campo_erp: string;
  campo_espelho: string | null;
}> = [
  {
    tabela: 'varejo',
    rotulo: 'Varejo',
    campo_erp: 'produtos.preco_venda',
    campo_espelho: 'com_fifo_completo.preco_venda_1',
  },
  {
    tabela: 'atacado_especial',
    rotulo: 'Atacado especial',
    campo_erp: 'produtos.preco2',
    campo_espelho: 'com_fifo_completo.preco_venda_2',
  },
  {
    // Não espelhado em com_fifo_completo — só existe no ERP (cerne da US-038).
    tabela: 'atacado',
    rotulo: 'Atacado',
    campo_erp: 'produtos.preco5',
    campo_espelho: null,
  },
];

/** Um preço vigente, rotulado pela tabela de negócio a que pertence. */
export class PrecoTabelaDto {
  @ApiProperty({
    description: 'Identificador da tabela de preço de negócio.',
    enum: ['varejo', 'atacado_especial', 'atacado'],
    example: 'atacado',
  })
  tabela!: TabelaPreco;

  @ApiProperty({ description: 'Rótulo de negócio para exibição.', example: 'Atacado' })
  rotulo!: string;

  @ApiProperty({
    description: 'Coluna do ERP que sustenta a tabela (rastreabilidade da origem).',
    example: 'produtos.preco5',
  })
  campo_erp!: string;

  @ApiPropertyOptional({
    description:
      'Coluna do espelho `com_fifo_completo` correspondente; null quando a tabela não é espelhada (caso do atacado).',
    example: 'com_fifo_completo.preco_venda_1',
    nullable: true,
  })
  campo_espelho!: string | null;

  @ApiPropertyOptional({
    description: 'Valor em R$; null apenas quando o produto não tem o preço cadastrado.',
    example: 189.9,
    nullable: true,
  })
  valor!: number | null;

  @ApiProperty({
    description: 'De onde o valor foi lido.',
    enum: ['erp', 'espelho', 'ausente'],
    example: 'erp',
  })
  origem!: OrigemPreco;

  @ApiPropertyOptional({
    description:
      'Data/hora do dado: instante da leitura quando origem = erp; `data_processamento` do espelho quando origem = espelho.',
    example: '2026-07-29T11:04:12.000Z',
    nullable: true,
  })
  atualizado_em!: string | null;
}

/** Os três preços vigentes de UM produto, com a idade do espelhamento. */
export class PrecoVigenteDto {
  @ApiProperty({ example: 38883 })
  pro_codigo!: number;

  @ApiPropertyOptional({ example: 'PASTILHA DE FREIO DIANTEIRA', nullable: true })
  pro_descricao!: string | null;

  @ApiProperty({
    description: 'Sempre 3 entradas: varejo, atacado especial e atacado — nesta ordem.',
    type: [PrecoTabelaDto],
  })
  precos!: PrecoTabelaDto[];

  @ApiProperty({
    description: 'Instante em que esta resposta foi montada (leitura do ERP).',
    example: '2026-07-29T11:04:12.000Z',
  })
  lido_em!: string;

  @ApiPropertyOptional({
    description:
      'Última atualização do espelhamento (`com_fifo_completo.data_processamento`) para este produto; null quando o produto não está no espelho.',
    example: '2026-07-29T06:00:00.000Z',
    nullable: true,
  })
  espelho_atualizado_em!: string | null;

  @ApiPropertyOptional({
    description:
      'Idade do espelhamento em horas no momento da leitura — quão velho é o dado espelhado.',
    example: 5.07,
    nullable: true,
  })
  espelho_idade_horas!: number | null;

  @ApiProperty({
    description:
      'true quando o ERP respondeu; false quando a resposta caiu inteira no espelho (e o atacado fica ausente).',
    example: true,
  })
  erp_disponivel!: boolean;
}
