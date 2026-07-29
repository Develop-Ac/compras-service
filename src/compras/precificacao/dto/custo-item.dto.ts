import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';

/**
 * De onde saiu o número de uma parcela do custo. Auditabilidade é requisito da
 * US-037: o comprador precisa saber se a parcela veio do XML da NF, do CT-e, da
 * guia de ST, do pedido (fallback) ou se simplesmente não existe para o item.
 */
export type OrigemParcela = 'xml' | 'pedido' | 'cte' | 'guia' | 'ausente';

/** Uma das 5 parcelas do custo composto, sempre POR UNIDADE (unidade base). */
export class ParcelaCustoDto {
  @ApiProperty({ description: 'Valor da parcela em R$ por unidade base.', example: 12.345 })
  valor!: number;

  @ApiProperty({
    description: 'Origem do dado da parcela.',
    enum: ['xml', 'pedido', 'cte', 'guia', 'ausente'],
    example: 'xml',
  })
  origem!: OrigemParcela;

  @ApiPropertyOptional({
    description: 'Explicação curta de como a parcela foi obtida/rateada.',
    example: 'CT-e 3526... (tomador_nos) rateado por valor do item',
  })
  detalhe?: string;
}

/** As 5 parcelas discriminadas do custo do item. */
export class ParcelasCustoDto {
  @ApiProperty({ type: ParcelaCustoDto, description: 'Preço unitário líquido (sem IPI, já com desconto/acréscimo).' })
  unitario!: ParcelaCustoDto;

  @ApiProperty({ type: ParcelaCustoDto, description: 'ICMS ST destacado no XML da NF (vICMSST + vFCPST).' })
  icms_st!: ParcelaCustoDto;

  @ApiProperty({
    type: ParcelaCustoDto,
    description: 'ICMS ST complementar: guia recolhida pela empresa, rateada entre os itens com ST.',
  })
  icms_st_complementar!: ParcelaCustoDto;

  @ApiProperty({ type: ParcelaCustoDto, description: 'IPI da linha do XML (vIPI) por unidade.' })
  ipi!: ParcelaCustoDto;

  @ApiProperty({
    type: ParcelaCustoDto,
    description: 'Frete por nossa conta (CT-e com tomador_nos = true) rateado por valor do item. Zero quando o frete é do fornecedor.',
  })
  frete!: ParcelaCustoDto;
}

/** Custo composto de UM item de NF conferida. */
export class CustoItemDto {
  @ApiProperty({ example: 38883 })
  pro_codigo!: number;

  @ApiProperty({ example: 'PASTILHA DE FREIO DIANTEIRA' })
  pro_descricao!: string;

  @ApiPropertyOptional({ example: 'UN', nullable: true })
  unidade!: string | null;

  @ApiProperty({
    description: 'Quantidade faturada na unidade base (fallback: quantidade do pedido quando não há NF).',
    example: 12,
  })
  quantidade!: number;

  @ApiProperty({ description: 'Chaves de NF-e que cobrem este item.', type: [String] })
  chaves_nfe!: string[];

  @ApiProperty({ description: 'Custo unitário composto = soma das 5 parcelas.', example: 51.2345 })
  custo_unitario!: number;

  @ApiProperty({ description: 'custo_unitario × quantidade.', example: 614.81 })
  custo_total!: number;

  @ApiProperty({ type: ParcelasCustoDto })
  parcelas!: ParcelasCustoDto;
}

/** Totais agregados da composição (somatório das parcelas × quantidade). */
export class TotaisCustoDto {
  @ApiProperty({ example: 789 })
  itens!: number;

  @ApiProperty({ example: 48210.55 })
  custo_total!: number;

  @ApiProperty({ example: 39100.0 })
  unitario!: number;

  @ApiProperty({ example: 5200.11 })
  icms_st!: number;

  @ApiProperty({ example: 310.44 })
  icms_st_complementar!: number;

  @ApiProperty({ example: 2800.0 })
  ipi!: number;

  @ApiProperty({ example: 800.0 })
  frete!: number;
}

/** Resposta do endpoint: composição de custo de todos os itens em UMA requisição. */
export class CustoComposicaoDto {
  @ApiProperty({ example: 'clx0k9v2h0000abcd1234efgh' })
  pedido_id!: string;

  @ApiPropertyOptional({ example: 1234, nullable: true })
  for_codigo!: number | null;

  @ApiProperty({
    description:
      'Flag do pedido: o valor negociado já inclui IPI. NÃO altera o custo composto (o IPI é sempre parcela própria); serve para conferência contra o pedido.',
    example: false,
  })
  ipi_no_valor!: boolean;

  @ApiPropertyOptional({
    description: 'Chave da NF quando a consulta foi filtrada por nota; null quando é o pedido inteiro.',
    nullable: true,
  })
  chave_nfe!: string | null;

  @ApiProperty({ type: TotaisCustoDto })
  totais!: TotaisCustoDto;

  @ApiProperty({ type: [CustoItemDto] })
  itens!: CustoItemDto[];
}
