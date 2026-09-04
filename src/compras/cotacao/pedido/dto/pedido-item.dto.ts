// src/pedido/dto/pedido-item.dto.ts
import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';
import { IsBoolean, IsInt, IsNumber, IsOptional, IsString, IsISO8601 } from 'class-validator';

export class PedidoItemDto {
  @ApiPropertyOptional({ description: 'ID do item na origem (item_id_origem)', example: 'cm123abc' })
  @IsOptional() @IsString()
  id?: string; // item_id_origem

  @ApiProperty({ description: 'Código do produto', example: 10523 })
  @IsInt()
  pro_codigo!: number;

  @ApiProperty({ description: 'Descrição do produto', example: 'PALHETA LIMPADOR 16"' })
  @IsString()
  pro_descricao!: string;

  @ApiPropertyOptional({ description: 'Descrição da marca', example: 'DYNA', nullable: true })
  @IsOptional() @IsString()
  mar_descricao?: string | null;

  @ApiPropertyOptional({ description: 'Referência do produto', example: 'DY-1602', nullable: true })
  @IsOptional() @IsString()
  referencia?: string | null;

  @ApiPropertyOptional({ description: 'Unidade de medida', example: 'PC', nullable: true })
  @IsOptional() @IsString()
  unidade?: string | null;

  @ApiPropertyOptional({
    description: 'Data de emissão da última compra (ISO 8601)',
    example: '2026-08-01T00:00:00.000Z',
    nullable: true,
  })
  @IsOptional() @IsISO8601()
  emissao?: string | null;

  @ApiPropertyOptional({ description: 'Valor unitário negociado', example: 24.9, nullable: true })
  @IsOptional() @IsNumber()
  valor_unitario?: number | null;

  @ApiPropertyOptional({ description: 'Custo de fábrica', example: 18.35, nullable: true })
  @IsOptional() @IsNumber()
  custo_fabrica?: number | null;

  @ApiPropertyOptional({ description: 'Preço de custo', example: 21.7, nullable: true })
  @IsOptional() @IsNumber()
  preco_custo?: number | null;

  @ApiPropertyOptional({ description: 'Percentual de IPI do item', example: 9.75, nullable: true })
  @IsOptional() @IsNumber()
  ipi?: number | null;

  @ApiPropertyOptional({ description: 'Indica se o item possui ICMS', example: true, nullable: true })
  @IsOptional() @IsBoolean()
  icms?: boolean | null;

  @ApiProperty({ description: 'Código do fornecedor', example: 4312 })
  @IsInt()
  for_codigo!: number;

  @ApiProperty({ description: 'Valor do frete do fornecedor', example: 150.5 })
  @IsNumber()
  frete!: number;

  @ApiProperty({ description: 'Prazo de entrega/pagamento do fornecedor', example: '30/60/90' })
  @IsString()
  prazo!: string;

  @ApiProperty({ description: 'Nome do frete / transportadora', example: 'Transportadora XYZ' })
  @IsString()
  nomeFrete!: string;

  @ApiPropertyOptional({
    description:
      'Previsão de chegada do pedido do fornecedor (ISO 8601). Salva na coluna previsao_chegada de com_pedido.',
    example: '2026-10-10T00:00:00.000Z',
    nullable: true,
  })
  @IsOptional() @IsISO8601()
  previsao_chegada?: string | null;

  @ApiProperty({ description: 'Quantidade comprada', example: 120 })
  @IsNumber()
  quantidade!: number;

  @ApiPropertyOptional({ description: 'Quantidade sugerida mínima', example: 80, nullable: true })
  @IsOptional() @IsNumber()
  qtd_sugerida_min?: number | null;

  @ApiPropertyOptional({ description: 'Quantidade sugerida máxima', example: 200, nullable: true })
  @IsOptional() @IsNumber()
  qtd_sugerida_max?: number | null;

  @ApiPropertyOptional({
    description: 'Justificativa quando a quantidade sai da faixa sugerida (máx. 100 caracteres)',
    example: 'Compra antecipada por reajuste',
    nullable: true,
  })
  @IsString()
  justificativa?: string | null;
}
