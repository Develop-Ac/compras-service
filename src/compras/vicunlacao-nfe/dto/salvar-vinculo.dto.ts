import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';
import {
  IsArray,
  IsInt,
  IsNumber,
  IsOptional,
  IsString,
  Length,
  ValidateNested,
} from 'class-validator';
import { Type } from 'class-transformer';

/**
 * Item do snapshot de conferência (uma linha de qualquer das 3 listas).
 */
export class ItemVinculoDto {
  @ApiProperty({
    description: "Tipo da linha: 'vinculado' | 'xml_sem_vinculo' | 'pedido_sem_vinculo'.",
    example: 'vinculado',
  })
  @IsString()
  tipo!: string;

  // dados do lado XML
  @ApiPropertyOptional()
  @IsOptional()
  @IsString()
  produto_xml?: string;

  @ApiPropertyOptional()
  @IsOptional()
  @IsString()
  cprod_xml?: string;

  @ApiPropertyOptional()
  @IsOptional()
  @Type(() => Number)
  @IsNumber()
  quantidade_xml?: number;

  @ApiPropertyOptional()
  @IsOptional()
  @Type(() => Number)
  @IsNumber()
  vuncom_xml?: number;

  // dados do lado pedido/cotação
  @ApiPropertyOptional()
  @IsOptional()
  @Type(() => Number)
  @IsInt()
  pro_codigo?: number;

  @ApiPropertyOptional()
  @IsOptional()
  @IsString()
  pro_descricao?: string;

  @ApiPropertyOptional()
  @IsOptional()
  @Type(() => Number)
  @IsNumber()
  quantidade_cotacao?: number;

  @ApiPropertyOptional()
  @IsOptional()
  @Type(() => Number)
  @IsNumber()
  quantidade_pedido?: number;

  @ApiPropertyOptional()
  @IsOptional()
  @Type(() => Number)
  @IsNumber()
  valor_pedido?: number;

  // metadados do casamento
  @ApiPropertyOptional()
  @IsOptional()
  @IsString()
  match_campo?: string;

  @ApiPropertyOptional()
  @IsOptional()
  @IsString()
  match_valor?: string;

  @ApiPropertyOptional({ description: "Origem do vínculo: 'auto' | 'manual'." })
  @IsOptional()
  @IsString()
  origem?: string;
}

/**
 * Body do upsert do vínculo NF-e × Pedido (cabeçalho + itens tipados).
 */
export class SalvarVinculoDto {
  @ApiProperty({ description: 'FK com_pedido.id (contexto da tela do pedido).' })
  @IsString()
  pedido_id!: string;

  @ApiProperty({ description: 'Número do pedido de cotação.', example: 4293 })
  @Type(() => Number)
  @IsInt()
  pedido_cotacao!: number;

  @ApiPropertyOptional()
  @IsOptional()
  @Type(() => Number)
  @IsInt()
  for_codigo?: number;

  @ApiProperty({ description: 'Chave de acesso da NF-e (44 dígitos).' })
  @IsString()
  @Length(44, 44)
  chave_nfe!: string;

  @ApiPropertyOptional()
  @IsOptional()
  @IsString()
  emitente?: string;

  @ApiPropertyOptional({ description: 'Data de emissão (ISO).' })
  @IsOptional()
  @IsString()
  data_emissao?: string;

  @ApiPropertyOptional()
  @IsOptional()
  @Type(() => Number)
  @IsNumber()
  valor_total?: number;

  @ApiPropertyOptional()
  @IsOptional()
  @IsString()
  usuario?: string;

  @ApiProperty({ type: [ItemVinculoDto] })
  @IsArray()
  @ValidateNested({ each: true })
  @Type(() => ItemVinculoDto)
  itens!: ItemVinculoDto[];
}
