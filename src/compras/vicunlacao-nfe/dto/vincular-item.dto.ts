import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';
import { IsInt, IsNumber, IsOptional, IsString } from 'class-validator';
import { Type } from 'class-transformer';

/**
 * Body do vínculo manual de um item da NF (xml_sem_vinculo) a um produto do
 * pedido, feito pela tela de conferência.
 */
export class VincularItemDto {
  @ApiProperty({ description: 'pro_codigo do produto do pedido escolhido.' })
  @Type(() => Number)
  @IsInt()
  pro_codigo!: number;

  @ApiPropertyOptional()
  @IsOptional()
  @IsString()
  pro_descricao?: string;

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
}
