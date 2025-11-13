// src/compras/dto/create-cotacao.dto.ts
import { Type } from 'class-transformer';
import { IsArray, IsInt, IsISO8601, IsNotEmpty, IsNumber, IsOptional, IsString, ValidateNested } from 'class-validator';
import { ApiProperty } from '@nestjs/swagger';

export class CotacaoItemDto {
  @IsInt() PEDIDO_COTACAO: number;

  @IsOptional()
  @IsISO8601()
  EMISSAO?: string | null;

  @IsNumber() PRO_CODIGO: number;
  @IsString() PRO_DESCRICAO: string;

  @IsOptional() @IsString() MAR_DESCRICAO?: string | null;
  @IsOptional() @IsString() REFERENCIA?: string | null;
  @IsOptional() @IsString() UNIDADE?: string | null;

  @IsNumber() QUANTIDADE: number;

  @ApiProperty({
    description: 'Data da última compra do produto',
    example: '2024-11-10T10:30:00.000Z',
    required: false
  })
  @IsOptional()
  @IsISO8601()
  DT_ULTIMA_COMPRA?: string | null;
}

export class CreateCotacaoDto {
  @IsInt() empresa: number;
  @IsInt() pedido_cotacao: number;

  @IsArray()
  @ValidateNested({ each: true })
  @Type(() => CotacaoItemDto)
  itens: CotacaoItemDto[];
}
