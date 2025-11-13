// src/pedido/dto/pedido-item.dto.ts
import { IsInt, IsNumber, IsOptional, IsString, IsISO8601 } from 'class-validator';

export class PedidoItemDto {
  @IsOptional() @IsString()
  id?: string; // item_id_origem

  @IsInt()
  pro_codigo!: number;

  @IsString()
  pro_descricao!: string;

  @IsOptional() @IsString()
  mar_descricao?: string | null;

  @IsOptional() @IsString()
  referencia?: string | null;

  @IsOptional() @IsString()
  unidade?: string | null;

  @IsOptional() @IsISO8601()
  emissao?: string | null;

  @IsOptional() @IsNumber()
  valor_unitario?: number | null;

  @IsOptional() @IsNumber()
  custo_fabrica?: number | null;

  @IsOptional() @IsNumber()
  preco_custo?: number | null;

  @IsInt()
  for_codigo!: number;

  @IsNumber()
  quantidade!: number;
}
