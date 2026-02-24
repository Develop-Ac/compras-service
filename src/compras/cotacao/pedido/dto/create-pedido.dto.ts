// src/pedido/dto/create-pedido.dto.ts
import { ArrayNotEmpty, IsArray, IsInt, IsString, ValidateNested } from 'class-validator';
import { Type } from 'class-transformer';
import { PedidoItemDto } from './pedido-item.dto';

export class CreatePedidoDto {
  @IsInt()
  pedido_cotacao!: number;

  @IsString()
  usuario: string;

  @IsArray()
  @ArrayNotEmpty()
  @ValidateNested({ each: true })
  @Type(() => PedidoItemDto)
  itens!: PedidoItemDto[];
}
