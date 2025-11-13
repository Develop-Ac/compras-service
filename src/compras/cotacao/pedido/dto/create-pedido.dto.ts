// src/pedido/dto/create-pedido.dto.ts
import { ArrayNotEmpty, IsArray, IsInt, ValidateNested } from 'class-validator';
import { Type } from 'class-transformer';
import { PedidoItemDto } from './pedido-item.dto';

export class CreatePedidoDto {
  @IsInt()
  pedido_cotacao!: number;

  @IsArray()
  @ArrayNotEmpty()
  @ValidateNested({ each: true })
  @Type(() => PedidoItemDto)
  itens!: PedidoItemDto[];
}
