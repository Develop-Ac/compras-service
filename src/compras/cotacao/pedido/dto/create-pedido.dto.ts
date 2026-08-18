// src/pedido/dto/create-pedido.dto.ts
import { ApiProperty } from '@nestjs/swagger';
import { ArrayNotEmpty, IsArray, IsInt, IsString, ValidateNested } from 'class-validator';
import { Type } from 'class-transformer';
import { PedidoItemDto } from './pedido-item.dto';

export class CreatePedidoDto {
  @ApiProperty({ description: 'Número da cotação que originou o pedido', example: 1042 })
  @IsInt()
  pedido_cotacao!: number;

  @ApiProperty({ description: 'Usuário responsável pela geração do pedido', example: 'gabriel' })
  @IsString()
  usuario: string;

  @ApiProperty({
    description:
      'Itens do pedido. Os itens são agrupados por for_codigo, gerando um pedido por fornecedor.',
    type: [PedidoItemDto],
  })
  @IsArray()
  @ArrayNotEmpty()
  @ValidateNested({ each: true })
  @Type(() => PedidoItemDto)
  itens!: PedidoItemDto[];
}
