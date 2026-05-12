import { ApiProperty } from '@nestjs/swagger';
import { IsNotEmpty, IsNumber, IsPositive, IsString } from 'class-validator';

export class UpdateItemQuantidadeDto {
  @ApiProperty({ description: 'ID do item (com_pedido_itens)', example: 'cm123abc' })
  @IsString()
  @IsNotEmpty()
  id: string;

  @ApiProperty({ description: 'Nova quantidade do item', example: 10 })
  @IsNumber()
  @IsPositive()
  quantidade: number;
}
