import { ApiProperty } from '@nestjs/swagger';
import { IsNotEmpty, IsString, MaxLength } from 'class-validator';

export class UpdateItemJustificativaDto {
  @ApiProperty({ description: 'ID do item (com_pedido_itens)', example: 'cm123abc' })
  @IsString()
  @IsNotEmpty()
  id: string;

  @ApiProperty({ description: 'Justificativa do item', example: 'Produto em falta no estoque', maxLength: 100 })
  @IsString()
  @MaxLength(100)
  justificativa: string;
}
