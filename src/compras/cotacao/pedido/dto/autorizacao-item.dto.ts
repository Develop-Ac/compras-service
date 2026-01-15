import { IsBoolean, IsEnum, IsString } from 'class-validator';
import { ApiProperty } from '@nestjs/swagger';

export class AutorizacaoItemDto {
  @ApiProperty({
    description: 'ID do item do pedido',
    example: 'cm123abc456def'
  })
  @IsString()
  id: string;

  @ApiProperty({
    description: 'Coluna de autorização a ser alterada',
    enum: ['carlos', 'renato'],
    example: 'carlos'
  })
  @IsEnum(['carlos', 'renato'], { 
    message: 'coluna deve ser "carlos" ou "renato"' 
  })
  coluna: 'carlos' | 'renato';

  @ApiProperty({
    description: 'Estado da autorização',
    example: true
  })
  @IsBoolean()
  check: boolean;
}