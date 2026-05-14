import { ApiProperty } from '@nestjs/swagger';
import { IsString, IsNotEmpty } from 'class-validator';

export class UpdateStatusDto {
  @ApiProperty({
    description: 'Novo status do pedido',
    example: 'APROVADO',
  })
  @IsString()
  @IsNotEmpty()
  status: string;
}
