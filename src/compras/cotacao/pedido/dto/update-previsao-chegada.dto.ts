import { ApiProperty } from '@nestjs/swagger';
import { IsDateString, IsOptional } from 'class-validator';

export class UpdatePrevisaoChegadaDto {
  @ApiProperty({
    description: 'Nova previsão de chegada do pedido (ISO 8601). Envie null para limpar.',
    example: '2026-09-15T00:00:00.000Z',
    nullable: true,
  })
  @IsOptional()
  @IsDateString()
  previsao_chegada: string | null;
}
