import { ApiProperty } from '@nestjs/swagger';
import { IsNumber, IsString, IsOptional } from 'class-validator';
import { Type } from 'class-transformer';

export class TransportadoraDto {
  @ApiProperty({ description: 'Nome do frete / transportadora', example: 'Transportadora XYZ' })
  @IsString()
  nomeFrete: string;

  @ApiProperty({ description: 'Valor do frete', example: 150.5 })
  @IsNumber()
  @Type(() => Number)
  frete: number;
}
