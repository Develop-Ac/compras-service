import { ApiProperty } from '@nestjs/swagger';
import { IsInt, IsString, Length } from 'class-validator';
import { Type } from 'class-transformer';

/**
 * Body da rota de vinculação NFe <-> Pedido de Cotação.
 */
export class VincularNfeDto {
  @ApiProperty({
    description: 'Número do pedido de cotação (Firebird PEDIDOS_COTACOES).',
    example: 4293,
  })
  @Type(() => Number)
  @IsInt()
  pedido!: number;

  @ApiProperty({
    description: 'Chave de acesso da NF-e (44 dígitos).',
    example: '35260561736732003588550010001915931033927230',
  })
  @IsString()
  @Length(44, 44)
  nfe!: string;
}
