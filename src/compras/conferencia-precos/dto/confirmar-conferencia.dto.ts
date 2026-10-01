import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';

export class JustificativaItemDto {
  @ApiProperty({ example: 4384 })
  pro_codigo!: number;

  @ApiProperty({ example: 'Preço de concorrente; mantido por decisão da gerência.' })
  texto!: string;
}

/** Confirmação da NF. Item abaixo do piso ou sem custo exige justificativa. */
export class ConfirmarConferenciaDto {
  @ApiPropertyOptional()
  observacao?: string;

  @ApiProperty({ type: [JustificativaItemDto] })
  justificativas!: JustificativaItemDto[];
}
