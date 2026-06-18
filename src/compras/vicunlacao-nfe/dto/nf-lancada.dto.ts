import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';
import { IsArray, IsOptional, IsString, Length, ValidateNested } from 'class-validator';
import { Type } from 'class-transformer';

/**
 * Uma NF-e lançada no ERP (status_erp='LANCADA'), com a chave de acesso e a
 * data de entrada (DT_ENTRADA) quando disponível.
 */
export class NfLancadaItemDto {
  @ApiProperty({
    description: 'Chave de acesso da NF-e (44 dígitos).',
    example: '35260561736732003588550010001915931033927230',
  })
  @IsString()
  @Length(44, 44)
  chave_nfe!: string;

  @ApiPropertyOptional({
    description:
      'Data de entrada da NF-e (DT_ENTRADA) em ISO. Se ausente/null, usa now().',
    example: '2026-06-18T00:00:00.000Z',
    nullable: true,
  })
  @IsOptional()
  @IsString()
  dt_entrada?: string | null;
}

/**
 * Body da rota POST /compras/vinculacao-nfe/nf-lancada.
 * Recebe o lote de NF-e que viraram LANCADA no ERP para marcar os pedidos
 * vinculados como 'Entregue' e gravar a data de recebimento.
 */
export class NfLancadaDto {
  @ApiProperty({
    description: 'Lote de NF-e lançadas (chave + dt_entrada opcional).',
    type: [NfLancadaItemDto],
  })
  @IsArray()
  @ValidateNested({ each: true })
  @Type(() => NfLancadaItemDto)
  lancadas!: NfLancadaItemDto[];
}
