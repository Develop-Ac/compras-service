import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';

export class SalvarGrupoDto {
  @ApiPropertyOptional({ description: 'group_id existente; se ausente, cria um novo grupo' })
  group_id?: string | null;

  @ApiProperty({ description: 'for_codigo dos fornecedores do grupo', type: [Number] })
  membros!: number[];

  @ApiPropertyOptional({ description: 'for_codigo marcado como principal (matriz)' })
  principal?: number | null;
}
