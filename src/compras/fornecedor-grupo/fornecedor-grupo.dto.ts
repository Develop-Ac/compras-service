import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';

export class SalvarGrupoDto {
  @ApiPropertyOptional({ description: 'group_id existente; se ausente, cria um novo grupo' })
  group_id?: string | null;

  @ApiProperty({ description: 'for_codigo dos fornecedores do grupo', type: [Number] })
  membros!: number[];

  @ApiPropertyOptional({ description: 'for_codigo marcado como principal (matriz)' })
  principal?: number | null;
}

export class SalvarParametrosDto {
  @ApiProperty({ description: 'for_codigo do fornecedor de referência (âncora)' })
  for_codigo!: number;

  @ApiPropertyOptional({
    description: 'Aplicar também aos demais membros do grupo (herança matriz→filiais)',
    default: true,
  })
  aplicar_grupo?: boolean;

  @ApiPropertyOptional({ description: 'Lead time (pedido→recebimento) em dias; null = fallback global' })
  lead_time_dias?: number | null;

  @ApiPropertyOptional({ description: 'Período de revisão do fornecedor em dias; null/0 = revisão contínua' })
  tempo_revisao_dias?: number | null;

  @ApiPropertyOptional({ description: 'Pedido mínimo do fornecedor em R$; null = sem mínimo' })
  pedido_minimo_valor?: number | null;

  @ApiPropertyOptional({ description: 'Usuário que salvou (auditoria)' })
  updated_by?: string | null;
}
