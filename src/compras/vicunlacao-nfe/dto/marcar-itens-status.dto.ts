import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';

/** Um item do pedido cujo status manual será marcado/revertido. */
export class MarcarItemStatusDto {
  @ApiProperty({ description: 'pro_codigo do item do pedido' })
  pro_codigo!: number;

  @ApiProperty({ description: 'for_codigo do item do pedido (chave junto com pro_codigo)' })
  for_codigo!: number;

  @ApiPropertyOptional({
    description: "'nao_atendido' para marcar Não Atendido pelo Fornecedor; null para reverter a pendente",
    nullable: true,
    example: 'nao_atendido',
  })
  status_item?: string | null;
}

/** Lote de itens do pedido para marcar/reverter status no fechamento. */
export class MarcarItensStatusDto {
  @ApiProperty({ type: [MarcarItemStatusDto] })
  itens!: MarcarItemStatusDto[];

  @ApiPropertyOptional({ description: 'Usuário que fez a ação (auditoria)' })
  usuario?: string | null;
}
