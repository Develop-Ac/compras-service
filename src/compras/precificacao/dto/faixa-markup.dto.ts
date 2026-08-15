import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';
// `import type`: TabelaPreco aparece em assinatura decorada e o projeto compila com
// isolatedModules + emitDecoratorMetadata (TS1272 se for import de valor).
import type { TabelaPreco } from './preco-vigente.dto';

/**
 * Régua de margem (US-040) — contratos da faixa de markup, do corredor e da
 * avaliação de piso.
 *
 * O vocabulário de tabela de preço é o MESMO da US-038 (`TabelaPreco` de
 * `preco-vigente.dto.ts`): varejo / atacado_especial / atacado. Não se duplica o
 * enum por camada.
 *
 * **Sem class-validator aqui de propósito.** O `compras-service` não tem
 * `ValidationPipe` global no `main.ts` (medido) e o precedente do módulo
 * (`preco-vigente.dto.ts`) documenta com `@ApiProperty` e valida no service.
 * Decorator de validação que ninguém executa é pior que nenhum: a validação de
 * entrada desta fatia vive em `ReguaService`, onde é testável.
 */

/**
 * Teto absoluto de markup aceito: `NUMERIC(8,4)` → 9999,9999%.
 *
 * Achado da T-023: `com_fifo_completo.margem_pct` é `Decimal(8,4)` (precisão total
 * **8**, não 15). As colunas de markup da régua adotam a mesma precisão para que a
 * margem gravada e o markup configurado nunca discordem por overflow de uma das
 * pontas. A régua de hoje vai a 1000% (faixa 0–15), bem abaixo do teto.
 */
export const MARKUP_PCT_MAXIMO = 9999.9999;

/** Uma faixa da régua, como está no banco (vigente ou encerrada). */
export class FaixaMarkupDto {
  @ApiProperty({ description: 'Identificador da linha de faixa.', example: 4 })
  id!: number;

  @ApiProperty({
    description: 'Tabela de preço a que a faixa pertence.',
    enum: ['varejo', 'atacado_especial', 'atacado'],
    example: 'varejo',
  })
  tabela!: TabelaPreco;

  @ApiProperty({ description: 'Rótulo de negócio da tabela.', example: 'Varejo' })
  rotulo!: string;

  @ApiProperty({
    description: 'Custo mínimo da faixa, em R$ — INCLUSIVO.',
    example: 99.01,
  })
  custo_min!: number;

  @ApiPropertyOptional({
    description:
      'Custo máximo da faixa, em R$ — EXCLUSIVO (igual ao custo_min da faixa seguinte). ' +
      'null na última faixa: sem limite superior.',
    example: 400.01,
    nullable: true,
  })
  custo_max!: number | null;

  @ApiProperty({
    description:
      'Piso INVIOLÁVEL de markup sobre o custo, em %. 100% significa preço = 2× custo.',
    example: 100,
  })
  markup_min_pct!: number;

  @ApiProperty({
    description:
      'Teto de markup da faixa, em %. Nas tabelas de markup único (atacado 67%, ' +
      'atacado especial 50%) é igual ao piso — o PRD declara um valor, não um corredor.',
    example: 110,
  })
  markup_max_pct!: number;

  @ApiProperty({ description: 'Início da vigência da faixa.', example: '2026-07-30T09:00:00.000Z' })
  vigencia_inicio!: string;

  @ApiPropertyOptional({
    description: 'Fim da vigência; null quando a faixa é a vigente.',
    example: null,
    nullable: true,
  })
  vigencia_fim!: string | null;

  @ApiProperty({ description: 'Quem criou esta versão da faixa.', example: 'u-1042' })
  criado_por!: string;

  @ApiProperty({ description: 'Quando esta versão foi criada.', example: '2026-07-30T09:00:00.000Z' })
  criado_em!: string;

  @ApiPropertyOptional({
    description: 'Quem encerrou a faixa (substituiu por outra); null na vigente.',
    nullable: true,
  })
  encerrado_por!: string | null;

  @ApiPropertyOptional({ description: 'Quando a faixa foi encerrada.', nullable: true })
  encerrado_em!: string | null;

  @ApiPropertyOptional({ description: 'Motivo declarado da alteração.', nullable: true })
  motivo!: string | null;
}

/** Corredor de markup aplicável a um custo numa tabela — o que o motor consome. */
export class CorredorMarkupDto {
  @ApiProperty({ enum: ['varejo', 'atacado_especial', 'atacado'], example: 'varejo' })
  tabela!: TabelaPreco;

  @ApiProperty({ example: 'Varejo' })
  rotulo!: string;

  @ApiProperty({ description: 'Custo composto avaliado, em R$.', example: 12 })
  custo!: number;

  @ApiProperty({ description: 'Faixa da régua que atendeu este custo.', example: 1 })
  faixa_id!: number;

  @ApiProperty({ description: 'Custo mínimo da faixa (inclusivo).', example: 0 })
  faixa_custo_min!: number;

  @ApiPropertyOptional({
    description: 'Custo máximo da faixa (exclusivo); null = sem limite.',
    example: 15.01,
    nullable: true,
  })
  faixa_custo_max!: number | null;

  @ApiProperty({ description: 'Piso de markup, em %.', example: 200 })
  markup_min_pct!: number;

  @ApiProperty({ description: 'Teto de markup, em %.', example: 1000 })
  markup_max_pct!: number;

  @ApiProperty({
    description:
      'Menor preço aceito, em R$: custo × (1 + markup_min_pct/100), arredondado PARA CIMA ' +
      'no centavo — arredondar para baixo furaria o piso por fração de centavo.',
    example: 36,
  })
  preco_minimo!: number;

  @ApiProperty({
    description:
      'Maior preço do corredor, em R$: custo × (1 + markup_max_pct/100), arredondado ' +
      'PARA BAIXO no centavo.',
    example: 132,
  })
  preco_maximo!: number;

  @ApiProperty({
    description: 'Instante da leitura da régua no banco (rastreio de qual versão respondeu).',
    example: '2026-07-30T12:00:00.000Z',
  })
  lido_em!: string;
}

/** Veredito da trava de piso sobre um preço proposto. */
export class AvaliacaoPrecoDto {
  @ApiProperty({
    description:
      'false SOMENTE quando o preço fica abaixo do piso da faixa — a única recusa dura ' +
      'da régua (piso inviolável).',
    example: false,
  })
  aceito!: boolean;

  @ApiProperty({ enum: ['varejo', 'atacado_especial', 'atacado'], example: 'varejo' })
  tabela!: TabelaPreco;

  @ApiProperty({ description: 'Custo composto usado na avaliação, em R$.', example: 12 })
  custo!: number;

  @ApiProperty({ description: 'Preço proposto, em R$.', example: 35.99 })
  preco!: number;

  @ApiProperty({ description: 'Markup que o preço proposto representa, em %.', example: 199.92 })
  markup_pct!: number;

  @ApiProperty({ description: 'Piso de markup da faixa, em %.', example: 200 })
  piso_markup_pct!: number;

  @ApiProperty({ description: 'Menor preço aceito, em R$.', example: 36 })
  preco_minimo!: number;

  @ApiProperty({ description: 'Maior preço do corredor, em R$.', example: 132 })
  preco_maximo!: number;

  @ApiPropertyOptional({
    description:
      'Motivo explícito da recusa, em pt-BR; null quando aceito. Sempre nomeia o piso, ' +
      'o preço mínimo e a faixa aplicada.',
    example:
      'Preço de R$ 35,99 abaixo do piso da tabela Varejo: markup 199,9167% < 200% ' +
      '(mínimo R$ 36,00 para custo de R$ 12,00 na faixa 0,00–15,01).',
    nullable: true,
  })
  motivo!: string | null;

  @ApiProperty({
    description:
      'true quando o preço passa do teto da faixa. NÃO recusa: a story declara ' +
      'inviolável só o limite inferior; o teto governa a PROPOSTA (US-041), não a ' +
      'aceitação de um preço informado. Vira aviso para o extrato de decisão.',
    example: false,
  })
  acima_do_teto!: boolean;

  @ApiPropertyOptional({
    description: 'Aviso de teto, em pt-BR; null quando o preço está dentro do corredor.',
    nullable: true,
  })
  aviso!: string | null;

  @ApiProperty({ description: 'Faixa da régua aplicada.', example: 1 })
  faixa_id!: number;
}

/** Entrada da avaliação de piso (POST /precificacao/regua/avaliar). */
export class AvaliarPrecoDto {
  @ApiProperty({ enum: ['varejo', 'atacado_especial', 'atacado'], example: 'varejo' })
  tabela!: TabelaPreco;

  @ApiProperty({ description: 'Custo composto do item, em R$ (> 0).', example: 12 })
  custo!: number;

  @ApiProperty({ description: 'Preço proposto ou a salvar, em R$.', example: 35.99 })
  preco!: number;
}

/**
 * Entrada da alteração de faixa (PUT /precificacao/regua/faixa).
 *
 * A faixa é identificada por `(tabela, custo_min)`: existir uma vigente com esse
 * `custo_min` significa substituição (a antiga é encerrada); não existir significa
 * faixa nova. `alterado_por` é obrigatório — direto no corpo ou via header
 * `x-user-id`; a régua não aceita alteração anônima (DoD: "quem alterou").
 */
export class AlterarFaixaDto {
  @ApiProperty({ enum: ['varejo', 'atacado_especial', 'atacado'], example: 'varejo' })
  tabela!: TabelaPreco;

  @ApiProperty({ description: 'Custo mínimo da faixa, em R$ — INCLUSIVO.', example: 0 })
  custo_min!: number;

  @ApiPropertyOptional({
    description: 'Custo máximo EXCLUSIVO; null/ausente = última faixa (sem limite).',
    example: 15.01,
    nullable: true,
  })
  custo_max?: number | null;

  @ApiProperty({ description: 'Novo piso de markup, em %.', example: 220 })
  markup_min_pct!: number;

  @ApiProperty({ description: 'Novo teto de markup, em %.', example: 1000 })
  markup_max_pct!: number;

  @ApiPropertyOptional({
    description: 'Quem está alterando. Obrigatório se o header x-user-id não vier.',
    example: 'u-1042',
  })
  alterado_por?: string;

  @ApiPropertyOptional({ description: 'Motivo da alteração (entra na trilha de auditoria).' })
  motivo?: string;
}
