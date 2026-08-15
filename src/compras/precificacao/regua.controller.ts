import { BadRequestException, Body, Controller, Get, Headers, Post, Put, Query } from '@nestjs/common';
import {
  ApiBody,
  ApiHeader,
  ApiOperation,
  ApiQuery,
  ApiResponse,
  ApiTags,
} from '@nestjs/swagger';
import {
  AlterarFaixaDto,
  AvaliacaoPrecoDto,
  AvaliarPrecoDto,
  CorredorMarkupDto,
  FaixaMarkupDto,
} from './dto/faixa-markup.dto';
import { ReguaService } from './regua.service';

/**
 * Régua de margem configurável (US-040 / T-024).
 *
 * Público e documentado por Swagger, como o resto do `compras-service`
 * (`CODING_STANDARDS#convencoes-backend-compras-service`: não existem `AuthGuard`
 * nem `pode()` neste serviço — introduzir guard aqui seria mudança de arquitetura).
 * A **identidade de quem altera** a régua, exigida pelo DoD, vem do header
 * `x-user-id` (o mesmo que o proxy do portal injeta) ou do campo `alterado_por`; sem
 * um dos dois a alteração é recusada com 400.
 */
@ApiTags('Compras - Precificação')
@Controller('precificacao/regua')
export class ReguaController {
  constructor(private readonly service: ReguaService) {}

  // GET /compras/precificacao/regua
  @Get()
  @ApiOperation({
    summary: 'Faixas VIGENTES da régua de margem das três tabelas',
    description:
      'Lidas de com_precificacao_faixa. Intervalo de custo meio-aberto ' +
      '[custo_min, custo_max) — custo_max nulo na última faixa. A não-monotonicidade ' +
      'entre as duas últimas faixas do varejo é intencional (PRD#escopo).',
  })
  @ApiResponse({ status: 200, type: [FaixaMarkupDto] })
  @ApiResponse({ status: 503, description: 'Régua não configurada ou inconsistente (DDL pendente).' })
  async vigentes(): Promise<FaixaMarkupDto[]> {
    return this.service.faixasVigentes();
  }

  // GET /compras/precificacao/regua/historico?tabela=varejo
  @Get('historico')
  @ApiOperation({
    summary: 'Trilha de auditoria da régua — quem alterou cada faixa e quando',
    description:
      'Faixas vigentes e encerradas, mais recente primeiro. Alterar uma faixa não ' +
      'sobrescreve a anterior: encerra (vigencia_fim/encerrado_por) e insere outra.',
  })
  @ApiQuery({
    name: 'tabela',
    required: false,
    description: 'varejo | atacado_especial | atacado. Ausente = todas.',
    example: 'varejo',
  })
  @ApiResponse({ status: 200, type: [FaixaMarkupDto] })
  async historico(@Query('tabela') tabela?: string): Promise<FaixaMarkupDto[]> {
    return this.service.historico(tabela);
  }

  // GET /compras/precificacao/regua/corredor?tabela=varejo&custo=12
  @Get('corredor')
  @ApiOperation({
    summary: 'Corredor de markup (mínimo/máximo) para um custo numa tabela',
    description:
      'Devolve o piso e o teto de markup e os preços correspondentes. 100% significa ' +
      'preço = 2× custo. O preço mínimo arredonda PARA CIMA no centavo, para não furar ' +
      'o piso por fração de centavo.',
  })
  @ApiQuery({ name: 'tabela', description: 'varejo | atacado_especial | atacado', example: 'varejo' })
  @ApiQuery({ name: 'custo', description: 'Custo composto do item em R$ (> 0).', example: 12 })
  @ApiResponse({ status: 200, type: CorredorMarkupDto })
  @ApiResponse({ status: 400, description: 'Tabela inválida ou custo não positivo.' })
  @ApiResponse({ status: 503, description: 'Régua não configurada ou inconsistente.' })
  async corredor(
    @Query('tabela') tabela: string,
    @Query('custo') custo: string,
  ): Promise<CorredorMarkupDto> {
    return this.service.corredor(tabela, Number(String(custo ?? '').replace(',', '.')));
  }

  // POST /compras/precificacao/regua/avaliar
  @Post('avaliar')
  @ApiOperation({
    summary: 'Avalia um preço contra o piso inviolável da faixa',
    description:
      'Devolve aceito=false com motivo explícito quando o preço fica abaixo do piso ' +
      '(varejo: mínimo da faixa; atacado: 67%; atacado especial: 50%). Passar do teto ' +
      'NÃO recusa — devolve aceito=true com acima_do_teto=true e aviso, porque a ' +
      'story declara inviolável apenas o limite inferior.',
  })
  @ApiBody({ type: AvaliarPrecoDto })
  @ApiResponse({ status: 201, type: AvaliacaoPrecoDto })
  @ApiResponse({ status: 400, description: 'Tabela inválida, custo não positivo ou preço inválido.' })
  @ApiResponse({ status: 503, description: 'Régua não configurada ou inconsistente.' })
  async avaliar(@Body() body: AvaliarPrecoDto): Promise<AvaliacaoPrecoDto> {
    if (!body) throw new BadRequestException('Corpo obrigatório: { tabela, custo, preco }.');
    return this.service.avaliarPreco(body.tabela, body.custo, body.preco);
  }

  // PUT /compras/precificacao/regua/faixa
  @Put('faixa')
  @ApiOperation({
    summary: 'Altera (versiona) uma ou mais faixas da régua — sem deploy',
    description:
      'A faixa é identificada por (tabela, custo_min). A vigente é ENCERRADA e uma nova ' +
      'entra, registrando quem alterou e quando. Aceita um objeto ou um ARRAY: dividir ' +
      'uma faixa em duas exige enviar as duas no mesmo lote, senão a régua ficaria com ' +
      'buraco de cobertura e a alteração é recusada com 400. O corredor devolvido pelas ' +
      'consultas muda na avaliação seguinte, sem rebuild nem restart.',
  })
  @ApiHeader({
    name: 'x-user-id',
    required: false,
    description: 'Quem está alterando. Dispensável apenas se `alterado_por` vier no corpo.',
  })
  @ApiBody({ type: [AlterarFaixaDto] })
  @ApiResponse({ status: 200, type: [FaixaMarkupDto] })
  @ApiResponse({
    status: 400,
    description: 'Autor ausente, limites inválidos ou alteração que deixaria buraco na régua.',
  })
  async alterar(
    @Body() body: AlterarFaixaDto | AlterarFaixaDto[],
    @Headers('x-user-id') userId?: string,
  ): Promise<FaixaMarkupDto[]> {
    if (!body) throw new BadRequestException('Corpo obrigatório: faixa ou lista de faixas.');
    const lote = Array.isArray(body) ? body : [body];
    return this.service.alterarFaixas(lote, userId);
  }
}
