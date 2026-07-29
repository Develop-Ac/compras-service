import { Controller, Get, Param, Query } from '@nestjs/common';
import { ApiOperation, ApiParam, ApiQuery, ApiResponse, ApiTags } from '@nestjs/swagger';
import { SinalGiroDto } from './dto/sinal-giro.dto';
import { GiroService } from './giro.service';

@ApiTags('Compras - Precificação')
@Controller('precificacao/giro')
export class GiroController {
  constructor(private readonly service: GiroService) {}

  // GET /compras/precificacao/giro?codigos=38883,38884
  @Get()
  @ApiOperation({
    summary: 'Sinal de giro (real e esperado) de vários produtos (lote)',
    description:
      'Uma ida a `com_relacionamento_itens` e uma a `com_fifo_completo` para o lote ' +
      'inteiro. Cada produto devolve giro real e esperado no escopo do grupo de itens ' +
      'iguais (ou do próprio produto, quando não há grupo), com a idade da ' +
      'materialização. Códigos desconhecidos não voltam na lista.',
  })
  @ApiQuery({
    name: 'codigos',
    description: 'pro_codigo separados por vírgula (máx. 500).',
    example: '38883,38884',
  })
  @ApiResponse({ status: 200, type: [SinalGiroDto] })
  @ApiResponse({ status: 400, description: 'Nenhum código válido ou lote acima do limite.' })
  async emLote(@Query('codigos') codigos: string): Promise<SinalGiroDto[]> {
    const lista = String(codigos ?? '')
      .split(',')
      .map((c) => c.trim())
      .filter(Boolean);
    return this.service.sinalGiroLote(lista);
  }

  // GET /compras/precificacao/giro/:proCodigo
  @Get(':proCodigo')
  @ApiOperation({
    summary: 'Sinal de giro (real e esperado) de um produto',
    description:
      'Giro real (`demanda média/dia`) e giro esperado (`demanda de planejamento/dia`) ' +
      'calculados sobre o GRUPO de itens iguais quando o grupo está materializado. ' +
      'Produto sem grupo devolve `sem_grupo: true` e o sinal do próprio produto — o motor ' +
      'de preço trata como exceção. O retorno sempre traz `materializado_em`, ' +
      '`idade_horas`/`idade_dias` e o `frescor` contra a janela aceitável de 24h.',
  })
  @ApiParam({ name: 'proCodigo', description: 'Código do produto no ERP.', example: 38883 })
  @ApiResponse({ status: 200, type: SinalGiroDto })
  @ApiResponse({ status: 400, description: 'pro_codigo inválido.' })
  @ApiResponse({ status: 404, description: 'Produto sem materialização e sem vínculo de grupo.' })
  async porProduto(@Param('proCodigo') proCodigo: string): Promise<SinalGiroDto> {
    return this.service.sinalGiro(proCodigo);
  }
}
