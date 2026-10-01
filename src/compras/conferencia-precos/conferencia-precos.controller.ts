import { Body, Controller, Get, Headers, HttpCode, Param, ParseIntPipe, Post, Query } from '@nestjs/common';
import { ApiBody, ApiHeader, ApiOperation, ApiQuery, ApiResponse, ApiTags } from '@nestjs/swagger';
import { ConferenciaPrecosService } from './conferencia-precos.service';
import { ConferenciaPrecosVarreduraService } from './conferencia-precos-varredura.service';
import { ConfirmarConferenciaDto } from './dto/confirmar-conferencia.dto';

@ApiTags('Compras - Conferência de preços')
@Controller('conferencia-precos')
export class ConferenciaPrecosController {
  constructor(
    private readonly service: ConferenciaPrecosService,
    private readonly varredura: ConferenciaPrecosVarreduraService,
  ) {}

  // GET /compras/conferencia-precos
  @Get()
  @ApiOperation({ summary: 'Fila de NFs da conferência de preços (ordem: entrada mais recente primeiro).' })
  @ApiQuery({ name: 'status', required: false, enum: ['pendente', 'conferida', 'removida'] })
  @ApiQuery({ name: 'desde', required: false, example: '2026-10-01' })
  @ApiQuery({ name: 'ate', required: false, example: '2026-10-31' })
  listar(@Query('status') status?: string, @Query('desde') desde?: string, @Query('ate') ate?: string) {
    return this.service.listar({ status, desde, ate });
  }

  // POST /compras/conferencia-precos/varredura — declarada antes de /:id.
  @Post('varredura')
  @HttpCode(200)
  @ApiOperation({ summary: 'Dispara a varredura do ERP (o mesmo método do cron).' })
  @ApiResponse({ status: 409, description: 'Varredura já em execução.' })
  varrer() {
    return this.varredura.varrer();
  }

  // GET /compras/conferencia-precos/:id
  @Get(':id')
  @ApiOperation({
    summary: 'Detalhe da NF: custo base, pisos e semáforos por item.',
    description: 'Pendente/removida: leitura ao vivo do ERP (refaz as contagens). Conferida: fotografia gravada.',
  })
  detalhe(@Param('id', ParseIntPipe) id: number) {
    return this.service.detalhe(id);
  }

  // POST /compras/conferencia-precos/:id/confirmar
  @Post(':id/confirmar')
  @HttpCode(200)
  @ApiOperation({ summary: 'Confirma a NF (definitivo) e grava a fotografia dos itens.' })
  @ApiHeader({ name: 'x-user-id', required: true, description: 'Usuário que confirma (conferido_por).' })
  @ApiBody({ type: ConfirmarConferenciaDto })
  @ApiResponse({ status: 400, description: 'Justificativa obrigatória (lista em itens_sem_justificativa).' })
  @ApiResponse({ status: 404, description: 'Conferência não encontrada.' })
  @ApiResponse({ status: 409, description: 'Já conferida ou removida.' })
  confirmar(
    @Param('id', ParseIntPipe) id: number,
    @Body() body: ConfirmarConferenciaDto,
    @Headers('x-user-id') usuario?: string,
  ) {
    return this.service.confirmar(id, body, usuario);
  }
}
