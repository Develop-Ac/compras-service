import { Controller, Get, Query, Res } from '@nestjs/common';
import { NotaFiscalService } from './notaFiscal.service';
import { ApiOperation, ApiQuery, ApiResponse, ApiTags } from '@nestjs/swagger';
import type { Response } from 'express';

@ApiTags('Compras - Nota Fiscal')
@Controller('nota-fiscal')
export class NotaFiscalController {
  constructor(private readonly notaFiscalService: NotaFiscalService) {}

    @Get('nfe-distribuicao')
    async getNfeDistribuicao() {
    return this.notaFiscalService.getNfeDistribuicao();
    }

    @Get('danfe')
    @ApiOperation({ summary: 'Gera o DANFE em PDF para a chave NFe informada' })
    @ApiResponse({ status: 200, description: 'PDF gerado com sucesso', content: { 'application/pdf': {} } })
    @ApiQuery({ name: 'chaveNfe', type: String, required: true, description: 'Chave da NFe' })
    async generateDanfe(@Query('chaveNfe') chaveNfe: string, @Res() res: Response) {
    const pdfBuffer = await this.notaFiscalService.generateDanfe(chaveNfe);
    res.setHeader('Content-Type', 'application/pdf');
    res.setHeader('Content-Disposition', 'inline; filename=danfe.pdf');
    res.send(pdfBuffer);
    }
}