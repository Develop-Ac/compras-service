
import { Controller, Get, Query } from '@nestjs/common';
import { ApiTags } from '@nestjs/swagger';
import { ItensService } from './itens.service';

@ApiTags('Compras - Pedidos de Cotação')
@Controller('itens')
export class ItensController {
	constructor(private readonly itensService: ItensService) {}

	@Get('ultima-compra')
	async getUltimaCompra(@Query('pro_codigo') proCodigo: string) {
		if (!proCodigo) {
			return { error: 'pro_codigo é obrigatório' };
		}
		return this.itensService.getUltimaCompra(proCodigo);
	}
}


