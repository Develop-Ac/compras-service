import { Injectable } from '@nestjs/common';
import { ItensRepository } from './itens.repository';

@Injectable()
export class ItensService {
	constructor(private readonly itensRepository: ItensRepository) {}

	async getUltimaCompra(proCodigo: string) {
		return this.itensRepository.getUltimaCompra(proCodigo);
	}
}
