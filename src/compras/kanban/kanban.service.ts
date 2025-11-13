import { Injectable } from '@nestjs/common';
import { kanbanRepository } from './kanban.repository';

@Injectable()
export class kanbanService {
  constructor(private readonly repo: kanbanRepository) {}

  async getKanban() {
    const kanban = await this.repo.findFirst();
    return kanban?.data ?? {
      cadastro_produto: [],
      criacao_pedido: [],
      conferencia_pedido: [],
      digitacao_pedido: [],
      acompanhamento_pedido: [],
      conciliacao_pedido: [],
      recebimento: [],
      checkin: [],
      codificacao: [],
      impressao_mapa: [],
      arquivo: []
    };
  }

  async updateKanban(data: any) {
    const existing = await this.repo.findFirst();
    if (existing) {
      await this.repo.update(existing.id, data);
    } else {
      await this.repo.create(data);
    }
    return { ok: true };
  }
}
