import { Injectable, Logger } from '@nestjs/common';
import { Cron } from '@nestjs/schedule';
import { VinculacaoNfeService } from './vinculacao-nfe.service';
import { VinculacaoNfeRepository } from './vinculacao-nfe.repository';

/**
 * Job periódico que mantém os pedidos em dia com o transporte:
 *  - sincroniza os campos de transportadora (nomeFrete/frete/categoriaFrete) a partir
 *    do(s) CT-e(s) das NFs vinculadas;
 *  - recalcula o status, fazendo o pedido entrar/sair de 'Em Trânsito' conforme o
 *    rastreio do CT-e (atualizado pelo cron de rastreio do calculadora-st-service).
 *
 * Config por env:
 *  - PEDIDO_TRANSPORTE_CRON: expressão cron (default a cada 35 min, após o SSW).
 *  - PEDIDO_TRANSPORTE_CRON_DISABLED=true: desliga o disparo periódico.
 *  - PEDIDO_TRANSPORTE_LIMITE: máx. de pedidos por execução (default 300).
 */
@Injectable()
export class PedidoTransporteCron {
  private readonly logger = new Logger(PedidoTransporteCron.name);
  private rodando = false;

  constructor(
    private readonly vinculacao: VinculacaoNfeService,
    private readonly repo: VinculacaoNfeRepository,
  ) {}

  @Cron(process.env.PEDIDO_TRANSPORTE_CRON || '*/35 * * * *', { name: 'pedido-transporte' })
  async sync() {
    if (process.env.PEDIDO_TRANSPORTE_CRON_DISABLED === 'true') return;
    if (this.rodando) {
      this.logger.warn('Sincronização de transporte anterior ainda em execução; pulando.');
      return;
    }

    this.rodando = true;
    try {
      const limite = Number(process.env.PEDIDO_TRANSPORTE_LIMITE) || 300;
      const pedidos = await this.repo.findPedidosComVinculoAtivos(limite);
      let emTransito = 0;
      for (const pedidoId of pedidos) {
        try {
          const { status } = await this.vinculacao.sincronizarTransportePedido(pedidoId);
          if (status === 'Em Trânsito' || status === 'Em Trânsito parcialmente') emTransito++;
        } catch (e) {
          this.logger.error(
            `Falha ao sincronizar transporte do pedido ${pedidoId}: ${e instanceof Error ? e.message : String(e)}`,
          );
        }
      }
      this.logger.log(`Transporte de pedidos: ${pedidos.length} processados, ${emTransito} em trânsito.`);
    } catch (err) {
      this.logger.error(
        `Falha na sincronização de transporte: ${err instanceof Error ? err.message : String(err)}`,
      );
    } finally {
      this.rodando = false;
    }
  }
}
