import { Injectable, Logger, OnModuleInit, OnModuleDestroy } from '@nestjs/common';
import * as amqp from 'amqplib';
import { CotacaoRepository } from '../cotacao.repository';
import { Prisma } from '@prisma/client';
import { PrismaService } from 'src/prisma/prisma.service';

type Tx = PrismaService | Prisma.TransactionClient;

@Injectable()
export class RabbitMqConsumerService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(RabbitMqConsumerService.name);

  private connection: amqp.Connection | null = null;
  private channel: amqp.Channel | null = null;

  private readonly RABBITMQ_URL =
    'amqp://admin:admin@intranet-rabbitmq.naayqg.easypanel.host:5672';

  private readonly QUEUES = ['cotacao_offline_excluir'];

  constructor(
    private readonly comCotacaoService: CotacaoRepository, // injetado pelo CotacaoModule
  ) {}

  async onModuleInit() {
    await this.connectAndConsume();
  }

  async onModuleDestroy() {
    await this.close();
  }

  private async connectAndConsume() {
    try {
      this.logger.log('Conectando ao RabbitMQ...');
      this.connection = await amqp.connect(this.RABBITMQ_URL);
      this.channel = await this.connection.createChannel();

      for (const queue of this.QUEUES) {
        await this.channel.assertQueue(queue, { durable: true });

        this.logger.log(`Consumindo fila: ${queue}`);

        await this.channel.consume(
          queue,
          async (msg) => {
            if (!msg) return;

            const content = msg.content.toString();
            this.logger.log(`Mensagem recebida na fila ${queue}: ${content}`);

            try {
              await this.handleJob(queue, content);
              this.channel!.ack(msg);
            } catch (error: any) {
              this.logger.error(
                `Erro ao processar mensagem na fila ${queue}: ${error.message}`,
                error.stack,
              );
              this.channel!.nack(msg, false, false);
            }
          },
          { noAck: false },
        );
      }
    } catch (error: any) {
      this.logger.error('Erro conectando ao RabbitMQ', error.stack);
      setTimeout(() => this.connectAndConsume(), 5000);
    }
  }

  private async handleJob(queue: string, message: string) {
    const id = Number(message);

    if (Number.isNaN(id)) {
      this.logger.warn(`Mensagem inválida na fila ${queue}: ${message}`);
      return;
    }

    if (queue === 'cotacao_offline_excluir') {
      this.logger.log(`Excluindo registro com id ${id} da tabela com_pedido`);

      // Aqui não estamos usando transação, então passa undefined:
      await this.comCotacaoService.deleteCotacaoByPedido(id);
    }
  }

  private async close() {
    try {
      await this.channel?.close();
      await this.connection?.close();
      this.logger.log('Conexão RabbitMQ encerrada');
    } catch (error: any) {
      this.logger.error('Erro ao fechar conexão RabbitMQ', error.stack);
    }
  }
}
