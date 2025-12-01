// src/rabbitmq/rabbitmq.module.ts
import { Module } from '@nestjs/common';
import { RabbitMqConsumerService } from './rabbitmq-consumer.service';
import { PrismaModule } from 'src/prisma/prisma.module';
import { CotacaoRepository } from '../cotacao.repository';

@Module({
  imports: [PrismaModule], // para o PedidoRepository ter o PrismaService
  providers: [
    RabbitMqConsumerService,
    CotacaoRepository,      // <<< AQUI é o ponto chave
  ],
})
export class RabbitMqModule {}
