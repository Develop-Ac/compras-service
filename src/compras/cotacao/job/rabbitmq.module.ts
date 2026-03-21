import { Module } from '@nestjs/common';
import { RabbitMqConsumerService } from './rabbitmq-consumer.service';
import { PrismaModule } from 'src/prisma/prisma.module';
import { CotacaoRepository } from '../cotacao.repository';
import { OpenQueryModule } from 'src/shared/database/openquery/openquery.module'; // adicionado

@Module({
  imports: [
    PrismaModule,
    OpenQueryModule, // adicionado
  ],
  providers: [
    RabbitMqConsumerService,
    CotacaoRepository,
  ],
})
export class RabbitMqModule {}