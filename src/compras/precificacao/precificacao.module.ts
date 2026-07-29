import { Module } from '@nestjs/common';
import { PrismaModule } from '../../prisma/prisma.module';
import { OpenQueryModule } from '../../shared/database/openquery/openquery.module';
import { CustoController } from './custo.controller';
import { CustoService } from './custo.service';
import { GiroController } from './giro.controller';
import { GiroRepository } from './giro.repository';
import { GiroService } from './giro.service';
import { TabelasPrecoController } from './tabelas-preco.controller';
import { TabelasPrecoRepository } from './tabelas-preco.repository';
import { TabelasPrecoService } from './tabelas-preco.service';

/**
 * Motor de precificação das compras (O6) — roda no compras-service por
 * ADR-0001. Fatias entregues:
 * - custo composto do item da NF conferida (US-037 / T-020);
 * - três tabelas de preço vigentes: varejo, atacado especial e atacado (US-038 / T-021);
 * - sinal de giro real/esperado do grupo de itens iguais, com a idade do dado
 *   (US-039 / T-022).
 *
 * `PrismaModule` cobre tudo que já está espelhado no Postgres `intranet` — inclusive
 * `com_fifo_completo` e `com_relacionamento_itens`, lidas pelo sinal de giro **somente
 * leitura** (dono de escrita: ETL do `analise-estoque-service`).
 * `OpenQueryModule` entra por causa do **atacado (`pro.preco5`)**, que o ETL do
 * `analise-estoque-service` não espelha em `com_fifo_completo` e por isso é lido
 * direto do ERP (OPENQUERY `[CONSULTA]`, **somente leitura** — nada é escrito nem
 * no ERP nem no espelho).
 */
@Module({
  imports: [PrismaModule, OpenQueryModule],
  controllers: [CustoController, TabelasPrecoController, GiroController],
  providers: [
    CustoService,
    TabelasPrecoRepository,
    TabelasPrecoService,
    GiroRepository,
    GiroService,
  ],
  exports: [CustoService, TabelasPrecoService, GiroService],
})
export class PrecificacaoModule {}
