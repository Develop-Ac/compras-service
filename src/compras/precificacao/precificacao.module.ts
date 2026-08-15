import { Module } from '@nestjs/common';
import { PrismaModule } from '../../prisma/prisma.module';
import { OpenQueryModule } from '../../shared/database/openquery/openquery.module';
import { CustoController } from './custo.controller';
import { CustoService } from './custo.service';
import { ElegibilidadeService } from './elegibilidade.service';
import { GiroController } from './giro.controller';
import { GiroRepository } from './giro.repository';
import { GiroService } from './giro.service';
import { PropostaController } from './proposta.controller';
import { PropostaRepository } from './proposta.repository';
import { PropostaService } from './proposta.service';
import { ReguaController } from './regua.controller';
import { ReguaRepository } from './regua.repository';
import { ReguaService } from './regua.service';
import { TabelasPrecoController } from './tabelas-preco.controller';
import { TabelasPrecoRepository } from './tabelas-preco.repository';
import { TabelasPrecoService } from './tabelas-preco.service';
import { TriagemController } from './triagem.controller';
import { TriagemRepository } from './triagem.repository';
import { TriagemService } from './triagem.service';

/**
 * Motor de precificação das compras (O6) — roda no compras-service por
 * ADR-0001. Fatias entregues:
 * - custo composto do item da NF conferida (US-037 / T-020);
 * - três tabelas de preço vigentes: varejo, atacado especial e atacado (US-038 / T-021);
 * - sinal de giro real/esperado do grupo de itens iguais, com a idade do dado
 *   (US-039 / T-022);
 * - régua de margem configurável em dado, com piso inviolável (US-040 / T-024) —
 *   faixas versionadas em `com_precificacao_faixa` (DDL manual em
 *   `sql/2026-07-30_precificacao_faixas.sql`), lidas a cada avaliação: mudar uma
 *   faixa não exige rebuild nem restart.
 * - travas de elegibilidade do item (as duas de custo) e classificação de performance
 *   pela idade do saldo (US-041 / T-025) — `ElegibilidadeService`, política **pura**
 *   (sem repositório: nenhuma DDL nova, nenhuma leitura nova; recebe custo, estoque,
 *   giro e `chaves_nfe` já lidos e devolve a decisão). Insumo da T-026, que propõe o
 *   preço; exportado sem controller próprio de propósito — a decisão só faz sentido
 *   dentro da proposta.
 * - proposta de preço ancorada no markup anterior, com fallback pela faixa (US-041 / T-026) —
 *   `PropostaService` orquestra as cinco fatias acima e devolve **uma proposta por tabela**
 *   para cada item da nota, com o motivo em uma linha para o extrato de decisão (EP-014).
 *   Nenhuma DDL nova: consome o que as fatias anteriores já leem. O piso é validado pela
 *   trava da régua (`ReguaService.avaliarPreco`), nunca reimplementado.
 * - triagem da nota entre automáticos e exceções (US-043 / T-030) — `TriagemService`
 *   consome a proposta **pelo contrato** (`PropostaNotaDto`) e classifica cada item, com
 *   motivo do conjunto **fechado de sete**, fechado no sistema de tipos. Fatia própria, não
 *   extensão da proposta: aquela responde "quanto?", esta responde "quem revisa?".
 *   Única DDL nova do módulo desde a régua — `sql/2026-07-30_precificacao_triagem_execucao.sql`,
 *   **entregue e não aplicada** —, e de propósito **sem** dependência dura: a escrita da
 *   série é best-effort, então a triagem e a taxa funcionam antes de a DDL ser aplicada.
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
  controllers: [
    CustoController,
    TabelasPrecoController,
    GiroController,
    ReguaController,
    PropostaController,
    TriagemController,
  ],
  providers: [
    CustoService,
    TabelasPrecoRepository,
    TabelasPrecoService,
    GiroRepository,
    GiroService,
    ReguaRepository,
    ReguaService,
    ElegibilidadeService,
    PropostaRepository,
    PropostaService,
    TriagemRepository,
    TriagemService,
  ],
  exports: [
    CustoService,
    TabelasPrecoService,
    GiroService,
    ReguaService,
    ElegibilidadeService,
    PropostaService,
    TriagemService,
  ],
})
export class PrecificacaoModule {}
