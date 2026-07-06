/**
 * Backfill do resumo de GARANTIA nos pedidos ABERTOS.
 *
 * Varre com_pedido cujo status NÃO é 'Entregue', 'Entregue parcialmente' nem
 * 'Cancelado' e, para cada um, consulta a garantia do fornecedor (contas a
 * receber IC/RET em aberto) e grava o resumo em com_pedido (tem_garantia +
 * contagens + valor + data). Reaproveita a MESMA lógica do serviço
 * (GarantiaService.recalcularGarantiaPedido) via um contexto Nest mínimo
 * (só o GarantiaModule — sem crons/rabbit).
 *
 * Uso (a partir de compras-service/):
 *   npx ts-node -r tsconfig-paths/register scripts/seed-garantia-pedidos.ts          # dry-run
 *   npx ts-node -r tsconfig-paths/register scripts/seed-garantia-pedidos.ts --apply  # grava
 *
 * Pré-requisito: aplicar o DDL sql/2026-07-06_pedido_garantia_resumo.sql e rodar
 * `npx prisma generate` antes (as colunas de garantia precisam existir).
 */
import 'dotenv/config';
import { NestFactory } from '@nestjs/core';
import { PrismaClient } from '@prisma/client';
import { GarantiaModule } from '../src/compras/garantia/garantia.module';
import { GarantiaService } from '../src/compras/garantia/garantia.service';

const APPLY = process.argv.includes('--apply');
// --limit N: processa no máximo N pedidos (útil p/ teste). 0 = sem limite.
const LIMIT = (() => {
  const i = process.argv.indexOf('--limit');
  const n = i >= 0 ? Number(process.argv[i + 1]) : 0;
  return Number.isFinite(n) && n > 0 ? n : 0;
})();
// Status que NÃO devem ser reprocessados (pedido já fechado/cancelado).
const EXCLUIR = ['Entregue', 'Entregue parcialmente', 'Cancelado'];

async function main() {
  const prisma = new PrismaClient();
  const app = await NestFactory.createApplicationContext(GarantiaModule, {
    logger: ['error', 'warn'],
  });
  const garantia = app.get(GarantiaService);

  try {
    const pedidos = await prisma.com_pedido.findMany({
      where: { status: { notIn: EXCLUIR } },
      select: { id: true, pedido_cotacao: true, for_codigo: true, status: true },
      orderBy: { created_at: 'desc' },
      ...(LIMIT ? { take: LIMIT } : {}),
    });

    console.log(
      `${pedidos.length} pedido(s) a verificar (status ∉ ${EXCLUIR.join(' / ')}). ` +
        `Modo: ${APPLY ? 'APLICAR (grava)' : 'DRY-RUN (não grava)'}.`,
    );

    let processados = 0;
    let comGarantia = 0;
    let erros = 0;

    for (const p of pedidos) {
      try {
        // DRY: só consulta (não persiste). APPLY: consulta e grava o resumo.
        const resumo = APPLY
          ? await garantia.recalcularGarantiaPedido(p.id)
          : await garantia.garantiasDoPedido(p.id);
        processados++;
        if (resumo?.tem_garantia) {
          comGarantia++;
          console.log(
            `${APPLY ? 'GRAVADO ' : 'GARANTIA'} · cot ${p.pedido_cotacao} / for ${p.for_codigo} ` +
              `(${p.status}): ${resumo.totais.titulos} título(s), ${resumo.totais.produtos} produto(s), ` +
              `R$ ${resumo.totais.valor_total.toFixed(2)}`,
          );
        }
      } catch (err: any) {
        erros++;
        console.warn(`ERRO · pedido ${p.id}: ${err?.message || err}`);
      }
    }

    console.log(
      `\n${APPLY ? 'APLICADO' : 'DRY-RUN'}: ${processados}/${pedidos.length} processado(s), ` +
        `${comGarantia} com garantia, ${erros} erro(s).`,
    );
    if (!APPLY) console.log('Rode com --apply para gravar o resumo em com_pedido.');
  } finally {
    await app.close();
    await prisma.$disconnect();
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
