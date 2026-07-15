/**
 * Backfill do resumo de GARANTIA nos pedidos ABERTOS.
 *
 * Varre com_pedido cujo status NÃO é 'Entregue', 'Entregue parcialmente' nem
 * 'Cancelado' e, para cada um, consulta a garantia do fornecedor (contas a
 * receber IC/RET em aberto) e grava o resumo em com_pedido (tem_garantia +
 * contagens + valor + data). Reaproveita a MESMA lógica do serviço
 * (GarantiaService.recalcularGarantiaPedido) carregando o módulo COMPILADO do
 * dist/ num contexto Nest mínimo (só o GarantiaModule — sem crons/rabbit).
 *
 * Roda com Node puro (NÃO usa ts-node) — igual aos demais scripts do projeto.
 * Pré-requisitos:
 *   1) `npm run build` (precisa existir dist/compras/garantia/*.js).
 *   2) DDL sql/2026-07-06_pedido_garantia_resumo.sql aplicado + `npx prisma generate`
 *      (necessário só para o modo --apply, que grava as colunas novas).
 *
 * Uso (a partir de compras-service/):
 *   node scripts/seed-garantia-pedidos.js                 # dry-run (não grava)
 *   node scripts/seed-garantia-pedidos.js --apply         # grava o resumo
 *   node scripts/seed-garantia-pedidos.js --limit 20      # limita a 20 pedidos
 */
try { require('dotenv').config(); } catch (_) { /* dotenv opcional (em prod as envs já vêm do container) */ }

const { NestFactory } = require('@nestjs/core');
const { PrismaClient } = require('@prisma/client');
const { GarantiaModule } = require('../dist/compras/garantia/garantia.module');
const { GarantiaService } = require('../dist/compras/garantia/garantia.service');

const APPLY = process.argv.includes('--apply');
const LIMIT = (() => {
  const i = process.argv.indexOf('--limit');
  const n = i >= 0 ? Number(process.argv[i + 1]) : 0;
  return Number.isFinite(n) && n > 0 ? n : 0;
})();
// Status que NÃO devem ser reprocessados (pedido já fechado/cancelado).
const EXCLUIR = ['Entregue', 'Entregue parcialmente', 'Cancelado'];

(async () => {
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
        `Modo: ${APPLY ? 'APLICAR (grava)' : 'DRY-RUN (nao grava)'}.`,
    );

    let processados = 0;
    let comGarantia = 0;
    let erros = 0;

    for (const p of pedidos) {
      try {
        // DRY: so consulta (nao persiste). APPLY: consulta e grava o resumo.
        const resumo = APPLY
          ? await garantia.recalcularGarantiaPedido(p.id)
          : await garantia.garantiasDoPedido(p.id);
        processados++;
        if (resumo && resumo.tem_garantia) {
          comGarantia++;
          console.log(
            `${APPLY ? 'GRAVADO ' : 'GARANTIA'} · cot ${p.pedido_cotacao} / for ${p.for_codigo} ` +
              `(${p.status}): ${resumo.totais.titulos} titulo(s), ${resumo.totais.produtos} produto(s), ` +
              `R$ ${Number(resumo.totais.valor_total || 0).toFixed(2)}`,
          );
        }
      } catch (err) {
        erros++;
        console.warn(`ERRO · pedido ${p.id}: ${(err && err.message) || err}`);
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
})().catch((e) => {
  console.error(e);
  process.exit(1);
});
