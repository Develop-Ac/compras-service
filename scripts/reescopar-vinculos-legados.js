/**
 * Correção pontual de vínculos NF-e × Pedido gravados ANTES do filtro por
 * fornecedor (escopo do pedido). Reaplica, item a item, o mesmo "reescopo" que o
 * código novo faz em confirmarVinculo:
 *
 *   1) itens tipo='vinculado' cujo pro_codigo NÃO pertence ao com_pedido_itens do
 *      pedido (fornecedor) deste vínculo voltam para tipo='xml_sem_vinculo'
 *      (liberando o saldo da NF que estavam consumindo indevidamente);
 *   2) os itens tipo='pedido_sem_vinculo' são reconstruídos a partir dos itens
 *      reais do pedido ainda não vinculados.
 *
 * É IDEMPOTENTE: roda quantas vezes quiser; só mexe no que está fora de escopo.
 * NÃO altera status de pedido (um produto que não é do pedido nunca contou para a
 * cobertura, então a confirmação/o status permanecem corretos).
 *
 * Uso:
 *   node scripts/reescopar-vinculos-legados.js --dry     # só relata, não grava
 *   node scripts/reescopar-vinculos-legados.js --apply   # aplica as correções
 */
const { PrismaClient } = require('@prisma/client');
const prisma = new PrismaClient();

const APPLY = process.argv.includes('--apply');

async function reescopar(vinculoId, pedidoId) {
  const reais = await prisma.com_pedido_itens.findMany({
    where: { pedido_id: pedidoId },
    select: { pro_codigo: true, pro_descricao: true, quantidade: true, valor_unitario: true },
  });
  const realPorCod = new Map();
  for (const r of reais) if (!realPorCod.has(r.pro_codigo)) realPorCod.set(r.pro_codigo, r);

  const itens = await prisma.com_pedido_nfe_vinculo_item.findMany({
    where: { vinculo_id: vinculoId },
    select: { id: true, tipo: true, pro_codigo: true },
  });

  const foraDoPedido = [];
  const codigosVinculadosNoPedido = new Set();
  for (const it of itens) {
    if (it.tipo !== 'vinculado') continue;
    if (it.pro_codigo != null && realPorCod.has(Number(it.pro_codigo))) {
      codigosVinculadosNoPedido.add(Number(it.pro_codigo));
    } else {
      foraDoPedido.push(it.id);
    }
  }

  const novosPedidoSem = [...realPorCod.values()]
    .filter((r) => !codigosVinculadosNoPedido.has(r.pro_codigo))
    .map((r) => ({
      vinculo_id: vinculoId,
      tipo: 'pedido_sem_vinculo',
      pro_codigo: r.pro_codigo,
      pro_descricao: r.pro_descricao,
      quantidade_pedido: r.quantidade ?? null,
      valor_pedido: r.valor_unitario ?? null,
    }));

  if (!foraDoPedido.length) return { tocou: false, removidos: 0 };

  if (APPLY) {
    await prisma.$transaction(async (tx) => {
      await tx.com_pedido_nfe_vinculo_item.updateMany({
        where: { id: { in: foraDoPedido } },
        data: {
          tipo: 'xml_sem_vinculo',
          pro_codigo: null,
          pro_descricao: null,
          quantidade_pedido: null,
          valor_pedido: null,
          quantidade_alocada: null,
          excede_saldo: false,
          match_campo: 'Fora do pedido (reescopo)',
          match_valor: null,
        },
      });
      await tx.com_pedido_nfe_vinculo_item.deleteMany({
        where: { vinculo_id: vinculoId, tipo: 'pedido_sem_vinculo' },
      });
      if (novosPedidoSem.length) {
        await tx.com_pedido_nfe_vinculo_item.createMany({ data: novosPedidoSem });
      }
    });
  }
  return { tocou: true, removidos: foraDoPedido.length };
}

(async () => {
  const vincs = await prisma.com_pedido_nfe_vinculo.findMany({
    select: { id: true, pedido_id: true, pedido_cotacao: true, for_codigo: true, confirmado: true },
  });
  let afetados = 0, removidos = 0;
  for (const v of vincs) {
    const r = await reescopar(v.id, v.pedido_id);
    if (r.tocou) {
      afetados++;
      removidos += r.removidos;
      console.log(
        `${APPLY ? 'CORRIGIDO' : 'A CORRIGIR'} vínculo ${v.id} (cotação ${v.pedido_cotacao}, for ${v.for_codigo}, confirmado=${v.confirmado}) — ${r.removidos} item(ns) fora do pedido`,
      );
    }
  }
  console.log(
    `\n${APPLY ? 'Aplicado' : 'Simulação (dry-run)'}: ${afetados} vínculo(s) afetado(s), ${removidos} item(ns) reescopado(s).`,
  );
  if (!APPLY) console.log('Rode com --apply para gravar.');
})()
  .catch((e) => { console.error(e); process.exit(1); })
  .finally(() => prisma.$disconnect());
