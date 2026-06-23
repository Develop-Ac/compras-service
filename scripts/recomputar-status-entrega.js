/**
 * Recalcula o status de entrega/faturamento dos pedidos aplicando a NOVA regra
 * (com 'Entregue parcialmente'), corrigindo pedidos gravados pela lógica antiga.
 *
 * Regra por pro_codigo do pedido (vínculos confirmados x conciliação ERP):
 *  - "faturado" = coberto por ≥1 NF confirmada; "entregue" = coberto e todas as
 *    NFs que o cobrem estão LANCADA.
 *  - todos entregues               -> 'Entregue'
 *  - algum entregue, nem todos      -> 'Entregue parcialmente'
 *  - nenhum lançado, todos cobertos -> 'Faturado'
 *  - nenhum lançado, parte coberta  -> 'Faturado parcialmente'
 *
 * Diferente do serviço, ESTE script PODE rebaixar 'Entregue' -> 'Entregue
 * parcialmente' (corrige pedidos marcados 'Entregue' indevidamente pelo nfLancada
 * antigo, que forçava 'Entregue' mesmo sem cobrir todos os itens). Nunca toca em
 * 'Cancelado'. Ajusta data_recebimento (mantém só quando 'Entregue').
 *
 * Uso:  node scripts/recomputar-status-entrega.js --dry
 *       node scripts/recomputar-status-entrega.js --apply
 */
const { PrismaClient } = require('@prisma/client');
const p = new PrismaClient();
const APPLY = process.argv.includes('--apply');

async function novoStatus(pedidoId) {
  const codes = new Set(
    (await p.com_pedido_itens.findMany({ where: { pedido_id: pedidoId }, select: { pro_codigo: true } }))
      .map((r) => Number(r.pro_codigo)),
  );
  if (!codes.size) return null;
  const itens = await p.com_pedido_nfe_vinculo_item.findMany({
    where: { tipo: 'vinculado', pro_codigo: { not: null }, vinculo: { pedido_id: pedidoId, confirmado: true } },
    select: { pro_codigo: true, vinculo: { select: { chave_nfe: true } } },
  });
  const codeToCh = new Map();
  for (const it of itens) {
    const c = Number(it.pro_codigo);
    if (!codes.has(c)) continue;
    const ch = it.vinculo?.chave_nfe;
    if (!ch) continue;
    if (!codeToCh.has(c)) codeToCh.set(c, new Set());
    codeToCh.get(c).add(ch);
  }
  if (!codeToCh.size) return null; // nenhum item contemplado: não mexe
  const chaves = [...new Set([...codeToCh.values()].flatMap((s) => [...s]))];
  const concs = await p.com_nfe_conciliacao.findMany({ where: { chave_nfe: { in: chaves } }, select: { chave_nfe: true, status_erp: true, dt_entrada: true } });
  const porChave = new Map(concs.map((c) => [c.chave_nfe, c]));
  const lancada = (ch) => porChave.get(ch)?.status_erp === 'LANCADA';

  let todosE = true, algumE = false, todosF = true;
  for (const c of codes) {
    const chs = codeToCh.get(c);
    if (!chs || !chs.size) { todosF = false; todosE = false; continue; }
    const arr = [...chs];
    if (!arr.every(lancada)) todosE = false;
    if (arr.some(lancada)) algumE = true;
  }
  if (todosE) {
    const datas = chaves.filter(lancada).map((c) => porChave.get(c)?.dt_entrada).filter((d) => d instanceof Date);
    const data = datas.length ? new Date(Math.max(...datas.map((d) => d.getTime()))) : new Date();
    return { status: 'Entregue', data_recebimento: data };
  }
  if (algumE) return { status: 'Entregue parcialmente', data_recebimento: null };
  if (todosF) return { status: 'Faturado', data_recebimento: null };
  return { status: 'Faturado parcialmente', data_recebimento: null };
}

(async () => {
  const peds = await p.com_pedido.findMany({
    where: { status: { in: ['Faturado', 'Faturado parcialmente', 'Entregue', 'Entregue parcialmente'] } },
    select: { id: true, pedido_cotacao: true, for_codigo: true, status: true },
  });
  let mudancas = 0;
  for (const ped of peds) {
    const novo = await novoStatus(ped.id);
    if (!novo || novo.status === ped.status) continue;
    mudancas++;
    console.log(`${APPLY ? 'CORRIGIDO' : 'A CORRIGIR'} cot ${ped.pedido_cotacao}/for ${ped.for_codigo}: '${ped.status}' -> '${novo.status}'`);
    if (APPLY) {
      await p.com_pedido.update({ where: { id: ped.id }, data: { status: novo.status, data_recebimento: novo.data_recebimento } });
    }
  }
  console.log(`\n${APPLY ? 'Aplicado' : 'Simulação (dry-run)'}: ${mudancas} pedido(s) alterado(s).`);
  if (!APPLY) console.log('Rode com --apply para gravar.');
})().catch((e) => { console.error(e); process.exit(1); }).finally(() => p.$disconnect());
