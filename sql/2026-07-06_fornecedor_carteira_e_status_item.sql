-- Feature: "Carteira" do fornecedor + status "Não Atendido pelo Fornecedor" por item.
--
-- Contexto:
--  * Fornecedor COM carteira (backorder): item não faturado fica pendente
--    indefinidamente (comportamento antigo, mantido).
--  * Fornecedor SEM carteira (padrão): após lançar a(s) NF, o comprador resolve
--    os itens que ficaram de fora — quem continua pendente e quem vira
--    "Não Atendido pelo Fornecedor" (status_item = 'nao_atendido'). Itens não
--    atendidos deixam de travar o status do pedido e deixam de marcar
--    "produto já em pedido" na nova cotação.
--
-- Aplicar MANUALMENTE no Postgres da intranet (NÃO rodar migration).
-- Depois de aplicar, rodar `npx prisma generate` no compras-service.

-- 1) Flag de carteira por fornecedor (uma linha por for_codigo).
ALTER TABLE com_fornecedor_parametros
  ADD COLUMN IF NOT EXISTS trabalha_carteira BOOLEAN NOT NULL DEFAULT false;

-- 2) Status manual do item no fechamento do pedido.
--    NULL = pendente/normal; 'nao_atendido' = Não Atendido pelo Fornecedor.
ALTER TABLE com_pedido_itens
  ADD COLUMN IF NOT EXISTS status_item     VARCHAR(30),
  ADD COLUMN IF NOT EXISTS status_item_por VARCHAR(120),
  ADD COLUMN IF NOT EXISTS status_item_em  TIMESTAMP;

-- Acelera o filtro "itens não atendidos" da checagem de nova cotação e do
-- recálculo de status (parcial, mas cobre o predicado usado).
CREATE INDEX IF NOT EXISTS idx_com_pedido_itens_status_item
  ON com_pedido_itens (status_item)
  WHERE status_item IS NOT NULL;
