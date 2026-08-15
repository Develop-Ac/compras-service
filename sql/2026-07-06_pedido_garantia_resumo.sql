-- Feature: resumo de GARANTIA por pedido (contas a receber IC/RET em aberto do
-- fornecedor). Persistido em com_pedido para a listagem/aviso; o detalhe (títulos
-- e produtos) é recomputado ao vivo na aba Garantia.
--
-- Preenchido ao gerar/salvar o pedido (recalcularGarantiaPedido) e pelo seed
-- scripts/seed-garantia-pedidos.ts.
--
-- Aplicar MANUALMENTE no Postgres da intranet (NÃO rodar migration).
-- Depois de aplicar, rodar `npx prisma generate` no compras-service.

ALTER TABLE com_pedido
  ADD COLUMN IF NOT EXISTS tem_garantia           BOOLEAN NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS garantia_qtd_titulos   INTEGER,
  ADD COLUMN IF NOT EXISTS garantia_qtd_produtos  INTEGER,
  ADD COLUMN IF NOT EXISTS garantia_valor         NUMERIC(15,2),
  ADD COLUMN IF NOT EXISTS garantia_verificada_em TIMESTAMP;

-- Acelera o filtro "pedidos com garantia" na listagem.
CREATE INDEX IF NOT EXISTS idx_com_pedido_tem_garantia
  ON com_pedido (tem_garantia)
  WHERE tem_garantia = true;
