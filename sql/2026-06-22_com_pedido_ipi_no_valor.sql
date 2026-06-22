-- Feature: "IPI no valor unitário" na Conferência Pedido × Faturado.
-- Flag por pedido: quando true, a conferência soma o IPI por unidade da NF ao
-- valor faturado antes de comparar com o valor do pedido (que já inclui IPI).
--
-- Aplicar MANUALMENTE no Postgres da intranet (não rodar migration).
-- Após aplicar, rodar `npx prisma generate` no compras-service (já feito no código).

ALTER TABLE com_pedido
  ADD COLUMN IF NOT EXISTS ipi_no_valor BOOLEAN NOT NULL DEFAULT false;
