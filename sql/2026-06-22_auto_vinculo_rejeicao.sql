-- Auto-vínculo: rejeição persistente + reversão de status.
--
-- 1) com_pedido.status_anterior: guarda o status do pedido antes de virar
--    'Vínculo sugerido', para reverter ao rejeitar a sugestão.
-- 2) com_pedido_nfe_vinculo.rejeitado: marca uma sugestão rejeitada (a linha é
--    mantida — não apagada — para que o auto-vínculo NÃO sugira a mesma NF p/
--    este pedido de novo).
--
-- Aplicar MANUALMENTE no Postgres da intranet (não rodar migration).
-- Depois rodar `npx prisma generate` no compras-service (já feito no código).

ALTER TABLE com_pedido
  ADD COLUMN IF NOT EXISTS status_anterior VARCHAR(50);

ALTER TABLE com_pedido_nfe_vinculo
  ADD COLUMN IF NOT EXISTS rejeitado BOOLEAN NOT NULL DEFAULT false;
