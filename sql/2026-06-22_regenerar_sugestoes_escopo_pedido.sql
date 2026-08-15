-- OPCIONAL — rodar UMA vez, DEPOIS de subir a correção de escopo do vincular().
--
-- Sugestões automáticas criadas ANTES da correção somavam os itens de TODOS os
-- pedidos da cotação (ex.: pedido com 5 itens aparecia com 76 / 69 sem vínculo).
-- Este script apaga essas sugestões não confirmadas para o cron recriá-las
-- corretas (escopadas ao fornecedor do pedido).
--
-- Só mexe em sugestões AUTOMÁTICAS, NÃO confirmadas e NÃO rejeitadas.
-- NÃO toca em vínculos confirmados nem em rejeições.

-- 1) Reverte ao status elegível os pedidos cuja sugestão será apagada (para o cron
--    poder reprocessá-los). status_anterior é NULL p/ sugestões antigas -> cai em
--    'Aguardando analise' (a grande maioria já era esse status).
UPDATE com_pedido p
SET status = COALESCE(p.status_anterior, 'Aguardando analise'),
    status_anterior = NULL
WHERE p.status = 'Vínculo sugerido'
  AND EXISTS (
    SELECT 1 FROM com_pedido_nfe_vinculo v
    WHERE v.pedido_id = p.id
      AND v.confirmado = false
      AND v.origem_vinculo = 'auto'
      AND v.rejeitado = false
  );

-- 2) Apaga as sugestões automáticas não confirmadas / não rejeitadas.
DELETE FROM com_pedido_nfe_vinculo
WHERE confirmado = false
  AND origem_vinculo = 'auto'
  AND rejeitado = false;
