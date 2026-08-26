-- com_pedido_intranet_celta.pedido_intranet passa a guardar o com_pedido.id (cuid),
-- por isso a coluna deixa de ser integer e vira text.
ALTER TABLE "public"."com_pedido_intranet_celta"
  ALTER COLUMN "pedido_intranet" TYPE text USING "pedido_intranet"::text;
