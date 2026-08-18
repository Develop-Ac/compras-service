-- AlterTable
ALTER TABLE "com_pedido" ADD COLUMN     "tem_garantia" BOOLEAN NOT NULL DEFAULT false,
ADD COLUMN     "garantia_qtd_titulos" INTEGER,
ADD COLUMN     "garantia_qtd_produtos" INTEGER,
ADD COLUMN     "garantia_valor" DECIMAL(15,2),
ADD COLUMN     "garantia_verificada_em" TIMESTAMP(3);

-- AlterTable
ALTER TABLE "com_pedido_itens" ADD COLUMN     "status_item" VARCHAR(50),
ADD COLUMN     "status_item_por" VARCHAR(100),
ADD COLUMN     "status_item_em" TIMESTAMP(3);
