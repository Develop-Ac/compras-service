-- CreateTable
CREATE TABLE "est_contagem_log" (
    "id" TEXT NOT NULL,
    "contagem_id" TEXT NOT NULL,
    "usuario_id" TEXT NOT NULL,
    "item_id" TEXT NOT NULL,
    "estoque" INTEGER NOT NULL,
    "contado" INTEGER NOT NULL,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "est_contagem_log_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE INDEX "est_contagem_log_contagem_id_idx" ON "est_contagem_log"("contagem_id");

-- CreateIndex
CREATE INDEX "est_contagem_log_usuario_id_idx" ON "est_contagem_log"("usuario_id");

-- CreateIndex
CREATE INDEX "est_contagem_log_item_id_idx" ON "est_contagem_log"("item_id");

-- AddForeignKey
ALTER TABLE "est_contagem_log" ADD CONSTRAINT "est_contagem_log_contagem_id_fkey" FOREIGN KEY ("contagem_id") REFERENCES "est_contagem"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "est_contagem_log" ADD CONSTRAINT "est_contagem_log_usuario_id_fkey" FOREIGN KEY ("usuario_id") REFERENCES "sis_usuarios"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "est_contagem_log" ADD CONSTRAINT "est_contagem_log_item_id_fkey" FOREIGN KEY ("item_id") REFERENCES "est_contagem_itens"("id") ON DELETE RESTRICT ON UPDATE CASCADE;
