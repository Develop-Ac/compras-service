-- CreateTable
CREATE TABLE "est_contagem" (
    "id" TEXT NOT NULL,
    "colaborador" TEXT NOT NULL,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "est_contagem_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "est_contagem_itens" (
    "id" TEXT NOT NULL,
    "contagem_id" TEXT NOT NULL,
    "data" TIMESTAMP(3) NOT NULL,
    "cod_produto" INTEGER NOT NULL,
    "desc_produto" TEXT NOT NULL,
    "mar_descricao" TEXT,
    "ref_fabricante" TEXT,
    "ref_fornecedor" TEXT,
    "localizacao" TEXT,
    "unidade" TEXT,
    "qtde_saida" INTEGER NOT NULL,
    "estoque" INTEGER NOT NULL,
    "reserva" INTEGER NOT NULL,

    CONSTRAINT "est_contagem_itens_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE INDEX "est_contagem_colaborador_idx" ON "est_contagem"("colaborador");

-- CreateIndex
CREATE INDEX "est_contagem_itens_contagem_id_idx" ON "est_contagem_itens"("contagem_id");

-- CreateIndex
CREATE INDEX "est_contagem_itens_cod_produto_idx" ON "est_contagem_itens"("cod_produto");

-- AddForeignKey
ALTER TABLE "est_contagem" ADD CONSTRAINT "est_contagem_colaborador_fkey" FOREIGN KEY ("colaborador") REFERENCES "sis_usuarios"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "est_contagem_itens" ADD CONSTRAINT "est_contagem_itens_contagem_id_fkey" FOREIGN KEY ("contagem_id") REFERENCES "est_contagem"("id") ON DELETE CASCADE ON UPDATE CASCADE;
