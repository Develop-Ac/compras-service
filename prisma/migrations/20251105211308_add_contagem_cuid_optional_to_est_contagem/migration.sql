-- AlterTable
ALTER TABLE "est_contagem" ADD COLUMN     "contagem_cuid" TEXT;

-- CreateIndex
CREATE INDEX "est_contagem_contagem_cuid_idx" ON "est_contagem"("contagem_cuid");
