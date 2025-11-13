/*
  Warnings:

  - You are about to drop the column `contagem_id` on the `est_contagem_itens` table. All the data in the column will be lost.
  - Added the required column `contagem_cuid` to the `est_contagem_itens` table without a default value. This is not possible if the table is not empty.

*/
-- DropForeignKey
ALTER TABLE "est_contagem_itens" DROP CONSTRAINT "est_contagem_itens_contagem_id_fkey";

-- DropIndex
DROP INDEX "est_contagem_itens_contagem_id_idx";

-- AlterTable
ALTER TABLE "est_contagem_itens" DROP COLUMN "contagem_id",
ADD COLUMN     "contagem_cuid" TEXT NOT NULL;

-- CreateIndex
CREATE INDEX "est_contagem_itens_contagem_cuid_idx" ON "est_contagem_itens"("contagem_cuid");
