-- =====================================================================================
-- US-043 / T-030 — série histórica da taxa de exceção da triagem
--
-- Grão: UMA execução da triagem de UMA nota/pedido. Repetir a triagem do mesmo pedido
-- gera uma linha NOVA de propósito — a série é justamente a sucessão das medições, e
-- deduplicar por pedido apagaria a evolução que M5 precisa acompanhar. Por isso NÃO há
-- unique key de negócio aqui: a idempotência do projeto vale para importação e fila,
-- não para métrica de execução.
--
-- Dependência operacional: DDL MANUAL (CODING_STANDARDS#do). Este arquivo é entregue e
-- **NÃO foi aplicado**. Enquanto não for, a triagem continua funcionando por inteiro —
-- classificação e taxa saem na resposta — e apenas a série fica vazia, com o motivo
-- declarado em `TriagemNotaDto.registro.indisponivel_motivo`.
--
-- Depois de aplicar: rodar `npx prisma generate` (nunca `prisma migrate`).
-- =====================================================================================

CREATE TABLE IF NOT EXISTS com_precificacao_triagem_execucao (
  id                    SERIAL         PRIMARY KEY,
  pedido_id             VARCHAR(40)    NOT NULL,
  chave_nfe             VARCHAR(44)    NULL,
  for_codigo            INTEGER        NULL,
  itens                 INTEGER        NOT NULL,
  automaticos           INTEGER        NOT NULL,
  excecoes              INTEGER        NOT NULL,
  taxa_excecao_pct      NUMERIC(7, 4)  NOT NULL,
  meta_pct              NUMERIC(7, 4)  NOT NULL,
  dentro_da_meta        BOOLEAN        NOT NULL,
  excecoes_sem_proposta INTEGER        NOT NULL DEFAULT 0,
  -- Decomposição por motivo (ContagemMotivoDto[]): as chaves são as sete do conjunto
  -- fechado, garantidas pelo tipo no código. JSONB para a consulta de M5 poder agrupar
  -- por motivo sem uma segunda tabela de detalhe.
  por_motivo            JSONB          NOT NULL,
  triado_em             TIMESTAMP      NOT NULL,
  criado_em             TIMESTAMP      NOT NULL DEFAULT NOW()
);

-- A consulta da série é sempre "as últimas N execuções, mais recentes primeiro".
CREATE INDEX IF NOT EXISTS idx_com_precificacao_triagem_execucao_serie
  ON com_precificacao_triagem_execucao (triado_em DESC);

-- Acompanhamento por pedido (reprocessamentos da mesma nota).
CREATE INDEX IF NOT EXISTS idx_com_precificacao_triagem_execucao_pedido
  ON com_precificacao_triagem_execucao (pedido_id, triado_em DESC);

COMMENT ON TABLE com_precificacao_triagem_execucao IS
  'US-043/T-030: taxa de exceção por execução da triagem — insumo de M5 (alvo <= 33%).';
