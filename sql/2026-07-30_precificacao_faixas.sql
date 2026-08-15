-- Feature: régua de margem das três tabelas de preço, CONFIGURÁVEL EM DADO
-- (US-040 / T-024). Lida pelo motor de precificação a cada avaliação (cache
-- invalidado na escrita) — nunca constante em código, porque o PRD#escopo declara
-- que as faixas mudam com o tempo e a alteração não pode exigir deploy.
--
-- ---------------------------------------------------------------------------
-- MODELAGEM ESCOLHIDA: uma linha por (tabela_preco, faixa de custo)
-- ---------------------------------------------------------------------------
-- Alternativa DESCARTADA: uma linha por faixa com três colunas de markup
-- (markup_varejo/markup_atacado/markup_atacado_especial). Descartada porque
-- atacado (67%) e atacado especial (50%) têm UMA faixa cobrindo o domínio
-- inteiro, enquanto o varejo tem cinco: as três colunas obrigariam a repetir
-- 67/50 nas cinco linhas do varejo (invariante que nada no banco garante — mudar
-- o atacado passaria a ser 5 UPDATEs que precisam concordar entre si) e exigiriam
-- DDL nova no dia em que o atacado ganhar faixas próprias. Normalizado, alterar a
-- régua é INSERT, e o piso de cada tabela é lido da linha da própria tabela.
--
-- ---------------------------------------------------------------------------
-- VERSIONAMENTO: linha imutável com vigência (append-only) + autor
-- ---------------------------------------------------------------------------
-- Alterar uma faixa NÃO é UPDATE dos limites: fecha-se a linha vigente
-- (vigencia_fim / encerrado_por / encerrado_em) e insere-se a nova. A tabela É o
-- histórico — "quem alterou e quando" (DoD da US-040) sai de um SELECT nela mesma,
-- e o corredor vigente é `WHERE vigencia_fim IS NULL`.
-- Alternativa DESCARTADA: tabela `_historico` separada, alimentada por trigger ou
-- pela aplicação — duas fontes de verdade que podem divergir (basta um UPDATE sem
-- log e a auditoria mente), mais superfície de DDL, sem ganho de consulta.
--
-- ---------------------------------------------------------------------------
-- INTERVALO DE CUSTO: [custo_min, custo_max) — meio-aberto
-- ---------------------------------------------------------------------------
-- custo_min INCLUSIVO; custo_max EXCLUSIVO e igual ao custo_min da faixa
-- seguinte; NULL na última faixa = infinito.
-- Por que não intervalos fechados com passo de centavo (0–15,00 / 15,01–40,00,
-- como o PRD os declara): deixariam BURACO entre 15,00 e 15,01, e o custo composto
-- é divisão por quantidade — um custo de 15,004 ficaria SEM FAIXA, violando o AC3
-- ("nenhum custo sem faixa correspondente"). Meio-aberto, a cobertura é total por
-- construção e não há sobreposição.
-- Consequência declarada: 400,00 fica na faixa 99,01–400,00 (100–110%) e
-- **400,01 já cai na regra de "acima de 400,00"** (100–150%) — a mesma regra que
-- vale para um custo de R$ 500,00. A fração de centavo entre 400,00 e 400,01 cai
-- na faixa inferior.
--
-- ---------------------------------------------------------------------------
-- A NÃO-MONOTONICIDADE É INTENCIONAL
-- ---------------------------------------------------------------------------
-- A faixa 99,01–400,00 vai até 110% e a de acima de 400,00 volta a 150%.
-- Declaração explícita do responsável (PRD#escopo: "a não-monotonicidade é
-- intencional: reflete a variedade do mix"). Por isso NÃO existe CHECK comparando
-- faixas vizinhas — as constraints só olham dentro da própria linha. Quem for
-- "corrigir" a régua no futuro precisa mudar o PRD primeiro.
--
-- Semântica de markup: 100% = preço 2× custo, isto é
-- preco_minimo = custo × (1 + markup_min_pct / 100). Nunca preço = custo.
--
-- Precisão de markup_*_pct: NUMERIC(8,4), a MESMA de
-- com_fifo_completo.margem_pct (total 8, não 15 — medido na T-023). Teto de
-- 9999,9999%; a régua de hoje chega a 1000% na faixa 0–15, folga confortável.
--
-- Aplicar MANUALMENTE no Postgres da intranet (NÃO rodar migration).
-- Depois de aplicar, rodar `npx prisma generate` no compras-service.

CREATE TABLE IF NOT EXISTS com_precificacao_faixa (
  id              SERIAL PRIMARY KEY,
  -- 'varejo' | 'atacado_especial' | 'atacado' (vocabulário de negócio da US-038)
  tabela_preco    VARCHAR(20)    NOT NULL,
  custo_min       NUMERIC(15,2)  NOT NULL,
  -- exclusivo; NULL = sem limite superior de custo (última faixa)
  custo_max       NUMERIC(15,2),
  -- piso INVIOLÁVEL da faixa (percentual de markup sobre o custo)
  markup_min_pct  NUMERIC(8,4)   NOT NULL,
  -- teto da faixa; nas tabelas de markup único (atacado 67%, atacado especial 50%)
  -- é igual ao piso, porque o PRD declara um valor só, não um corredor
  markup_max_pct  NUMERIC(8,4)   NOT NULL,
  vigencia_inicio TIMESTAMP      NOT NULL,
  -- NULL = faixa vigente; preenchido quando a faixa é substituída
  vigencia_fim    TIMESTAMP,
  criado_por      VARCHAR(120)   NOT NULL,
  criado_em       TIMESTAMP      NOT NULL DEFAULT CURRENT_TIMESTAMP,
  encerrado_por   VARCHAR(120),
  encerrado_em    TIMESTAMP,
  motivo          TEXT,

  CONSTRAINT ck_com_precificacao_faixa_tabela
    CHECK (tabela_preco IN ('varejo', 'atacado_especial', 'atacado')),
  CONSTRAINT ck_com_precificacao_faixa_intervalo
    CHECK (custo_min >= 0 AND (custo_max IS NULL OR custo_max > custo_min)),
  -- min <= max vale DENTRO da linha; entre faixas não há comparação (ver acima)
  CONSTRAINT ck_com_precificacao_faixa_markup
    CHECK (markup_min_pct >= 0 AND markup_max_pct >= markup_min_pct)
);

-- Uma única faixa VIGENTE por (tabela, início do intervalo). Parcial de propósito:
-- as linhas encerradas (vigencia_fim NOT NULL) são o histórico e podem repetir a
-- combinação quantas vezes a régua for alterada.
-- NOTA: índice PARCIAL não é representável no schema.prisma — por isso o modelo
-- Prisma declara só o índice de histórico. A unicidade vive aqui.
CREATE UNIQUE INDEX IF NOT EXISTS uq_com_precificacao_faixa_vigente
  ON com_precificacao_faixa (tabela_preco, custo_min)
  WHERE vigencia_fim IS NULL;

-- Consulta de auditoria: "o que mudou nesta tabela de preço, em ordem".
CREATE INDEX IF NOT EXISTS idx_com_precificacao_faixa_historico
  ON com_precificacao_faixa (tabela_preco, vigencia_inicio);

-- ---------------------------------------------------------------------------
-- Régua vigente declarada no PRD#escopo (responsável, 2026-07-28).
-- Idempotente: só semeia se não existir NENHUMA faixa vigente.
-- ---------------------------------------------------------------------------
INSERT INTO com_precificacao_faixa
       (tabela_preco, custo_min, custo_max, markup_min_pct, markup_max_pct,
        vigencia_inicio, criado_por, motivo)
SELECT s.tabela_preco, s.custo_min, s.custo_max, s.markup_min_pct, s.markup_max_pct,
       CURRENT_TIMESTAMP,
       'responsavel (PRD#escopo, 2026-07-28)',
       'régua inicial da precificação das compras (US-040 / T-024)'
  FROM (VALUES
          -- varejo: markup por faixa de custo
          ('varejo',             0.00::numeric,  15.01::numeric,  200.0000::numeric, 1000.0000::numeric),
          ('varejo',            15.01::numeric,  40.01::numeric,  150.0000::numeric,  200.0000::numeric),
          ('varejo',            40.01::numeric,  99.01::numeric,  130.0000::numeric,  150.0000::numeric),
          ('varejo',            99.01::numeric, 400.01::numeric,  100.0000::numeric,  110.0000::numeric),
          -- acima de 400,00 — volta a 150%: NÃO é erro de digitação (ver cabeçalho)
          ('varejo',           400.01::numeric,   NULL::numeric,  100.0000::numeric,  150.0000::numeric),
          -- markup único, uma faixa para todo o domínio de custo
          ('atacado',            0.00::numeric,   NULL::numeric,   67.0000::numeric,   67.0000::numeric),
          ('atacado_especial',   0.00::numeric,   NULL::numeric,   50.0000::numeric,   50.0000::numeric)
       ) AS s(tabela_preco, custo_min, custo_max, markup_min_pct, markup_max_pct)
 WHERE NOT EXISTS (
         SELECT 1 FROM com_precificacao_faixa WHERE vigencia_fim IS NULL
       );
