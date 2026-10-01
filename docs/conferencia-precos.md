# Conferência de preços — crivo do gerente depois da precificação

> Backend: `compras-service`, módulo novo `src/compras/conferencia-precos` (nome diferente de
> `precificacao`, que existe na branch `teste` e será mergeado depois — os dois coexistem).
> Leitura do ERP: `erp-firebird-api`. Frontend: `cotacao-frontend`,
> `app/(private)/compras/precificacao/conferencia`, módulo Compras, permissão
> `/compras/precificacao/conferencia`.
>
> Origem: planilha "Confere Precificação.xlsx" (Power Query no Firebird, rodada à mão pelo
> gerente a cada NF). Levantamento com o responsável em 01/10/2026.

## Para que serve

O comprador precifica no ERP ao dar entrada na NF. Esta tela é o **crivo do gerente** depois
disso: lista as NFs de compra que entraram no estoque e, item a item, mostra se os três preços
de venda respeitam o piso da régua. O gerente corrige no ERP, atualiza a tela e, quando está
satisfeito, **confirma** a NF. A confirmação grava uma fotografia do que ele viu.

É o inverso do motor de precificação da branch `teste` (que *propõe* preço antes). Os dois
fazem parte do mesmo fluxo e vão coexistir; esta tela não depende dele.

## O que entra na fila

| Critério | Valor | Fonte |
|---|---|---|
| Empresa | 3 | `NF_ENTRADA.EMPRESA` |
| Status | 1 (lançada), sem `DT_CANCELAMENTO` | `NF_ENTRADA.STATUS` |
| Modelo | 55 (NF-e; CT-e fica fora) | `NF_ENTRADA.MODELO_NOTA` |
| Operação | OPF com destinação `COMERCIALIZACAO` e `ativo` | `com_fiscal_opf_destinacao` (Postgres intranet, editada em `/compras/notaFiscal/regras-fiscais`). Hoje: 1, 20, 40 |
| Data | `DT_ENTRADA >= CONFERENCIA_PRECO_DESDE` (data do deploy) | env |

Entra por **varredura** (cron, env `CONFERENCIA_PRECO_CRON`, padrão a cada 5 min), não pelo
aviso do fiscal-service: a varredura pega também NF lançada à mão no ERP. NF que deixa de
atender (status mudou, cancelada) sai da fila na varredura seguinte (`status = 'removida'`).
NF já confirmada nunca muda.

## O que a tela mostra por item

Leitura **ao vivo** do ERP a cada abertura do detalhe e no botão **Atualizar**. Nada é escrito
no Firebird; a correção de preço é no ERP.

| Coluna | Fonte |
|---|---|
| Produto, descrição, quantidade da NF | `NFE_ITENS` + `PRODUTOS` |
| Custo da NF | `PRODUTOS.PRECO_CUSTO` |
| Custo médio | `PRODUTOS.CUSTO_MEDIO` |
| Saldo anterior | `ESTOQUE_DISPONIVEL − Σ(movimentos do kardex depois da entrada desta NF) − QUANTIDADE` (mesma conta da planilha; ver "Saldo anterior") |
| Preços atuais | `PRECO_VENDA` (varejo), `PRECO2` (atacado especial), `PRECO5` (atacado) |
| Preços anteriores | `PRECO_ANT1`, `PRECO_ANT2`, `PRECO_ANT5` |

### Custo base

```
saldo_anterior > 0  → custo_base = CUSTO_MEDIO   (origem 'medio')
saldo_anterior = 0  → custo_base = PRECO_CUSTO   (origem 'nf')
custo_base nulo ou zero → origem 'ausente': sem margem, sem piso, item cinza
```

Nulo **nunca** vira zero. Margem nunca é calculada sobre zero.

### Régua (só piso, sem teto)

Fonte: `com_precificacao_faixa` com `vigencia_fim IS NULL`. Usa **só `markup_min_pct`**;
`markup_max_pct` é ignorado (o responsável não quer tetos agora; a coluna fica para o motor).

| Tabela | Regra vigente |
|---|---|
| Atacado especial (`PRECO2`) | custo_base × 1,538 |
| Atacado (`PRECO5`) | custo_base × 1,67 |
| Varejo (`PRECO_VENDA`) | por faixa de custo_base: 0–15 × 3,00 · 15–40 × 2,50 · 40–99 × 2,30 · 99+ × 2,00 |

A faixa do atacado especial precisa subir de 50% para 53,8%: SQL em
`sql/2026-10-01_conferencia_precos.sql` (encerra a faixa 7 e cria a nova; o índice único
`uq_com_precificacao_faixa_vigente` exige encerrar antes de inserir).

### Piso e semáforo

```
piso = arredonda_para_cima_90( custo_base × (1 + markup_min_pct/100) )

arredonda_para_cima_90(x): menor valor terminado em ,90 que seja >= x
  572,04 → 572,90      572,95 → 573,90      572,90 → 572,90
```

Por tabela:

| Semáforo | Quando |
|---|---|
| `abaixo` (vermelho) | preço < piso. **Sem tolerância.** |
| `reajuste` (amarelo) | preço ≥ piso e preço anterior > 0 e preço / anterior − 1 > 15% |
| `ok` | o resto |
| `sem_custo` (cinza) | custo_base ausente |
| preço anterior = 0 | mostrado como "sem histórico"; nunca é queda de 100% |

Semáforo do item = o pior entre as três tabelas (`abaixo` > `sem_custo` > `reajuste` > `ok`).
A fila mostra, por NF, quantos itens estão `abaixo` e quantos `sem_custo` (contagem gravada
na varredura e refeita a cada abertura do detalhe).

## Papéis

| Quem | Permissão em `sis_permissoes` (tela `/compras/precificacao/conferencia`) | Pode |
|---|---|---|
| Compras | `visualizar` | abrir fila e detalhe, atualizar, ver semáforos (conferir o próprio trabalho) |
| Gerente | `editar` | tudo acima + **Confirmar** |

Backend recebe o usuário por `x-user-id` (padrão dos outros serviços) e grava
`conferido_por`; o frontend só mostra o botão com `editar`.

## Confirmação

Definitiva, uma por NF. Não existe "reabrir".

1. Backend relê o ERP no momento da confirmação (não confia no que o navegador mostrou).
2. Todo item `abaixo` ou `sem_custo` exige `justificativa` (texto, por item). Faltou uma →
   400 com a lista dos `pro_codigo` sem justificativa.
3. Grava `com_precificacao_conferencia` (`status = 'conferida'`, `conferido_por`,
   `conferido_em`, `observacao` opcional) e uma linha por item em
   `com_precificacao_conferencia_item` com a fotografia: custo base e origem, saldo anterior,
   3 preços, 3 anteriores, 3 pisos, 3 semáforos, semáforo do item, justificativa.

## Saldo anterior (a conta da planilha, sem SQL escrito à mão)

A planilha faz subselect correlacionado em `LANCTOS_ESTOQUE`. Na API vira duas leituras pelo
montador e a conta em JS, dentro de uma **consulta nomeada** do domínio `entrada-fiscal`:

1. `LANCTOS_ESTOQUE` com `EMPRESA = 3`, `NFE = :nfe`, `OPERACAO = 'E'` → `max(LANCTO)` por
   `PRO_CODIGO` (o lançamento de entrada desta NF).
2. `LANCTOS_ESTOQUE` com `EMPRESA = 3`, `PRO_CODIGO IN (itens)`, `LANCTO > min(dos máximos)`
   → em JS, por produto, soma `+QUANTIDADE` se `OPERACAO = 'E'`, `−QUANTIDADE` se `'S'`,
   só dos lançamentos com `LANCTO >` o máximo daquele produto.
3. `saldo_anterior = ESTOQUE_DISPONIVEL − soma − QUANTIDADE_da_NF`.

Reproduz a planilha (que considera `OPERACAO`, não `ORIGEM`); a armadilha do catálogo sobre
"soma de saídas" fala do relatório legado e não se aplica aqui.

## API

### erp-firebird-api

- `src/erp/produtos/tabelas/produtos.tabela.ts`: expor `PRECO_ANT1`, `PRECO_ANT2`, `PRECO_ANT5`
  (preço anterior das tabelas 1/varejo, 2 e 5) e incluir na lista do repository.
- Consulta nomeada `GET /erp/nf-entrada/:nfe/precos` (empresa obrigatória, só 3 por ora):
  cabeçalho da NF + itens com os campos da tabela acima e `saldo_anterior`. Declarar
  `consumidores: [{ servico: 'compras-service', porque: 'conferência de preços', destino: 'Postgres com_precificacao_conferencia_item' }]`.

### compras-service (`/compras/conferencia-precos`)

| Rota | O que faz |
|---|---|
| `GET /` `?status=pendente\|conferida\|removida&desde&ate` | fila: NF, fornecedor, data de entrada, total, contagens, conferido por/em |
| `GET /:id` | detalhe ao vivo: cabeçalho + itens com custo base, pisos e semáforos |
| `POST /:id/confirmar` `{ observacao?, justificativas: [{ pro_codigo, texto }] }` | confirmação (regras acima) |
| `POST /varredura` | dispara a varredura à mão (mesmo método do cron) |

A régua é lida de `com_precificacao_faixa` pelo próprio módulo (uma query, cache de 1 min);
não importa nada da branch `teste`.

## Tabelas (DDL manual: `sql/2026-10-01_conferencia_precos.sql`)

- `com_precificacao_conferencia`: uma linha por NF da fila. Chave natural `(empresa, nfe)`.
- `com_precificacao_conferencia_item`: fotografia por item, só existe depois da confirmação.

Depois de aplicar: `npx prisma db pull` ou acrescentar os dois models no `schema.prisma` e
`npx prisma generate`.

## Frontend

- `/compras/precificacao/conferencia`: fila. Filtros status (padrão pendente) e período.
  Colunas: NF, fornecedor, entrada, total, itens, abaixo do piso, sem custo, conferido por/em.
- `/compras/precificacao/conferencia/[id]`: detalhe. Botão **Atualizar** (relê o ERP).
  Linha por item com as colunas da seção "O que a tela mostra"; piso e semáforo por tabela;
  campo de justificativa aparece nos itens vermelhos e cinzas. Botão **Confirmar** só com
  `editar`. Depois de confirmada, a tela mostra a fotografia gravada (não relê o ERP) e
  esconde Atualizar.
- Menu Compras: item "Conferência de preços" depois de "Pedido" em `app/(private)/layout.tsx`.
- Design system travado (TailAdmin v2, `components/ui`).

## Fora do escopo (decidido em 01/10/2026)

- Tetos da régua (coluna fica, tela ignora).
- Editar preço pela intranet (US-046 do backlog).
- Reabrir conferência.
- Aviso no sino quando entra NF nova.
- Empresa 1.
- Carregar NFs anteriores ao deploy.
- Proposta de preço (motor da branch `teste`). Quando ele for mergeado, o detalhe ganha a
  coluna "proposta" lendo `precificacao/proposta/nfe/:chave`; nada aqui precisa mudar.
