# Vinculação de Nota Fiscal ao Pedido — Documentação Técnica

> Documentação de desenvolvimento do módulo `vicunlacao-nfe` (funcionalidade
> **"Vincular NFe"** de Compras). Este README acompanha o código do módulo.
>
> O **manual do usuário** (passo a passo com prints) é servido como página de ajuda
> standalone no frontend, em `cotacao-frontend/public/ajuda/vincular-nf/index.html`
> (aberto pelo botão **"?"** na listagem de pedidos).

---

## 1. Visão geral

A funcionalidade permite **vincular uma NF-e (nota fiscal eletrônica de entrada) aos
itens de um pedido de compra**, fazendo a *conferência* item a item entre o que foi
pedido e o que foi faturado. O vínculo:

- casa automaticamente os itens do XML da NF com os itens do pedido (por referência e,
  como reforço, por análise semântica da descrição);
- permite ajuste manual (vincular/desvincular item a item);
- é **persistido** no Postgres do `compras-service` (um pedido pode ter várias NF-e;
  uma NF pode ser repartida entre vários pedidos — relação N:N com controle de saldo);
- **atualiza automaticamente o status do pedido** (`Faturado`, `Faturado parcialmente`,
  `Entregue`, `Entregue parcialmente`) conforme a cobertura e o estado da NF no ERP;
- conta com um **job de auto-vínculo** que varre pedidos abertos e cria *sugestões* de
  vínculo para validação manual.

A feature vive **dentro da tela do pedido** (`compras/cotacao/pedido/[id]`), não há mais
uma tela separada de "Vinculação de NF-e" no menu (decisão de projeto — ver §11).

### Princípio de projeto

O motor de casamento já existia (tela antiga de vinculação manual por digitação da chave).
O trabalho foi **relocar** a feature para dentro do pedido, **trocar a digitação da chave
por seleção em lista**, **persistir** o vínculo e adicionar **status automático**,
**conferência por item** e **auto-vínculo**. O algoritmo de casamento em si foi mantido.

---

## 2. Arquitetura

```
┌──────────────────────────────────────────────────────────────────────────┐
│  cotacao-frontend (Next.js / App Router)                                   │
│                                                                            │
│  app/(private)/compras/cotacao/pedido/[id]/page.tsx                        │
│    ├── Aba "Itens do Pedido"                                               │
│    └── Aba "Vincular NFe"                                                  │
│          ├── VincularNotaModal.tsx     (lista NF-e + conferência)         │
│          ├── VinculacaoResultado.tsx   (3 listas + vínculo manual)        │
│          └── Conferência Pedido × Faturado (visão por item)               │
└───────────────────────────────┬──────────────────────────────────────────┘
                                 │ HTTP (REST) — serviceUrl('compras')
                                 ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  compras-service (NestJS)                                                  │
│                                                                            │
│  compras/vicunlacao-nfe/                                                   │
│    ├── vinculacao-nfe.controller.ts   (endpoints REST)                    │
│    ├── vinculacao-nfe.service.ts      (motor de casamento + persistência  │
│    │                                    + status + conferência)           │
│    ├── auto-vinculo.service.ts        (job @Cron + sugestão sob demanda)  │
│    └── vinculacao-nfe.repository.ts   (Firebird via OPENQUERY + Prisma)   │
└──────┬───────────────────────────────────────┬───────────────────────────┘
       │ OPENQUERY (linked server CONSULTA)     │ Prisma
       ▼                                        ▼
┌────────────────────┐                ┌─────────────────────────────────────┐
│ Firebird (ERP)     │                │ PostgreSQL (compras-service)        │
│  NFE_DISTRIBUICAO  │                │  com_pedido / com_pedido_itens      │
│  NF_ENTRADA_XML    │                │  com_cotacao_itens_for              │
│  PEDIDOS_COTACOES  │                │  com_nfe_conciliacao  (XML íntegro) │
│  PRODUTOS / MARCAS │                │  com_pedido_nfe_vinculo*  (NOVO)    │
└────────────────────┘                │  com_nfe_saldo_item       (NOVO)    │
                                       └─────────────────────────────────────┘
                            ▲
                            │ POST /vinculacao-nfe/nf-lancada
                ┌───────────┴───────────┐
                │ calculadora-st-service │  (ao marcar status_erp='LANCADA')
                └────────────────────────┘
```

### Serviços e integrações

| Serviço | Papel |
|---|---|
| **cotacao-frontend** | Telas do pedido, modal de vínculo, conferência. |
| **compras-service** | Motor de casamento, persistência, status, conferência, auto-vínculo. |
| **calculadora-st-service** | Ao conciliar a NF e marcar `status_erp='LANCADA'`, chama `POST /vinculacao-nfe/nf-lancada` para que os pedidos virem `Entregue`. |
| **log-service** | Recebe os eventos do histórico do pedido (`POST /log`), em *fire-and-forget*. |

### Fontes do XML da NF-e (ordem de prioridade)

1. **Postgres `com_nfe_conciliacao`** (primária) — o `calculadora-st-service` importa o
   XML completo e **íntegro**.
2. **Firebird `NF_ENTRADA_XML`** via OPENQUERY (fallback) — atenção: o OPENQUERY
   **trunca** `XML_COMPLETO` em ~11 KB, zerando os itens de NF maiores. Por isso o
   Postgres é a fonte primária.

> ⚠️ **Empresa:** o cadastro/cotação/fornecedor usa a **empresa 3 (gerencial)**; a NF-e,
> por ser fiscal, está na **empresa 1**. Isso está codificado em `auto-vinculo.service.ts`
> (`const EMPRESA = 3`) e nas queries da NF (`empresa = 1`).

---

## 3. Modelo de dados

### 3.1. Tabelas novas (DDL — aplicação manual)

Os DDLs **não são aplicados por migration** — são entregues em arquivo `.sql` e aplicados
manualmente no Postgres (padrão do projeto). Arquivos:

Os arquivos `.sql` ficam em `specs/vincular-nf-no-pedido/` (na raiz do sprint):

| Arquivo | Conteúdo |
|---|---|
| `ddl-com_pedido_nfe_vinculo.sql` | Cabeçalho + itens do vínculo (Fase 1). |
| `ddl-fase2.sql` | `data_recebimento`, `confirmado`, `origem_vinculo` (Fase 2). |
| `ddl-com_nfe_saldo.sql` | Saldo por item da NF (relação N:N). |

#### `com_pedido_nfe_vinculo` (cabeçalho — 1 por par pedido × NF)

| Coluna | Tipo | Observação |
|---|---|---|
| `id` | TEXT (PK) | cuid |
| `pedido_id` | TEXT → `com_pedido.id` | FK `ON DELETE CASCADE` |
| `pedido_cotacao` | INTEGER | nº da cotação/pedido (Firebird) |
| `for_codigo` | INTEGER | fornecedor do pedido |
| `chave_nfe` | VARCHAR(44) | chave de acesso |
| `emitente` | TEXT | razão social do emitente |
| `data_emissao` | TIMESTAMP | |
| `valor_total` | DECIMAL(15,2) | |
| `usuario` | VARCHAR(120) | quem salvou |
| `confirmado` | BOOLEAN (default `true`) | `false` = sugestão automática pendente |
| `origem_vinculo` | VARCHAR(20) (default `'manual'`) | `'manual'` \| `'auto'` |
| `data_recebimento` | (em `com_pedido`) | gravada quando o pedido vira `Entregue` |

**Unicidade:** `UNIQUE (pedido_id, chave_nfe)` — re-salvar o mesmo par **substitui** o
snapshot anterior (upsert).

#### `com_pedido_nfe_vinculo_item` (itens do snapshot)

Cada item tem um **`tipo`** que define em qual das 3 listas ele aparece:

| `tipo` | Significado |
|---|---|
| `vinculado` | item do XML casado com um produto do pedido |
| `xml_sem_vinculo` | item do XML que não casou com nenhum produto do pedido |
| `pedido_sem_vinculo` | produto do pedido que não foi coberto por nenhum item do XML |

Colunas principais: `produto_xml`, `cprod_xml`, `quantidade_xml`, `vuncom_xml`,
`pro_codigo`, `pro_descricao`, `quantidade_cotacao`, `quantidade_pedido`, `valor_pedido`,
`quantidade_alocada`, `excede_saldo`, `match_campo`, `match_valor`, `origem`.

> Os itens `pedido_sem_vinculo` do snapshot são **ignorados ao recarregar** — a lista é
> sempre **recalculada** a partir dos `com_pedido_itens` reais do pedido (escopo por
> fornecedor). Isso corrige snapshots antigos gerados sem o filtro por fornecedor.

#### `com_nfe_saldo_item` (snapshot do total por item da NF)

Semeado a partir do XML (qCom agregado por `cProd`). Serve de denominador para o cálculo
de saldo quando a mesma NF é repartida entre vários pedidos.

| Coluna | Tipo |
|---|---|
| `id` | TEXT (PK) |
| `chave_nfe` | VARCHAR(44) |
| `cprod` | TEXT |
| `descricao` | TEXT |
| `qtd_total` | DECIMAL(15,4) |

**Unicidade:** `UNIQUE (chave_nfe, cprod)` — usado no upsert do snapshot.

### 3.2. Tabelas externas lidas (não alteradas)

- **Firebird:** `NFE_DISTRIBUICAO`, `NF_ENTRADA_XML`, `PEDIDOS_COTACOES` (+ `ITENS`,
  `PRODUTOS`, `MARCAS`), `PRODUTOS_FORNECEDOR_NFE` (relacionamento validado
  `COD_PROD_FORNECEDOR → PRO_CODIGO`, EMPRESA 1 — método 1 do casamento, §4.1).
- **Postgres:** `com_pedido`, `com_pedido_itens`, `com_cotacao_itens_for`,
  `com_nfe_conciliacao` (XML íntegro + `status_erp`), `com_produto_fornecedor_referencia`
  (referência por fornecedor/grupo).

### 3.3. Saldo (N:N entre NF e pedido)

Uma NF pode entregar para vários pedidos e um pedido pode receber várias NFs. O sistema
controla **saldo por item**:

- **Saldo da NF** = `qtd_total` (snapshot `com_nfe_saldo_item`) − consumido por vínculos
  **confirmados** de qualquer pedido.
- **Saldo do pedido** = quantidade do item no pedido − consumido por vínculos
  confirmados de outras NFs.
- `quantidade_alocada` = `min(qCom, saldoNf, saldoPedido)`; só vincula automaticamente se
  houver saldo (>0). Sem saldo, o item vai para "XML sem vínculo" (pode ser vinculado
  manualmente, com aviso `excede_saldo`).

O saldo é **sempre recalculado a partir de confirmados** no momento de salvar
(`aplicarSaldoNosItens`), garantindo consistência mesmo com edições concorrentes.

---

## 4. Motor de casamento (item do XML → produto do pedido)

Implementado em `vinculacao-nfe.service.ts`. Para cada `<prod>` do XML, tenta nesta ordem:

1. **Relacionamento validado** (`PRODUTOS_FORNECEDOR_NFE`) — §4.1;
2. **Referência** / **referência grupo** (`encontrarMatch`) — §4.2;
3. **Análise semântica** (`matchSemantico`) — §4.3.

### 4.1. Relacionamento validado produto-fornecedor — `matchProdutoFornecedorNfe`

**Primeira** tentativa (prioritária). Usa a tabela `PRODUTOS_FORNECEDOR_NFE` do ERP
(Firebird, **EMPRESA = 1**), onde o sistema grava o vínculo `COD_PROD_FORNECEDOR`
(= `cProd` da NF) → `PRO_CODIGO` (nosso código interno) **validado pelo usuário ao
lançar a NF**. Por ser a fonte de verdade:

- resolve o `cProd` **direto** no `PRO_CODIGO` (não passa pelas colunas de referência);
- **não** aplica o guard de atributos (modelo/cor/lado) — a relação já foi validada;
- considera o **GRUPO do fornecedor** (matriz/filiais, via `FornecedorGrupoService`),
  igual à referência grupo;
- lê **sempre ao vivo** no Firebird (`OPENQUERY CONSULTA`); filtra por `FOR_CODIGO IN
  (grupo)` e pelo `cProd` (forma crua e sem zeros à esquerda) para não varrer o catálogo.

O `PRO_CODIGO` validado pode **não** ser o que está no pedido (validado sob outra
unidade/descrição, ou de uma filial). Por isso só casa quando o código **pertence ao
pedido**; caso contrário, devolve `null` e o motor cai nos próximos métodos
(referência → grupo → semântica). Sem `for_codigo` o método é pulado (a tabela é por
fornecedor). Badge no frontend: **"Produto Fornecedor (NF-e)"** (`match_campo =
'produto_fornecedor_nfe'`).

> Uma falha no OPENQUERY desse método **não** quebra o vínculo: o mapa volta vazio e o
> motor segue para referência/grupo/semântica.

### 4.2. Match por referência — `encontrarMatch`

Compara **chaves derivadas do XML** contra **três colunas de referência** da cotação:

- **Chaves do XML:** `cProd`; primeiro token de `xProd` (ex.: `FCA1130DS …`); e — para
  fornecedores marcados com "referência no fim da descrição" (ex.: ARTEB) — o número no
  fim do `xProd`.
- **Colunas da cotação:** `referencia`, `ref_fabricante`, `ref_fornecedor`.

> **Nunca** casa contra `pro_codigo` (código interno). O código interno jamais coincide
> com o código do fornecedor, e casá-los gerava colisões (ex.: `cProd 038883` ≠
> `pro_codigo 38883` de outro produto).

**Normalização (`normRef`):** remove espaços, força maiúsculas e ignora zeros à esquerda
(`000000000004904246` → `4904246`). Há ainda um *guard* que barra o match quando
modelo/cor/lado/ano divergem, mesmo com a referência igual.

### 4.3. Match semântico (fallback) — `matchSemantico`

Quando a referência não casa, compara a **descrição** por interseção de tokens
significativos (`threshold = 0.7`, mínimo de 3 tokens em comum), reforçado por:

- **valor unitário** próximo (preço do pedido × `vUnCom` da NF);
- **ano** compatível (extraído da descrição, ex.: aplicações automotivas).

### 4.4. Escopo por pedido/fornecedor

O produto casado precisa **pertencer a este pedido** (`com_pedido_itens`). Um casamento
contra a cotação (que tem itens de vários fornecedores) cujo produto não está no pedido
vai para "XML sem vínculo", não conta como vinculado.

### 4.5. Referência por fornecedor (grupo)

A referência **exibida** dos itens do pedido prioriza a `com_produto_fornecedor_referencia`
(do próprio fornecedor ou de um **relacionado do grupo** — matriz/filiais). Ver
`FornecedorGrupoService` e a memória *Fornecedores relacionados (grupo)*.

### 4.6. Parsing do XML

`parseItensNfe` extrai `cProd/xProd/qCom/vUnCom/vProd` de cada `<prod>` por regex
(aceita prefixo de namespace). `sanitizeXml` trata: base64+gzip (`H4sI…`), BLOB gzip/zlib,
XML escapado (`&lt;`), BOM e bytes nulos. `parseAjustesUnitPorCprod` extrai, por item,
fator de conversão de unidade (uCom × uTrib), preço unitário, **IPI**, **desconto** e
**acréscimo** — usados na conferência de valor.

---

## 5. API REST (`compras-service`)

Base: `serviceUrl('compras') + '/compras'`. Controller: `@Controller('vinculacao-nfe')`.

| Método | Rota | Descrição |
|---|---|---|
| `POST` | `/vinculacao-nfe` | **Calcula** a vinculação (não persiste). Body `{ pedido, nfe, for_codigo }`. Devolve totais + `vinculados` / `xml_sem_vinculo` / `pedido_sem_vinculo`. |
| `POST` | `/vinculacao-nfe/salvar` | **Salva** (upsert) o snapshot da conferência. Recalcula saldo e status do pedido. |
| `GET` | `/vinculacao-nfe/pedido/:pedidoId` | Lista os cabeçalhos das NF-e já vinculadas ao pedido. |
| `GET` | `/vinculacao-nfe/conferencia/:pedidoId` | **Conferência por item** (pedido × faturado), somente leitura. |
| `GET` | `/vinculacao-nfe/:vinculoId` | Carrega um snapshot salvo (remonta as 3 listas). |
| `DELETE` | `/vinculacao-nfe/:vinculoId` | Remove um vínculo salvo (cascade nos itens). Reverte status. |
| `POST` | `/vinculacao-nfe/:vinculoId/confirmar` | Confirma um vínculo (sugestão → confirmado) e recalcula status. |
| `POST` | `/vinculacao-nfe/:vinculoId/rejeitar` | Rejeita uma sugestão (não sugere a mesma NF de novo). |
| `POST` | `/vinculacao-nfe/item/:itemId/vincular` | Vincula manualmente, pela conferência, um item da NF a um produto do pedido. |
| `POST` | `/vinculacao-nfe/item/:itemId/desvincular` | Desfaz o vínculo de um item (volta para "XML sem vínculo"). |
| `POST` | `/vinculacao-nfe/nf-lancada` | NF virou `LANCADA` no ERP → marca pedidos como `Entregue`. Idempotente. |
| `POST` | `/vinculacao-nfe/auto/varredura` | Dispara manualmente a varredura de auto-vínculo. |
| `POST` | `/vinculacao-nfe/auto/pedido/:pedidoId` | Sugere vínculos para **um** pedido (botão "Sugerir vínculo de NF"). |
| `POST` | `/vinculacao-nfe/pedido/:pedidoId/ipi-no-valor` | Liga/desliga "IPI incluso no valor unitário". |
| `POST` | `/vinculacao-nfe/resumo` | Resumo de conclusão por chave (para a listagem de NF). Body `{ chaves: [] }`. |
| `GET` | `/vinculacao-nfe/por-chave/:chave` | Detalhe da vinculação (confirmada) de uma NF, item a item, com o pedido de cada item. |

> ⚠️ **Ordem das rotas:** as rotas específicas (`/nf-lancada`, `/item/...`,
> `/conferencia/...`, `/auto/...`, `/resumo`, `/por-chave/...`) são declaradas **antes**
> de `/:vinculoId` para não serem sombreadas pelo parâmetro coringa.

Listagem das NF-e disponíveis para vincular (consumida pelo modal):
`GET /compras/nota-fiscal/disponiveis?pedidoId=...&mostrarTodas=true` — com fallback para
`/compras/nota-fiscal/nfe-distribuicao`. O filtro por grupo de fornecedores é aplicado **no
servidor** (parâmetro `mostrarTodas`).

---

## 6. Status automático do pedido

`recalcularStatusPedido(pedidoId)` recalcula o status a partir da **cobertura dos itens**
do pedido por vínculos confirmados (`tipo='vinculado'`), cruzando com o `status_erp` de
cada NF (`LANCADA` = entregue):

| Situação | Status resultante |
|---|---|
| Todos os itens cobertos e **todas** as NFs `LANCADA` | `Entregue` (grava `data_recebimento`) |
| Algum item entregue, mas não todos (mix lançado/não lançado, ou itens faltando) | `Entregue parcialmente` |
| Nenhuma NF lançada, **todos** os itens cobertos | `Faturado` |
| Nenhuma NF lançada, **parte** coberta | `Faturado parcialmente` |
| Nenhum item coberto | mantém o status atual |
| Status sugerido pendente | `Vínculo sugerido` |

**Regras de segurança:** nunca rebaixa `Entregue` nem altera `Cancelado`. Ao **remover** o
último vínculo confirmado, o pedido volta a `Liberado`.

> O vocabulário de status segue a memória *Compras: status do pedido* (`Liberado` é o
> antigo `Finalizado`; `Faturado/Entregue` só vêm de NF, nunca manualmente).

---

## 7. Auto-vínculo (sugestões)

`auto-vinculo.service.ts` — job periódico + sugestão sob demanda. **Nada é confirmado
automaticamente**; o job só cria *sugestões* (`confirmado=false`, `origem='auto'`) e marca
o pedido como `Vínculo sugerido`, para validação manual.

### Critérios de candidatura de uma NF a um pedido

1. **Fornecedor:** o emitente tem CNPJ do fornecedor do pedido **ou de um relacionado do
   grupo** (matriz/filiais). O casamento por nome foi removido (gerava falsos positivos).
2. **Data:** emissão da NF **posterior** à data do pedido e dentro de `AUTOVINCULO_MAX_DIAS`
   (default 60).
3. **Saldo:** a NF ainda tem saldo (não totalmente consumida por confirmados).
4. **Cobertura:** ≥ `COBERTURA_MINIMA` (30%) dos itens do pedido casaram.

### Disparos

- **`@Cron`** (`cronVarredura`): default a cada minuto (`AUTOVINCULO_CRON`). Lock
  `rodando` evita varreduras sobrepostas. Fonte de NF: `fetchNfeDisponiveis` (Firebird,
  só NF não importada).
- **Sob demanda** (`sugerirParaPedido` / botão na tela): fonte é a **conciliação
  (Postgres)** por janela de data, incluindo **NF já lançada** no ERP — assim reconcilia
  pedidos cujas notas já entraram.

Sugestões **pendentes** são reprocessadas a cada ciclo (refresh): mudanças na lógica de
casamento corrigem sozinhas as sugestões antigas. Vínculos **confirmados** ou **rejeitados**
nunca são sobrescritos.

---

## 8. NF lançada → Entregue (integração calculadora-st)

Quando o `calculadora-st-service` marca `status_erp='LANCADA'` em `com_nfe_conciliacao`, ele
chama `POST /vinculacao-nfe/nf-lancada` com `{ lancadas: [{ chave_nfe, dt_entrada }] }`.
Para cada chave, o `compras-service` acha os vínculos **confirmados**, e `recalcularStatusPedido`
decide entre `Entregue` (todos os itens cobertos por NF lançada) e `Entregue parcialmente`.
Idempotente; nunca rebaixa `Entregue` nem altera `Cancelado`.

---

## 9. Conferência por item (fechamento)

`conferenciaPorItem(pedidoId)` (somente leitura) monta **uma linha por item do pedido**
comparando **pedido × faturado** (somando todas as NF-e confirmadas), mais os itens das NFs
que não estão no pedido. Por item devolve: quantidade pedida, faturada, entregue, saldo,
valor pedido/faturado, **diferença de valor** e **situação**:

| Situação | Critério |
|---|---|
| `completo` | quantidade exata e valor OK |
| `parcial` | faturado a menos (valor OK) |
| `divergente` | valor diferente (≥ R$ 1,00) **ou** quantidade faturada a mais |
| `nao_faturado` | nada faturado |

**Valor faturado** considera, por unidade da NF: conversão de unidade (uCom × uTrib),
**desconto** (−), **acréscimo** (+) e **IPI** (+ apenas se o pedido tem o flag
`ipi_no_valor`). A tolerância de divergência de valor é **R$ 1,00**
(`TOLERANCIA_VALOR_DIVERGENTE`) — diferenças menores (centavos/arredondamento) são ignoradas.

---

## 10. Frontend

### 10.1. Integração na tela do pedido

`app/(private)/compras/cotacao/pedido/[id]/page.tsx` tem duas abas:

- **Itens do Pedido** (`aba='itens'`) — itens + status por produto.
- **Vincular NFe** (`aba='conferencia'`) — três sub-visões (`vincularView`):
  - `conferencia` — cards de totais + tabela **Conferência Pedido × Faturado** + botão
    **"Sugerir vínculo de NF"** + blocos de vínculo manual por item;
  - `nova` — abre o `VincularNotaModal` (modo `embedded`) para vincular uma nova NF;
  - `vinculadas` — lista as NF já vinculadas.

### 10.2. `VincularNotaModal.tsx`

Dois passos:

1. **Lista** — "Notas já vinculadas a este pedido" (Abrir / Ver NF / Remover) + "NF-e
   disponíveis" com **busca** e toggle **"Mostrar todas"**. Filtro por fornecedor/grupo é
   feito no servidor; a busca textual é local.
2. **Conferência** — chama `POST /vinculacao-nfe`, renderiza `VinculacaoResultado` e
   permite **Salvar** (`POST /vinculacao-nfe/salvar`).

Props relevantes: `embedded` (inline, sem overlay, para uso em aba), `mode`
(`'completo'` | `'somente-vinculadas'`), `autoAbrirVinculoId` (abre direto a conferência
de um vínculo — usado no "Revisar e confirmar" de uma sugestão).

Helpers de similaridade de nome (`normalizarNome`, `nomeSimilar`) ficam neste arquivo, mas
o filtro principal é do servidor.

### 10.3. `VinculacaoResultado.tsx` + `types.ts`

Renderiza as 3 listas lado a lado, o vínculo manual **por clique + busca** (bidirecional —
clicar num item de "XML sem vínculo" ou "Pedido sem vínculo" abre um seletor da lista
oposta) e o botão **"Salvar vinculação"**. Os tipos da resposta da API estão em `types.ts`
(`VinculacaoResponse`, `Vinculado`, `XmlSemVinculo`, `PedidoSemVinculo`, `Totais`) +
`formatCurrency`.

---

## 11. Decisões de projeto

1. **Filtro de NF por fornecedor/grupo (CNPJ), não só por nome.** A mesma empresa emite por
   CNPJs diferentes (matriz/filial); usa-se o grupo de fornecedores. Busca livre + toggle
   "mostrar todas" como escape.
2. **Removida a tela antiga "Vinculação de NF-e" do menu.** O vínculo passa a existir só
   dentro do pedido.
3. **Vínculo manual por clique** (sem arrasta-e-solta).
4. **Persistência no Postgres do compras-service** (não no ERP/Firebird). Relação N:N com
   saldo; re-salvar substitui o snapshot.
5. **Status automático** derivado da cobertura + estado da NF no ERP; nunca rebaixa
   `Entregue`/`Cancelado`.

### Fora de escopo

- Alterar o algoritmo de casamento (já está bom).
- Lançar/baixar a NF-e no ERP (Firebird). A persistência é só do **vínculo de conferência**
  no Postgres.
- Gerar planilha Excel (era do script antigo; não é requisito da tela).

---

## 12. Configuração (variáveis de ambiente)

| Variável | Default | Efeito |
|---|---|---|
| `AUTOVINCULO_CRON` | a cada 1 min | Expressão cron do job de auto-vínculo. |
| `AUTOVINCULO_MAX_DIAS` | `60` | Janela máx. (dias) entre data do pedido e emissão da NF. |
| `AUTOVINCULO_LIMITE` | `100` | Máx. de pedidos processados por varredura. |

`COBERTURA_MINIMA = 0.3` e `TOLERANCIA_VALOR_DIVERGENTE = 1` são constantes no código.

---

## 13. Scripts de manutenção

Em `compras-service/scripts/` (rodar sob demanda):

| Script | Função |
|---|---|
| `rodar-autovinculo-tudo.js` | Roda a varredura de auto-vínculo em todos os pedidos. |
| `recomputar-status-entrega.js` | Recalcula o status (`Entregue`/`Faturado`/…) dos pedidos vinculados. |
| `reescopar-vinculos-legados.js` | Reaplica o escopo por fornecedor em snapshots antigos. |

---

## 14. Arquivos de referência

**Backend** (`compras-service/src/compras/vicunlacao-nfe/`):
`vinculacao-nfe.controller.ts`, `vinculacao-nfe.service.ts`, `auto-vinculo.service.ts`,
`vinculacao-nfe.repository.ts`, `vinculacao-nfe.module.ts`, `dto/*.ts`.

**Frontend** (`cotacao-frontend/app/(private)/compras/`):
`cotacao/pedido/[id]/page.tsx`, `cotacao/pedido/[id]/VincularNotaModal.tsx`,
`cotacao/vinculacao-nfe/VinculacaoResultado.tsx`, `cotacao/vinculacao-nfe/types.ts`.

**Specs** (`specs/vincular-nf-no-pedido/`): `00-PLANO.md` … `10-frontend-aba-conferencia.md`,
`ddl-*.sql`.
