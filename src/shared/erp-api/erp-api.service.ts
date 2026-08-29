import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';

/* =============================================================================
   CLIENTE DA erp-firebird-api
   -----------------------------------------------------------------------------
   Caminho de leitura do ERP que NÃO passa pelo SQL Server.

   A tela de fornecedores lia tudo de [BI].[dbo].[Stage_Fornecedores]: uma cópia
   que o ETL atualiza de tempos em tempos. Fornecedor cadastrado ou corrigido
   agora só aparecia na próxima carga, e a tela não tinha como dizer isso — o
   cadastro simplesmente "não existia" para quem estava procurando. Pelo
   Firebird a leitura é do cadastro vivo.

   Do lado de lá o SELECT é montado a partir do catálogo: nome de coluna é
   validado contra os metadados, valor viaja como parâmetro, e o filtro de
   EMPRESA é obrigatório nas tabelas que têm a coluna.

   MIGRAÇÃO SEM JANELA: cada chamada tem o caminho antigo como alternativa. Sem
   `ERP_API_URL` configurada, nada muda de comportamento; com ela, o Stage vira
   plano B e o log diz sempre que caiu para ele.
   ============================================================================= */

/** Envelope de resposta da erp-firebird-api. */
interface RespostaErp {
  dados: any[];
  meta: {
    tabela: string;
    linhas: number;
    ms: number;
    /** Bateu no teto da tabela: falta filtro, ou a lista precisa ser paginada. */
    truncado: boolean;
    cache: boolean;
  };
}

/** Colunas da lista de fornecedores da tela (busca, membros do grupo, filiais). */
const CAMPOS_LISTA = 'FOR_CODIGO,FOR_NOME,NOME_FANTASIA,CPF_CNPJ,CIDADE,UF,INATIVO';

/** Colunas da aba "Fornecedor" — o cadastro completo. */
const CAMPOS_CADASTRO =
  'FOR_CODIGO,FOR_NOME,NOME_FANTASIA,CPF_CNPJ,RG_IE,ENDERECO,NUMERO,BAIRRO,' +
  'CIDADE,UF,CEP,FONE,FAX,CELULAR,EMAIL,CONTATO,INATIVO';

/** Teto de um filtro `em` do outro lado. */
const MAX_EM = 500;

@Injectable()
export class ErpApiService {
  private readonly logger = new Logger(ErpApiService.name);

  private readonly base: string;
  private readonly token: string;
  private readonly timeoutMs: number;

  /**
   * Breaker de saída. Quando a API está fora, insistir custa o timeout inteiro
   * a cada chamada — e o caminho do Stage, que funciona, só começa depois
   * disso. Passadas as falhas seguidas, para de tentar por um tempo e vai
   * direto para o plano B.
   */
  private falhasSeguidas = 0;
  private mudoAte = 0;
  private static readonly LIMITE_FALHAS = 3;
  private static readonly COOLDOWN_MS = 60_000;

  constructor(private readonly config: ConfigService) {
    this.base = String(this.config.get('ERP_API_URL') ?? '').trim().replace(/\/+$/, '');
    this.token = String(this.config.get('ERP_API_TOKEN') ?? '').trim();
    this.timeoutMs = Number(this.config.get('ERP_API_TIMEOUT_MS') ?? 30_000);

    if (this.base) {
      this.logger.log(`[ERP-API] leitura do ERP habilitada em ${this.base}`);
    } else {
      this.logger.log(
        '[ERP-API] ERP_API_URL não configurada — a tela de fornecedores segue lendo o Stage do SQL Server.',
      );
    }
  }

  /** Só chama a API quem tem para onde chamar, e fora do período de cooldown. */
  get habilitado(): boolean {
    if (!this.base) return false;
    return Date.now() >= this.mudoAte;
  }

  /* ------------------------------ transporte ------------------------------- */

  private async pedir(
    caminho: string,
    params: Record<string, any> = {},
    opts: { exigirCompleto?: boolean; checarTruncado?: boolean } = {},
  ): Promise<any[]> {
    const url = new URL(this.base + caminho);
    for (const [chave, valor] of Object.entries(params)) {
      if (valor === undefined || valor === null || valor === '') continue;
      // `f` é repetível: cada filtro vai no seu próprio parâmetro. Juntar dois
      // filtros numa string só faria o segundo virar valor do primeiro, porque
      // o operador `em` também usa vírgula.
      for (const item of Array.isArray(valor) ? valor : [valor]) {
        url.searchParams.append(chave, String(item));
      }
    }

    try {
      const resposta = await fetch(url, {
        headers: {
          'x-app-token': this.token,
          // O relatório /health/n1 do outro lado é por serviço: sem este header
          // o padrão de consulta unitária aparece como "desconhecido".
          'x-servico': 'compras-service',
        },
        signal: AbortSignal.timeout(this.timeoutMs),
      });

      if (!resposta.ok) {
        const corpo = await resposta.text().catch(() => '');
        throw new Error(`HTTP ${resposta.status} em ${caminho}: ${corpo.slice(0, 300)}`);
      }

      const json = (await resposta.json()) as RespostaErp;
      this.falhasSeguidas = 0;

      if (json?.meta?.truncado && opts.checarTruncado !== false) {
        const aviso = `${caminho} bateu no teto da tabela (${json.meta.linhas} linhas) e o resultado está INCOMPLETO`;
        if (opts.exigirCompleto) throw new Error(aviso);
        this.logger.warn(`[ERP-API] ${aviso} — reduza o filtro.`);
      }

      return json?.dados ?? [];
    } catch (erro: any) {
      this.registrarFalha(caminho, erro);
      throw erro;
    }
  }

  /** POST das rotas de consulta com corpo (consulta livre, por-chaves). */
  private async pedirPost(
    caminho: string,
    corpo: Record<string, any>,
    opts: { exigirCompleto?: boolean; checarTruncado?: boolean } = {},
  ): Promise<any[]> {
    try {
      const resposta = await fetch(this.base + caminho, {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          'x-app-token': this.token,
          'x-servico': 'compras-service',
        },
        body: JSON.stringify(corpo),
        signal: AbortSignal.timeout(this.timeoutMs),
      });

      if (!resposta.ok) {
        const texto = await resposta.text().catch(() => '');
        throw new Error(`HTTP ${resposta.status} em ${caminho}: ${texto.slice(0, 300)}`);
      }

      const json = (await resposta.json()) as RespostaErp;
      this.falhasSeguidas = 0;

      if (json?.meta?.truncado && opts.checarTruncado !== false) {
        const aviso = `${caminho} bateu no teto da tabela (${json.meta.linhas} linhas) e o resultado está INCOMPLETO`;
        if (opts.exigirCompleto) throw new Error(aviso);
        this.logger.warn(`[ERP-API] ${aviso} — reduza o filtro.`);
      }

      return json?.dados ?? [];
    } catch (erro: any) {
      this.registrarFalha(caminho, erro);
      throw erro;
    }
  }

  /**
   * Motivo da falha em uma linha.
   *
   * `fetch` embrulha qualquer problema de rede numa mensagem única — "fetch
   * failed" — e joga a causa real no `cause`. Sem abrir esse nível, o log não
   * distingue nome que não resolve de porta fechada, de certificado recusado,
   * e cada um deles pede uma correção diferente.
   */
  private motivo(erro: any): string {
    if (erro?.name === 'TimeoutError' || erro?.name === 'AbortError') {
      return `timeout de ${this.timeoutMs}ms`;
    }

    // Quando o host tem IPv4 e IPv6, o Node tenta os dois e embrulha as duas
    // falhas num AggregateError — que não tem `code`. O motivo está nos filhos.
    let causa: any = erro?.cause;
    if (Array.isArray(causa?.errors) && causa.errors.length) causa = causa.errors[0];

    const codigo = causa?.code ?? causa?.errno;
    if (!codigo) return causa?.message || erro?.message || String(erro);

    const onde = causa?.hostname ?? causa?.address;
    const porta = causa?.port ? `:${causa.port}` : '';
    const explicacao: Record<string, string> = {
      ENOTFOUND: 'o nome não resolve neste container',
      EAI_AGAIN: 'o DNS não respondeu',
      ECONNREFUSED: 'o endereço resolve, mas ninguém atende nessa porta',
      ECONNRESET: 'a conexão foi cortada pelo outro lado',
      ETIMEDOUT: 'o pacote saiu e não voltou — normalmente firewall ou host errado',
      DEPTH_ZERO_SELF_SIGNED_CERT: 'certificado autoassinado: não foi emitido pela CA interna',
      UNABLE_TO_VERIFY_LEAF_SIGNATURE: 'falta a CA interna no container (NODE_EXTRA_CA_CERTS)',
      SELF_SIGNED_CERT_IN_CHAIN: 'falta a CA interna no container (NODE_EXTRA_CA_CERTS)',
    };

    const detalhe = explicacao[codigo] ? ` — ${explicacao[codigo]}` : '';
    return `${codigo}${onde ? ` em ${onde}${porta}` : ''}${detalhe}`;
  }

  private registrarFalha(caminho: string, erro: any) {
    this.falhasSeguidas++;
    const motivo = this.motivo(erro);

    if (this.falhasSeguidas >= ErpApiService.LIMITE_FALHAS) {
      this.mudoAte = Date.now() + ErpApiService.COOLDOWN_MS;
      this.falhasSeguidas = 0;
      this.logger.error(
        `[ERP-API] ${caminho} falhou ${ErpApiService.LIMITE_FALHAS}x seguidas (${motivo}). ` +
          `Pausando por ${ErpApiService.COOLDOWN_MS / 1000}s — as leituras vão pelo Stage nesse período.`,
      );
    } else {
      this.logger.warn(`[ERP-API] ${caminho} falhou (${motivo}). Tentando pelo Stage.`);
    }
  }

  /**
   * Executa pela API e cai para o caminho antigo se ela não responder.
   *
   * A tela não pode depender da disponibilidade de mais um serviço: enquanto os
   * dois caminhos existem, indisponibilidade da API é degradação (volta a ler
   * a cópia do ETL), não interrupção.
   */
  async comFallback<T>(viaApi: () => Promise<T>, viaStage: () => Promise<T>): Promise<T> {
    if (!this.habilitado) return viaStage();
    try {
      return await viaApi();
    } catch {
      return viaStage();
    }
  }

  /* ------------------------------- consultas ------------------------------- */

  /**
   * Busca da tela: razão social, nome fantasia, documento ou código.
   *
   * A rota é nomeada do outro lado porque o filtro do montador só combina
   * condições com AND, e aqui é preciso OU entre quatro colunas. Deixar isso
   * para o consumidor significaria adivinhar se "429" é código ou pedaço de
   * CNPJ — e errar em silêncio.
   */
  async buscarFornecedores(termo: string, empresa: number, limite = 50): Promise<any[]> {
    return this.pedir(
      '/erp/fornecedores/busca',
      { termo, empresa, limite },
      { checarTruncado: false },
    );
  }

  /** Fornecedores por código, em lote (membros de grupo, alvos de parâmetro). */
  async fornecedoresPorCodigo(codigos: number[], empresa: number): Promise<any[]> {
    const lista = [...new Set(codigos.filter((n) => Number.isFinite(n)))];
    if (!lista.length) return [];

    // O `em` do outro lado aceita no máximo 500 valores. Fatiar aqui é melhor
    // que descobrir o teto com um 400 no meio da tela.
    const lotes: number[][] = [];
    for (let i = 0; i < lista.length; i += MAX_EM) lotes.push(lista.slice(i, i + MAX_EM));

    const partes = await Promise.all(
      lotes.map((lote) =>
        this.pedir(
          '/erp/fornecedores',
          {
            empresa,
            campos: CAMPOS_LISTA,
            f: `FOR_CODIGO:em:${lote.join(',')}`,
            // +1 de folga: a API marca `truncado` quando o número de linhas
            // bate exatamente no limite. Pedindo o tamanho exato do lote, TODA
            // consulta completa voltaria marcada como truncada — e o aviso que
            // deveria significar "faltou linha" viraria ruído no log.
            limite: lote.length + 1,
          },
          { checarTruncado: false },
        ),
      ),
    );
    return partes.flat();
  }

  /**
   * Cadastro completo de UM fornecedor — a aba "Fornecedor".
   *
   * Vai pela consulta livre com `FOR_CODIGO:igual` porque é essa forma que o
   * agrupador do outro lado reconhece: consultas unitárias que chegam na mesma
   * janela viram um único SELECT com IN.
   */
  async fornecedorCompleto(forCodigo: number, empresa: number): Promise<any | null> {
    const linhas = await this.pedir(
      '/erp/fornecedores',
      {
        empresa,
        campos: CAMPOS_CADASTRO,
        f: `FOR_CODIGO:igual:${forCodigo}`,
        limite: 2, // 1 linha esperada; a folga evita o falso "truncado" no log
      },
      { checarTruncado: false },
    );
    return linhas[0] ?? null;
  }

  /**
   * Filiais candidatas: mesma raiz de CNPJ (8 primeiros dígitos).
   *
   * O recorte mora do lado da API porque o cadastro guarda o CNPJ pontuado —
   * recortar 8 caracteres do texto não casa com nada. `excluir` tira quem já
   * está no grupo.
   */
  async filiaisPorRaiz(raiz: string, excluir: number[], empresa: number): Promise<any[]> {
    return this.pedir(
      '/erp/fornecedores/filiais',
      { raiz, empresa, excluir: excluir.length ? excluir.join(',') : undefined },
      { checarTruncado: false },
    );
  }

  /* ------------------------------- clientes -------------------------------- */

  /**
   * Cliente pelo documento — é assim que a aba Garantia liga fornecedor a
   * cliente (a garantia é contas a receber, chaveada por cli_codigo).
   *
   * A rota compara os DÍGITOS: no ERP o documento está pontuado nas duas
   * tabelas, e comparar texto com texto limpo devolve zero linhas com status
   * 200 — indistinguível de "não é cliente".
   */
  async clientePorDocumento(documento: string, empresa: number): Promise<any | null> {
    const digitos = String(documento ?? '').replace(/\D/g, '');
    if (digitos.length < 11) return null;
    const linhas = await this.pedir(
      '/erp/clientes/documento',
      { documento: digitos, empresa },
      { checarTruncado: false },
    );
    return linhas[0] ?? null;
  }

  /** Busca de cliente por nome, documento ou código — vínculo manual da aba Garantia. */
  async buscarClientes(termo: string, empresa: number, limite = 50): Promise<any[]> {
    return this.pedir(
      '/erp/clientes/busca',
      { termo, empresa, limite },
      { checarTruncado: false },
    );
  }

  /* ------------------------------- produtos -------------------------------- */

  /**
   * Produtos por código, em lote. `campos` aceita coluna de relação
   * ("marca.MAR_DESCRICAO") — por isso a consulta vai pela rota POST.
   */
  async produtosPorCodigos(
    codigos: Array<string | number>,
    empresa: number,
    campos: string[],
  ): Promise<any[]> {
    const lista = [
      ...new Set(codigos.map((c) => Math.trunc(Number(c))).filter((n) => Number.isFinite(n))),
    ];
    if (!lista.length) return [];

    const lotes: number[][] = [];
    for (let i = 0; i < lista.length; i += MAX_EM) lotes.push(lista.slice(i, i + MAX_EM));

    const partes = await Promise.all(
      lotes.map((lote) =>
        this.pedirPost(
          '/erp/produtos/consulta',
          {
            empresa,
            campos,
            filtros: [{ campo: 'PRO_CODIGO', op: 'em', valor: lote }],
            limite: lote.length + 1,
          },
          { checarTruncado: false },
        ),
      ),
    );
    return partes.flat();
  }

  /**
   * De-para produto-fornecedor da NF-e (PRODUTOS_FORNECEDOR_NFE): todos os
   * fornecedores do grupo E os códigos (cProd) na MESMA consulta — dois `em`
   * combinados com AND. Uma chamada por NF, retorno do tamanho da NF.
   *
   * Não fatiar em uma chamada por fornecedor: com dois filtros variando, o
   * agrupador da API não une as consultas, e cada uma vira uma ida ao Firebird
   * (um grupo grande tem dezenas de fornecedores). O `em` casa o valor mesmo
   * com o espaço à direita que o ERP grava — a comparação do Firebird ignora
   * o preenchimento.
   */
  async referenciasFornecedorNfe(
    forCodigos: number[],
    codigos: string[],
    empresa: number,
  ): Promise<any[]> {
    const fors = [...new Set(forCodigos.filter((n) => Number.isFinite(n)))].slice(0, MAX_EM);
    const lista = [...new Set(codigos.map((c) => String(c ?? '').trim()).filter(Boolean))];
    if (!fors.length || !lista.length) return [];

    const lotes: string[][] = [];
    for (let i = 0; i < lista.length; i += MAX_EM) lotes.push(lista.slice(i, i + MAX_EM));

    const partes = await Promise.all(
      lotes.map((lote) =>
        this.pedirPost(
          '/erp/produtos-fornecedor/consulta',
          {
            empresa,
            campos: ['FOR_CODIGO', 'COD_PROD_FORNECEDOR', 'PRO_CODIGO'],
            filtros: [
              { campo: 'FOR_CODIGO', op: 'em', valor: fors },
              { campo: 'COD_PROD_FORNECEDOR', op: 'em', valor: lote },
            ],
            limite: 5000,
          },
          { checarTruncado: false },
        ),
      ),
    );
    return partes.flat();
  }

  /* -------------------------------- cotações -------------------------------- */

  /**
   * Itens de UMA cotação, com produto e marca (rota nomeada /cotacoes/itens).
   * Substitui o OPENQUERY de PEDIDOS_COTACOES + ITENS + PRODUTOS + MARCAS —
   * o mais recorrente do serviço (tela do pedido e motor de vinculação).
   */
  async cotacaoItens(pedido: number, empresa: number): Promise<any[]> {
    return this.pedir(
      '/erp/cotacoes/itens',
      { pedido, empresa },
      { checarTruncado: false },
    );
  }

  /* ----------------------------- entrada fiscal ----------------------------- */

  /**
   * Última compra de um produto por fornecedor (rota nomeada do compra-casada).
   * A agregação (MAX por fornecedor + desempate por NFE/ITEM) roda do lado da
   * API; aqui chega uma linha por fornecedor, no shape do OPENQUERY antigo.
   */
  async ultimaCompraProduto(proCodigo: number, empresa: number): Promise<any[]> {
    return this.pedir(
      '/erp/nf-entrada/ultima-compra-produto',
      { produto: proCodigo, empresa },
      { checarTruncado: false },
    );
  }

  /**
   * NF-e recebidas e ainda não importadas (rota nomeada /pendentes). O XML não
   * vem junto: TEM_XML diz se existe, e o conteúdo sai por xmlPorChaves.
   */
  async nfeDistribuicaoPendentes(
    empresa: number,
    dataIni: string,
    dataFim: string,
  ): Promise<any[]> {
    return this.pedir(
      '/erp/nfe-distribuicao/pendentes',
      { empresa, dataIni, dataFim },
      { checarTruncado: false },
    );
  }

  /** Linhas da NFE_DISTRIBUICAO de uma chave (emitente/data/situação — sem XML). */
  async nfeDistribuicaoPorChave(chave: string, empresa: number): Promise<any[]> {
    return this.pedir(
      '/erp/nfe-distribuicao',
      {
        empresa,
        campos: 'CHAVE_NFE,NOME_EMITENTE,DATA_EMISSAO,SITUACAO_NFE',
        f: `CHAVE_NFE:igual:${String(chave ?? '').trim()}`,
        limite: 2,
      },
      { checarTruncado: false },
    );
  }

  /** XML completo por chave de acesso — a rota aceita no máximo 50 chaves por lote. */
  async xmlPorChaves(chaves: string[], empresa: number): Promise<any[]> {
    const lista = [...new Set(chaves.map((c) => String(c ?? '').trim()).filter(Boolean))];
    if (!lista.length) return [];

    const LOTE_XML = 50;
    const lotes: string[][] = [];
    for (let i = 0; i < lista.length; i += LOTE_XML) lotes.push(lista.slice(i, i + LOTE_XML));

    const partes = await Promise.all(
      lotes.map((lote) =>
        this.pedirPost(
          '/erp/nf-entrada-xml/por-chaves',
          { chaves: lote, empresa },
          { checarTruncado: false },
        ),
      ),
    );
    return partes.flat();
  }

  /* ------------------------------ faturamento ------------------------------- */

  /** Itens das NFS (venda) por número de nota, em lote — usados pela aba Garantia. */
  async nfsItensPorNotas(nfsList: number[], empresa: number): Promise<any[]> {
    const lista = [...new Set(nfsList.filter((n) => Number.isFinite(n)))];
    if (!lista.length) return [];

    const lotes: number[][] = [];
    for (let i = 0; i < lista.length; i += MAX_EM) lotes.push(lista.slice(i, i + MAX_EM));

    const partes = await Promise.all(
      lotes.map((lote) =>
        this.pedir('/erp/nfs-itens', {
          empresa,
          campos: 'NFS,PRO_CODIGO,QUANTIDADE,UNITARIO',
          f: `NFS:em:${lote.join(',')}`,
          limite: 5000,
        }),
      ),
    );
    return partes.flat();
  }
}
