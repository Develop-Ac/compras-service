/**
 * avisos-client v1.0.0 — cliente de EMISSÃO para o avisos-service da intranet.
 *
 * Sem dependências (fetch nativo do Node 20). Serve dentro do Nest (ver
 * avisos.module.ts) e fora dele (worker, script): `criarCliente({...}).emitir(...)`.
 *
 * Fonte única: repositório avisos-service, pasta sdk/avisos-client. Cada serviço recebe
 * uma CÓPIA em src/common/avisos/ (script copiar.mjs) — não edite a cópia;
 * edite aqui e copie de novo.
 *
 * Regras de uso (guia completo: knowledge-base › guia-emitir-avisos):
 *  - `emitir()` NUNCA lança e NUNCA deve ser aguardado: é fire-and-forget com
 *    timeout curto e uma retentativa. O negócio não pode depender do aviso.
 *  - `ref` dá idempotência: o mesmo evento para a mesma referência agrupa em
 *    vez de duplicar (regra com agrupar=true, que é o padrão).
 *  - A chave completa é `<servico>.<evento>`; o catálogo do serviço declara
 *    as chaves e os templates padrão, sincronizados no boot (regra nasce
 *    inativa; quem liga é a tela /avisos/config).
 *
 * Env lidas por `configDoAmbiente()`:
 *  - AVISOS_SERVICE_URL   base do serviço (ex.: http://avisos-service.acacessorios.local)
 *  - AVISOS_APP_TOKEN     opcional; vai no header x-app-token se o serviço exigir
 *  - AVISOS_DRY_RUN       "1" = só loga, não envia (dev)
 */

export type Prioridade = 'normal' | 'alta' | 'urgente';
export type Canal = 'badge' | 'mural' | 'whatsapp' | 'banner' | 'desktop';
export type TipoAlvo = 'setor' | 'usuario' | 'todos' | 'tela';

export interface RegraCatalogo {
  /** descrição para a tela /avisos/config */
  descricao?: string;
  /** templates com {variavel} */
  titulo: string;
  corpo?: string;
  link?: string;
  prioridade?: Prioridade;
  canais?: Canal[];
  /** alvo padrão quando a emissão não informa setor/tela/usuarios */
  alvo?: { tipo: TipoAlvo; valor?: string };
  agrupar?: boolean;
  cooldown_min?: number;
}

export type Catalogo = Record<string, RegraCatalogo>;

export interface EmitirOpcoes {
  /** referência do fato (id do orçamento, nº da NF...) — idempotência */
  ref?: string | number;
  /** variáveis dos templates */
  vars?: Record<string, unknown>;
  /** alvos desta emissão (têm precedência sobre o alvo padrão da regra) */
  setor?: string;
  tela?: string;
  usuarios?: string[];
  /** sobrescritas pontuais (normalmente não usar: o template vem da regra) */
  titulo?: string;
  corpo?: string;
  link?: string;
  prioridade?: Prioridade;
  canais?: Canal[];
}

export interface AvisosConfig {
  /** prefixo das chaves deste serviço: "vendas", "compras"... */
  servico: string;
  url?: string;
  token?: string;
  dryRun?: boolean;
  timeoutMs?: number;
  log?: (nivel: 'log' | 'warn' | 'error', mensagem: string) => void;
}

export function configDoAmbiente(servico: string, extra: Partial<AvisosConfig> = {}): AvisosConfig {
  return {
    servico,
    url: process.env.AVISOS_SERVICE_URL,
    token: process.env.AVISOS_APP_TOKEN,
    dryRun: process.env.AVISOS_DRY_RUN === '1',
    ...extra,
  };
}

export class AvisosClient {
  private readonly base: string | undefined;
  private readonly log: NonNullable<AvisosConfig['log']>;

  constructor(private readonly cfg: AvisosConfig) {
    this.base = cfg.url?.replace(/\/+$/, '') || undefined;
    this.log = cfg.log ?? ((nivel, msg) => console[nivel === 'log' ? 'log' : nivel](`[avisos] ${msg}`));
    if (!this.base && !cfg.dryRun) this.log('warn', 'AVISOS_SERVICE_URL não configurado — emissões viram no-op.');
  }

  get configurado(): boolean {
    return !!this.base || !!this.cfg.dryRun;
  }

  chave(evento: string): string {
    return evento.includes('.') && evento.startsWith(`${this.cfg.servico}.`) ? evento : `${this.cfg.servico}.${evento}`;
  }

  /**
   * Dispara um evento. Fire-and-forget: devolve na hora, nunca lança.
   * Não use `await`; não chame dentro de transação.
   */
  emitir(evento: string, o: EmitirOpcoes = {}): void {
    const chave = this.chave(evento);
    const payload: Record<string, unknown> = {
      chave,
      ref: o.ref == null ? undefined : String(o.ref),
      variaveis: o.vars,
      setor: o.setor,
      tela: o.tela,
      usuarios: o.usuarios?.length ? o.usuarios : undefined,
      titulo: o.titulo,
      corpo: o.corpo,
      link: o.link,
      prioridade: o.prioridade,
      canais: o.canais,
    };
    if (this.cfg.dryRun) {
      this.log('log', `DRY-RUN ${chave} ${JSON.stringify(payload)}`);
      return;
    }
    if (!this.base) return;
    void this.enviar('/avisos/sistema', payload, 1).catch((e) => this.log('warn', `${chave}: ${(e as Error).message}`));
  }

  /**
   * Manda o catálogo do serviço (regras nascem inativas; as existentes não
   * são sobrescritas). Chamar no boot; falha só loga.
   */
  async sincronizar(catalogo: Catalogo): Promise<{ criadas: number; existentes: number } | null> {
    const regras = Object.entries(catalogo).map(([evento, r]) => ({
      chave: this.chave(evento),
      descricao: r.descricao,
      titulo_template: r.titulo,
      corpo_template: r.corpo,
      link_template: r.link,
      prioridade: r.prioridade,
      canais: r.canais,
      alvo_tipo: r.alvo?.tipo,
      alvo_valor: r.alvo?.valor,
      agrupar: r.agrupar,
      cooldown_min: r.cooldown_min,
    }));
    if (this.cfg.dryRun) {
      this.log('log', `DRY-RUN sincronizar ${regras.length} regra(s) de ${this.cfg.servico}`);
      return null;
    }
    if (!this.base) return null;
    try {
      const r = (await this.enviar('/regras/sincronizar', { servico: this.cfg.servico, regras }, 0)) as { criadas: number; existentes: number };
      this.log('log', `catálogo ${this.cfg.servico}: ${r.criadas} nova(s), ${r.existentes} já existente(s)`);
      return r;
    } catch (e) {
      this.log('warn', `catálogo ${this.cfg.servico} não sincronizado: ${(e as Error).message}`);
      return null;
    }
  }

  private async enviar(rota: string, corpo: unknown, retentativas: number): Promise<unknown> {
    const headers: Record<string, string> = { 'content-type': 'application/json' };
    if (this.cfg.token) headers['x-app-token'] = this.cfg.token;
    let ultimoErro: unknown;
    for (let tentativa = 0; tentativa <= retentativas; tentativa++) {
      const ctrl = new AbortController();
      const t = setTimeout(() => ctrl.abort(), this.cfg.timeoutMs ?? 3000);
      try {
        const resp = await fetch(`${this.base}${rota}`, { method: 'POST', headers, body: JSON.stringify(corpo), signal: ctrl.signal });
        if (!resp.ok) throw new Error(`HTTP ${resp.status} ${await resp.text().catch(() => '')}`.trim());
        const texto = await resp.text();
        return texto ? JSON.parse(texto) : null;
      } catch (e) {
        ultimoErro = e;
        if (tentativa < retentativas) await new Promise((r) => setTimeout(r, 500));
      } finally {
        clearTimeout(t);
      }
    }
    throw ultimoErro instanceof Error ? ultimoErro : new Error(String(ultimoErro));
  }
}

export function criarCliente(cfg: AvisosConfig): AvisosClient {
  return new AvisosClient(cfg);
}
