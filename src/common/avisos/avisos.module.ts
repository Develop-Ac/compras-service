import { DynamicModule, Global, Inject, Injectable, Logger, Module, OnModuleInit } from '@nestjs/common';
import { AvisosClient, Catalogo, EmitirOpcoes, configDoAmbiente } from './avisos-client';

/**
 * avisos-client v1.0.0 — módulo Nest.
 *
 *   // app.module.ts
 *   AvisosModule.forRoot({ servico: 'vendas', catalogo: CATALOGO })
 *
 *   // em qualquer service
 *   constructor(private readonly avisos: AvisosService) {}
 *   this.avisos.emitir('orcamento.vencendo', { ref: id, vars: {...}, usuarios: [uid] });
 *
 * No boot manda o catálogo (regras nascem inativas). Lê AVISOS_SERVICE_URL,
 * AVISOS_APP_TOKEN e AVISOS_DRY_RUN do ambiente.
 */
export const AVISOS_OPCOES = 'AVISOS_OPCOES';

export interface AvisosModuleOpcoes {
  servico: string;
  catalogo?: Catalogo;
  url?: string;
  token?: string;
  dryRun?: boolean;
}

@Injectable()
export class AvisosService implements OnModuleInit {
  private readonly logger = new Logger('Avisos');
  readonly cliente: AvisosClient;

  constructor(@Inject(AVISOS_OPCOES) private readonly opcoes: AvisosModuleOpcoes) {
    this.cliente = new AvisosClient(
      configDoAmbiente(opcoes.servico, {
        url: opcoes.url ?? process.env.AVISOS_SERVICE_URL,
        token: opcoes.token ?? process.env.AVISOS_APP_TOKEN,
        dryRun: opcoes.dryRun ?? process.env.AVISOS_DRY_RUN === '1',
        log: (nivel, msg) => (nivel === 'log' ? this.logger.log(msg) : nivel === 'warn' ? this.logger.warn(msg) : this.logger.error(msg)),
      }),
    );
  }

  async onModuleInit() {
    if (this.opcoes.catalogo && this.cliente.configurado) await this.cliente.sincronizar(this.opcoes.catalogo);
  }

  /** Fire-and-forget. Não aguardar; não chamar dentro de transação. */
  emitir(evento: string, o: EmitirOpcoes = {}): void {
    this.cliente.emitir(evento, o);
  }
}

@Global()
@Module({})
export class AvisosModule {
  static forRoot(opcoes: AvisosModuleOpcoes): DynamicModule {
    return {
      module: AvisosModule,
      providers: [{ provide: AVISOS_OPCOES, useValue: opcoes }, AvisosService],
      exports: [AvisosService],
    };
  }
}
