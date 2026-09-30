import { Injectable } from '@nestjs/common';
import { AvisosService } from '../../common/avisos/avisos.module';
import type { Canal, Prioridade } from '../../common/avisos/avisos-client';

/**
 * Compatibilidade: os pontos de emissão do Compras chamam `emitirSistema(payload)`
 * com o formato cru do POST /avisos/sistema. Por baixo é o avisos-client
 * padrão (src/common/avisos, AvisosModule.forRoot no app.module) — mesmo
 * timeout, retentativa, dry-run e catálogo dos demais serviços.
 * Código novo deve usar `AvisosService.emitir('nfe.sugestao', { ref, vars })` direto.
 */
@Injectable()
export class AvisosClientService {
  constructor(private readonly avisos: AvisosService) {}

  /** Dispara um evento de sistema. NÃO usar await — é fire-and-forget. */
  emitirSistema(payload: Record<string, any>): void {
    if (!payload?.chave) return;
    this.avisos.emitir(String(payload.chave), {
      ref: payload.ref,
      vars: payload.variaveis,
      setor: payload.setor,
      tela: payload.tela,
      usuarios: payload.usuarios,
      titulo: payload.titulo,
      corpo: payload.corpo,
      link: payload.link,
      prioridade: payload.prioridade as Prioridade | undefined,
      canais: payload.canais as Canal[] | undefined,
    });
  }
}
