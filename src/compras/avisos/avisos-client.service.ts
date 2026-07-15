import { Injectable, Logger } from '@nestjs/common';

/**
 * Cliente fire-and-forget para o avisos-service (notificações da intranet).
 * Não bloqueia o fluxo de negócio: se o avisos-service estiver fora ou
 * AVISOS_SERVICE_URL não estiver configurado, vira no-op silencioso.
 *
 * Env:
 *  - AVISOS_SERVICE_URL: base do serviço (ex.: http://avisos-service.acacessorios.local)
 *  - AVISOS_APP_TOKEN: opcional; enviado no header x-app-token se o serviço exigir.
 */
@Injectable()
export class AvisosClientService {
  private readonly logger = new Logger(AvisosClientService.name);

  private get base(): string | undefined {
    return process.env.AVISOS_SERVICE_URL?.replace(/\/+$/, '');
  }

  /** Dispara um evento de sistema. NÃO usar await — é fire-and-forget. */
  emitirSistema(payload: Record<string, any>): void {
    const base = this.base;
    if (!base) return; // avisos-service não configurado

    const headers: Record<string, string> = { 'Content-Type': 'application/json' };
    if (process.env.AVISOS_APP_TOKEN) headers['x-app-token'] = process.env.AVISOS_APP_TOKEN;

    fetch(`${base}/avisos/sistema`, {
      method: 'POST',
      headers,
      body: JSON.stringify(payload),
    })
      .then(async (r) => {
        if (!r.ok) {
          this.logger.warn(
            `aviso ${payload.chave} -> HTTP ${r.status}: ${await r.text().catch(() => '')}`,
          );
        }
      })
      .catch((e) => this.logger.warn(`Falha ao emitir aviso ${payload.chave}: ${e?.message}`));
  }
}
