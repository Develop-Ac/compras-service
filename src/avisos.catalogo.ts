import type { Catalogo } from './common/avisos/avisos-client';

/**
 * O QUE O COMPRAS-SERVICE EMITE. As regras de Compras já existem no
 * avisos-service (seeds 002/003, ativas): a sincronização no boot só garante
 * que estas chaves existam e atualiza a descrição — nunca sobrescreve o que
 * foi ajustado em /avisos/config. Adicione aqui cada evento novo antes de
 * emitir (guia: knowledge-base › guia-emitir-avisos).
 */
export const CATALOGO_COMPRAS: Catalogo = {
  'nfe.sugestao': {
    descricao: 'Auto-vínculo encontrou NF-e(s) que casam com pedido em aberto.',
    titulo: 'NF-e com sugestão de vínculo',
    corpo: '{count} sugestão(ões) aguardando conferência',
    link: '/compras/vinculacao-nfe',
    canais: ['badge', 'mural'],
    prioridade: 'normal',
    alvo: { tipo: 'tela', valor: '/compras/vinculacao-nfe' },
    agrupar: true,
    cooldown_min: 60,
  },
};
