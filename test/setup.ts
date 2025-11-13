// test/setup.ts

// Mock global para crypto.randomUUID
Object.defineProperty(global, 'crypto', {
  value: {
    randomUUID: () => 'mocked-uuid-12345',
  },
});

// Mock global para console para evitar logs durante testes
const originalConsole = console;
global.console = {
  ...originalConsole,
  log: () => {},
  error: originalConsole.error,
  warn: () => {},
  info: () => {},
};

// Setup para timezone
process.env.TZ = 'UTC';