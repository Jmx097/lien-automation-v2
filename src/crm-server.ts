import express from 'express';
import { createCrmRouterFromEnvironment } from './crm/router';

function configuredPort(value: string | undefined): number {
  const port = Number.parseInt(value ?? '8091', 10);
  if (!Number.isInteger(port) || port < 1024 || port > 65535) {
    throw new Error('CRM_PORT must be an unprivileged TCP port between 1024 and 65535');
  }
  return port;
}

const port = configuredPort(process.env.CRM_PORT);
const app = express();
app.disable('x-powered-by');
app.use(express.json({ limit: '64kb' }));
app.use('/crm', createCrmRouterFromEnvironment());

const server = app.listen(port, '127.0.0.1', () => {
  console.log(`CRM service listening on 127.0.0.1:${port}`);
});

function shutdown(signal: string) {
  console.log(`CRM service received ${signal}; stopping`);
  server.close((error) => process.exit(error ? 1 : 0));
}

process.once('SIGTERM', () => shutdown('SIGTERM'));
process.once('SIGINT', () => shutdown('SIGINT'));
