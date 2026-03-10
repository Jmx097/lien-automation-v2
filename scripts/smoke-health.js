const http = require('http');
const path = require('path');
const { spawn } = require('child_process');

const PORT = Number.parseInt(process.env.PORT ?? '8080', 10);
const LOG_FILE = process.env.LOG_FILE ?? path.join(process.cwd(), 'tmp', 'lien-automation-health.log');

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function requestHealth() {
  return new Promise((resolve, reject) => {
    const req = http.get(`http://127.0.0.1:${PORT}/health`, (res) => {
      let body = '';
      res.setEncoding('utf8');
      res.on('data', (chunk) => { body += chunk; });
      res.on('end', () => {
        if (res.statusCode && res.statusCode >= 200 && res.statusCode < 300) {
          resolve(body);
          return;
        }
        reject(new Error(`Health request failed with status ${res.statusCode ?? 'unknown'}`));
      });
    });
    req.on('error', reject);
  });
}

async function main() {
  process.env.SBR_CDP_URL = process.env.SBR_CDP_URL ?? 'wss://example.invalid';

  const initDb = spawn(process.execPath, ['src/queue/init-db.js'], {
    cwd: process.cwd(),
    env: process.env,
    stdio: 'ignore',
  });

  const initExitCode = await new Promise((resolve) => initDb.on('exit', resolve));
  if (initExitCode !== 0) {
    throw new Error(`init-db failed with exit code ${initExitCode}`);
  }

  const server = spawn(process.execPath, [require.resolve('ts-node/dist/bin.js'), 'src/server.ts'], {
    cwd: process.cwd(),
    env: process.env,
    stdio: ['ignore', 'pipe', 'pipe'],
  });

  let combinedLog = '';
  server.stdout.on('data', (chunk) => { combinedLog += chunk.toString(); });
  server.stderr.on('data', (chunk) => { combinedLog += chunk.toString(); });

  try {
    for (let i = 0; i < 30; i += 1) {
      try {
        const body = await requestHealth();
        process.stdout.write(`${body}\n`);
        if (!body.includes('"status":"healthy"')) {
          throw new Error('Health check response did not include expected status');
        }
        return;
      } catch {
        await sleep(1000);
      }
    }
    throw new Error(`Timed out waiting for /health. Logs:\n${combinedLog}`);
  } finally {
    server.kill();
  }
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exit(1);
});
