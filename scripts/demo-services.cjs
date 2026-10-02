// Starts only local synthetic demos. No provider credentials are needed.
const { spawn } = require('node:child_process');
const { randomUUID, randomBytes } = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const net = require('node:net');
const root = path.resolve(__dirname, '..');
const smoke = process.argv.includes('--smoke');
const data = path.join(root, smoke ? `.test-data/browser-${randomUUID()}` : '.demo-data');
const children = [];
let stopping = false;
function start(name, command, args, cwd, env = {}) {
  const child = spawn(command, args, { cwd: path.join(root, cwd), env: { ...process.env, ...env }, stdio: ['ignore', 'pipe', 'pipe'], windowsHide: true });
  child.stdout.on('data', b => process.stdout.write(`[${name}] ${b}`));
  child.stderr.on('data', b => process.stderr.write(`[${name}] ${b}`));
  child.on('error', error => { console.error(`${name}: ${error.message}`); stop(1); });
  child.on('exit', code => { if (!stopping && code !== null) { console.error(`${name} exited: ${code}`); stop(code || 1); } });
  children.push(child);
  return child;
}
function stop(code = 0) {
  if (stopping) return;
  stopping = true;
  for (const child of children) if (child.exitCode === null) child.kill('SIGTERM');
  process.exitCode = code;
  setTimeout(() => process.exit(code), 1500).unref();
}
process.on('SIGINT', () => stop());
process.on('SIGTERM', () => stop());
async function portFree(port) {
  await new Promise((resolve, reject) => {
    const server = net.createServer();
    server.once('error', () => reject(new Error(`Port ${port} is already used. Stop its existing server before starting the managed demo suite.`)));
    server.listen(port, '127.0.0.1', () => server.close(resolve));
  });
}
async function ready(url) {
  const deadline = Date.now() + 90000;
  while (Date.now() < deadline && !stopping) {
    try { if ((await fetch(url, { signal: AbortSignal.timeout(1500) })).ok) return; } catch { /* service still starting */ }
    await new Promise(r => setTimeout(r, 400));
  }
  throw new Error(`Service did not become ready: ${url}`);
}
(async () => {
  for (const port of [3000, 3100, 3200, 3300, 5000, 8000, 8090]) await portFree(port);
  for (const frontend of ['trace-stores/apps/storefront/.next/BUILD_ID', ...['interview-nailer', 'blockchain-api-service', 'kubernetes-demo'].map(p => `portfolio-projects/${p}/frontend/build/index.html`)]) {
    if (!fs.existsSync(path.join(root, frontend))) throw new Error(`Missing frontend build: ${frontend}. Run npm run demo:setup first.`);
  }
  const model = path.join(root, 'trace-stores/models/u2netp.onnx');
  if (!fs.existsSync(model)) throw new Error('Missing model. Run python trace-stores/scripts/download_model.py first.');
  fs.mkdirSync(data, { recursive: true });
  const venvPython = path.join(root, '.venv', process.platform === 'win32' ? 'Scripts/python.exe' : 'bin/python');
  const python = process.env.PYTHON || (fs.existsSync(venvPython) ? venvPython : 'python');
  const local = { NODE_ENV: 'development', HOST: '127.0.0.1', DATABASE_URL: '', ANTHROPIC_API_KEY: '', JWT_SECRET: randomBytes(32).toString('hex') };
  start('trace-api', python, ['-m', 'uvicorn', 'app.main:app', '--host', '127.0.0.1', '--port', '8000'], 'trace-stores/services/trace-api', { MODEL_PATH: model, REQUIRE_MODEL: 'true', CORS_ORIGINS: 'http://localhost:3000,http://127.0.0.1:3000' });
  start('trace', process.execPath, ['node_modules/next/dist/bin/next', 'start', '-H', '127.0.0.1', '-p', '3000'], 'trace-stores/apps/storefront', { NODE_ENV: 'production' });
  start('interview', process.execPath, ['server.js'], 'portfolio-projects/interview-nailer/backend', { ...local, PORT: '5000', AI_MODE: 'mock', STORAGE_MODE: 'file', STORE_FILE: path.join(data, 'interview.json'), CLIENT_URL: 'http://localhost:5000,http://127.0.0.1:5000' });
  start('banking', process.execPath, ['src/server.js'], 'portfolio-projects/pesa-fams-banking-prototype', { ...local, PORT: '3100', APP_MODE: 'prototype' });
  start('blockchain', process.execPath, ['server.js'], 'portfolio-projects/blockchain-api-service/backend', { ...local, PORT: '3300', DEMO_MODE: 'true', ENABLE_TRANSACTION_BROADCAST: 'false', DATA_FILE: path.join(data, 'blockchain.json') });
  start('kubernetes', process.execPath, ['server.js'], 'portfolio-projects/kubernetes-demo/backend', { ...local, PORT: '3200', POD_NAME: 'local-process' });
  start('voice', python, ['demo/server.py', '--port', '8090', '--database', path.join(data, 'voice.sqlite')], 'portfolio-projects/voice-ai-missed-call');
  await Promise.all([3000,3100,3200,3300,5000,8090].map(port => ready(`http://127.0.0.1:${port}/`)).concat(ready('http://127.0.0.1:8000/ready')));
  console.log('All six local demos ready: Trace3000, Banking3100, Kubernetes3200, Blockchain3300, Interview5000, Voice8090. Ctrl+C stops this suite.');
  if (smoke) {
    const child = spawn(process.execPath, ['scripts/browser-smoke.cjs'], { cwd: root, env: process.env, stdio: 'inherit', windowsHide: true });
    children.push(child);
    child.on('error', error => { console.error(error); stop(1); });
    child.on('exit', code => stop(code ?? 1));
  }
})().catch(error => { console.error(error.message); stop(1); });
