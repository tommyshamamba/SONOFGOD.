const { spawnSync } = require('node:child_process');
const path = require('node:path');
const fs = require('node:fs');
const root = path.resolve(__dirname, '..');
const npm = process.env.npm_execpath;
if (!npm) throw new Error('Run this script with npm run demo:setup.');
function run(command, args, cwd = root) {
  const result = spawnSync(command, args, { cwd, stdio: 'inherit', windowsHide: true, env: process.env });
  if (result.error) throw result.error;
  if (result.status !== 0) throw new Error(`${args.join(' ')} failed (${result.status})`);
}
const projects = ['trace-stores/apps/storefront', 'portfolio-projects/pesa-fams-banking-prototype', ...['interview-nailer','blockchain-api-service','kubernetes-demo'].flatMap(p => [`portfolio-projects/${p}/backend`, `portfolio-projects/${p}/frontend`])];
for (const project of projects) {
  const cwd = path.join(root, project);
  run(process.execPath, [npm, 'ci'], cwd);
  if (project.endsWith('/frontend') || project.endsWith('/storefront')) run(process.execPath, [npm, 'run', 'build'], cwd);
}
const python = process.env.PYTHON || 'python';
const venvPython = path.join(root, '.venv', process.platform === 'win32' ? 'Scripts/python.exe' : 'bin/python');
if (!fs.existsSync(venvPython)) run(python, ['-m', 'venv', '.venv']);
run(venvPython, ['-m','pip','install','-r','trace-stores/services/trace-api/requirements-test.txt']);
run(venvPython, ['trace-stores/scripts/download_model.py']);
console.log('Setup complete. Run npm run demos, or npm run test:browser:managed after installing Playwright Chromium.');
