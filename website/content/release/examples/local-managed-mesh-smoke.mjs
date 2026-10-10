// No cloud/cluster prerequisites: exercise native central + a fake factory.
// Usage: node examples/local-managed-mesh-smoke.mjs <source-or-bundle-root>
import {spawn, spawnSync} from 'node:child_process';
import {randomBytes} from 'node:crypto';
import {access} from 'node:fs/promises';
import {constants} from 'node:fs';
import net from 'node:net';
import path from 'node:path';
import assert from 'node:assert/strict';

assert(process.argv[2], 'usage: node examples/local-managed-mesh-smoke.mjs <bundle-root>');
const bundle = path.resolve(process.argv[2]);
for (const name of ['yscale-cloud', 'yscale-factory']) {
  await access(path.join(bundle, 'bin', name), constants.X_OK);
}
const children = [];
let interrupted = false;
let launchError;
const interrupt = () => { interrupted = true; };
process.once('SIGINT', interrupt);
process.once('SIGTERM', interrupt);
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));
async function port() {
  const server = net.createServer();
  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', resolve);
  });
  const chosen = server.address().port;
  await new Promise(resolve => server.close(resolve));
  return chosen;
}
function start(binary, env, args = []) {
  // Deliberately do not inherit provider credentials, database URLs or kubeconfig.
  const child = spawn(path.join(bundle, 'bin', binary), args, {
    cwd: bundle, env: {PATH: process.env.PATH, ...env}, stdio: 'ignore'
  });
  child.once('error', err => { launchError = `${binary}: ${err.code}`; });
  children.push(child);
  return child;
}
const request = (url, options = {}) => fetch(url, {...options, signal: AbortSignal.timeout(1500)});
async function waitFor(check, description) {
  for (let attempt = 0; attempt < 100; attempt++) {
    if (interrupted) throw Error('local smoke interrupted');
    if (launchError) throw Error(launchError);
    if (children.some(p => p.exitCode !== null || p.signalCode !== null)) {
      throw Error(`service exited while waiting for ${description}`);
    }
    try { if (await check()) return; } catch { /* Retry local readiness only. */ }
    await delay(100);
  }
  throw Error(`timed out waiting for ${description}; verify binaries and local TCP access`);
}
async function stop(child) {
  if (!child.pid || child.exitCode !== null || child.signalCode !== null) return;
  const exited = new Promise(resolve => child.once('exit', resolve));
  child.kill('SIGTERM');
  const fallback = setTimeout(() => child.kill('SIGKILL'), 5000);
  await exited;
  clearTimeout(fallback);
}
try {
  const factoryPort = await port();
  let centralPort = await port();
  while (centralPort === factoryPort) centralPort = await port();
  const factoryURL = `http://127.0.0.1:${factoryPort}`;
  const centralURL = `http://127.0.0.1:${centralPort}`;
  const factoryToken = randomBytes(32).toString('hex');
  const adminToken = randomBytes(32).toString('hex');
  start('yscale-factory', {
    FACTORY_DEV: '1', FACTORY_KEK: randomBytes(32).toString('base64'),
    FACTORY_BEARER_TOKEN: factoryToken, FACTORY_LISTEN: `127.0.0.1:${factoryPort}`
  });
  await waitFor(async () => (await request(factoryURL + '/v1/tenants/probe/fabric')).status === 401,
    'factory authentication boundary');
  start('yscale-cloud', {
    FACTORY_URL: factoryURL, FACTORY_BEARER_TOKEN: factoryToken,
    YSCALE_ADMIN_TOKEN: adminToken, YSCALE_CENTRAL_ENDPOINT: centralURL,
    YSCALE_TOKEN: randomBytes(32).toString('hex')
  }, [`-listen=127.0.0.1:${centralPort}`]);
  await waitFor(async () => (await request(centralURL + '/healthz')).ok, 'central readiness');
  const response = await request(centralURL + '/v1/admin/tenants', {
    method: 'POST', headers: {Authorization: 'Bearer ' + adminToken, 'Content-Type': 'application/json'},
    body: JSON.stringify({id: 'cust_local_smoke'})
  });
  assert(response.ok, `tenant provisioning failed, HTTP ${response.status}`);
  const tenant = await response.json();
  assert.equal(tenant.customer_id, 'cust_local_smoke');
  assert(tenant.token, 'tenant credential missing');
  assert.equal(tenant.endpoint, centralURL);
  await waitFor(async () => {
    const r = await request(factoryURL + '/v1/tenants/cust_local_smoke/fabric', {
      headers: {Authorization: 'Bearer ' + factoryToken}
    });
    if (!r.ok) return false;
    const fabric = await r.json();
    return fabric.status === 'ready' && Boolean(fabric.login_server);
  }, 'central-to-factory provisioning');
  const rejected = spawnSync(path.join(bundle, 'bin', 'yscale-cloud'), [], {
    cwd: bundle, env: {PATH: process.env.PATH, TS_OAUTH_CLIENT_ID: 'diagnostic-only'},
    encoding: 'utf8', timeout: 10000
  });
  assert.equal(rejected.status, 1, 'legacy OAuth configuration was not rejected');
  assert(rejected.stderr.includes('TS_OAUTH_CLIENT_ID is not supported'));
  if (interrupted) throw Error('local smoke interrupted');
  console.log('PASS: local managed-mesh API flow. Fake provisioning only; no cloud resources.');
} finally {
  await Promise.all(children.map(stop));
  process.removeListener('SIGINT', interrupt);
  process.removeListener('SIGTERM', interrupt);
}
