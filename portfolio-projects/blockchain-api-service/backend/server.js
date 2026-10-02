const express = require('express');
const cors = require('cors');
const { ethers } = require('ethers');
const { randomBytes, randomUUID } = require('node:crypto');
const path = require('node:path');
const fs = require('node:fs');
const redis = require('redis');
const { RateLimiterRedis, RateLimiterMemory } = require('rate-limiter-flexible');
const jwt = require('jsonwebtoken');
const bcrypt = require('bcryptjs');
const { FileStore, digest } = require('./store');

const CHAINS = ['ethereum', 'polygon', 'arbitrum', 'optimism'];
const RPC_VARIABLES = ['ETH_RPC_URL', 'POLYGON_RPC_URL', 'ARBITRUM_RPC_URL', 'OPTIMISM_RPC_URL'];
const asyncRoute = fn => (req, res, next) => Promise.resolve(fn(req, res, next)).catch(next);
const failure = (status, message) => Object.assign(new Error(message), { status });

function readConfig(env) {
  const production = env.NODE_ENV === 'production';
  const demo = env.DEMO_MODE === 'true';
  const secret = env.JWT_SECRET || (production ? '' : randomBytes(32).toString('hex'));
  if (production && (secret.length < 32 || /secret|change|example|your-/i.test(secret))) {
    throw new Error('Production requires a random JWT_SECRET of at least 32 characters');
  }
  if (production && (!env.DATA_FILE || !env.CORS_ORIGIN || !env.REDIS_URL || demo)) {
    throw new Error('Production requires DATA_FILE, CORS_ORIGIN and REDIS_URL, with DEMO_MODE disabled');
  }
  const origins = (env.CORS_ORIGIN || 'http://localhost:3002,http://127.0.0.1:3002,http://localhost:3001,http://localhost,http://127.0.0.1:3001').split(',').map(value => value.trim());
  for (const origin of origins) {
    const parsed = new URL(origin);
    if (parsed.origin !== origin || (production && parsed.protocol !== 'https:')) throw new Error('CORS_ORIGIN must contain exact origins (HTTPS in production)');
  }
  const timeout = Number(env.RPC_TIMEOUT_MS || 8000);
  if (!Number.isInteger(timeout) || timeout < 10 || timeout > 60000) throw new Error('RPC_TIMEOUT_MS must be between 10 and 60000');
  const endpoints = Object.fromEntries(CHAINS.map((chain, index) => {
    if (production && !env[RPC_VARIABLES[index]]) throw new Error(`Production requires ${RPC_VARIABLES[index]}`);
    const endpoint = env[RPC_VARIABLES[index]] || `https://${chain === 'ethereum' ? 'eth' : chain}.llamarpc.com`;
    if (!['http:', 'https:'].includes(new URL(endpoint).protocol)) throw new Error('RPC URLs must use HTTP or HTTPS');
    return [chain, endpoint];
  }));
  return { production, demo, secret, origins, timeout, endpoints, redisUrl: env.REDIS_URL,
    dataFile: env.DATA_FILE || path.join(__dirname, 'data', 'store.json'), broadcast: env.ENABLE_TRANSACTION_BROADCAST === 'true' && !demo };
}

function demoProviders() {
  return Object.fromEntries(CHAINS.map(chain => [chain, {
    getBlockNumber: async () => 12345,
    getBalance: async () => 1000000000000000000n,
    getTransactionCount: async () => 3,
    getBlock: async number => ({ number, hash: `0x${'a'.repeat(64)}`, timestamp: 1700000000, transactions: [], gasUsed: 0n, gasLimit: 30000000n }),
    getTransaction: async () => null,
    getTransactionReceipt: async () => null,
    getFeeData: async () => ({ gasPrice: 1000000000n, maxFeePerGas: 2000000000n, maxPriorityFeePerGas: 1000000000n }),
    estimateGas: async () => 21000n,
  }]));
}

async function boundedCall(operation, timeout) {
  let timer;
  try {
    return await Promise.race([Promise.resolve().then(operation), new Promise((resolve, reject) => {
      timer = setTimeout(() => reject(failure(504, 'Blockchain provider timed out')), timeout);
    })]);
  } catch (error) {
    if (error.status === 504) throw error;
    throw failure(502, 'Blockchain provider unavailable');
  } finally { clearTimeout(timer); }
}

function createApp(options = {}) {
  const config = readConfig(options.env || process.env);
  const store = options.store || new FileStore(config.dataFile);
  const providers = options.providers || (config.demo ? demoProviders() : Object.fromEntries(Object.entries(config.endpoints).map(([chain, url]) => {
    const request = new ethers.FetchRequest(url);
    request.timeout = config.timeout;
    return [chain, new ethers.JsonRpcProvider(request)];
  })));
  let redisClient;
  let limiter = options.limiter;
  if (!limiter && config.redisUrl) {
    redisClient = redis.createClient({ url: config.redisUrl, disableOfflineQueue: true,
      socket: { connectTimeout: 1500, reconnectStrategy: retries => Math.min(100 * (retries + 1), 2000) } });
    // Authentication and RPC must never hang or fall back to unlimited requests on a Redis outage.
    redisClient.on('error', () => {});
    redisClient.connect().catch(() => {});
    limiter = new RateLimiterRedis({ storeClient: redisClient, useRedisPackage: true, keyPrefix: 'blockchain-api', points: 100, duration: 60 });
  }
  if (!limiter) limiter = new RateLimiterMemory({ points: 100, duration: 60 });
  const authLimiter = new RateLimiterMemory({ points: 20, duration: 60 });
  let limiterAvailable = true;
  const app = express();
  app.disable('x-powered-by');
  app.use(cors({ origin: config.origins }));
  app.use(express.json({ limit: '32kb' }));
  app.use((req, res, next) => {
    res.set('X-Content-Type-Options', 'nosniff');
    res.set('Cache-Control', 'no-store');
    res.set('X-Blockchain-Mode', config.demo ? 'demo' : 'live');
    next();
  });
  // A quota rejection is an object with msBeforeNext; infrastructure errors are HTTP 503.
  const consumeLimit = async (rateLimiter, key, res) => {
    let timer;
    try {
      if (rateLimiter === limiter && redisClient && !redisClient.isReady) throw new Error('Redis unavailable');
      await Promise.race([Promise.resolve().then(() => rateLimiter.consume(key)), new Promise((resolve, reject) => {
        timer = setTimeout(() => reject(new Error('Limiter timeout')), 2000);
      })]);
      if (rateLimiter === limiter) limiterAvailable = true;
    } catch (error) {
      if (Number.isFinite(error.msBeforeNext)) {
        res.set('Retry-After', String(Math.max(1, Math.ceil(error.msBeforeNext / 1000))));
        throw failure(429, 'Rate limit exceeded');
      }
      if (rateLimiter === limiter) limiterAvailable = false;
      throw failure(503, 'Rate limiter unavailable; retry later');
    } finally { clearTimeout(timer); }
  };
  const authenticateJwt = (req, res, next) => {
    const token = /^Bearer (\S+)$/.exec(req.get('authorization') || '')?.[1];
    try {
      const decoded = jwt.verify(token, config.secret, { algorithms: ['HS256'], issuer: 'blockchain-api', audience: 'dashboard' });
      if (!store.hasUser(decoded.userId)) throw new Error('Unknown user');
      req.userId = decoded.userId;
      next();
    } catch { res.status(401).json({ error: 'Invalid or missing token' }); }
  };
  const authenticateApiKey = asyncRoute(async (req, res, next) => {
    const secret = req.get('x-api-key') || '';
    const key = /^bk_[a-f0-9]{64}$/.test(secret) ? store.findKey(secret) : undefined;
    if (!key || key.revoked) throw failure(401, 'Invalid or revoked API key');
    await consumeLimit(limiter, key.id, res);
    req.key = key;
    next();
  });
  const signToken = user => jwt.sign({ userId: user.userId }, config.secret, {
    algorithm: 'HS256', expiresIn: '1d', issuer: 'blockchain-api', audience: 'dashboard',
  });
  const credentials = body => {
    const { email, password } = body || {};
    if (typeof email !== 'string' || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email.trim()) || email.length > 254 ||
        typeof password !== 'string' || password.length < 12 || Buffer.byteLength(password) > 72) {
      throw failure(400, 'Provide a valid email and a password of at least 12 characters (maximum 72 bytes)');
    }
    return { email: email.trim().toLowerCase(), password };
  };

  app.get('/health', (req, res) => res.json({ status: 'alive', mode: config.demo ? 'demo' : 'live', chains: CHAINS,
    storage: 'single-process-file', rateLimiter: config.redisUrl ? 'redis' : 'memory' }));
  app.get('/ready', asyncRoute(async (req, res) => {
    const chains = Object.fromEntries(await Promise.all(Object.entries(providers).map(async ([chain, provider]) => {
      try { await boundedCall(() => provider.getBlockNumber(), config.timeout); return [chain, 'ready']; }
      catch { return [chain, 'unavailable']; }
    })));
    const ready = Object.values(chains).every(value => value === 'ready') && (redisClient ? redisClient.isReady : limiterAvailable);
    res.status(ready ? 200 : 503).json({ ready, mode: config.demo ? 'demo' : 'live', chains });
  }));
  app.use('/api/auth', asyncRoute(async (req, res, next) => { await consumeLimit(authLimiter, req.ip, res); next(); }));
  app.post('/api/auth/register', asyncRoute(async (req, res) => {
    const { email, password } = credentials(req.body);
    const user = { userId: randomUUID(), email, password: await bcrypt.hash(password, 12), createdAt: new Date().toISOString() };
    if (!store.addUser(user)) throw failure(409, 'User already exists');
    res.status(201).json({ token: signToken(user), userId: user.userId });
  }));
  app.post('/api/auth/login', asyncRoute(async (req, res) => {
    const { email, password } = credentials(req.body);
    const user = store.findUser(email);
    if (!user || !await bcrypt.compare(password, user.password)) throw failure(401, 'Invalid credentials');
    res.json({ token: signToken(user), userId: user.userId });
  }));
  app.post('/api/keys', authenticateJwt, (req, res, next) => {
    try {
      const name = req.body.name === undefined ? 'Default Key' : req.body.name;
      if (typeof name !== 'string' || !name.trim() || name.length > 100) throw failure(400, 'Key name must contain 1 to 100 characters');
      const apiKey = `bk_${randomBytes(32).toString('hex')}`;
      const key = { id: randomUUID(), digest: digest(apiKey), key: `${apiKey.slice(0, 12)}...`, userId: req.userId,
        name: name.trim(), createdAt: new Date().toISOString(), revoked: false, requests: 0 };
      store.addKey(key);
      res.status(201).json({ apiKey, id: key.id, name: key.name, message: 'Save this key now; it is shown only once' });
    } catch (error) { next(error); }
  });
  app.get('/api/keys', authenticateJwt, (req, res) => res.json({ keys: store.listKeys(req.userId) }));
  app.delete('/api/keys/:keyId', authenticateJwt, (req, res, next) => {
    try {
      if (!store.revoke(req.params.keyId, req.userId)) throw failure(404, 'API key not found');
      res.json({ message: 'API key revoked' });
    } catch (error) { next(error); }
  });
  app.get('/api/v1/chains', (req, res) => res.json({ chains: CHAINS, mode: config.demo ? 'demo' : 'live' }));
  app.get('/api/v1/usage', authenticateApiKey, (req, res) => res.json({ totalRequests: req.key.requests, rateLimit: { points: 100, duration: 60 } }));

  const address = value => { if (typeof value !== 'string' || !ethers.isAddress(value)) throw failure(400, 'Invalid Ethereum address'); return value; };
  const route = (method, routePath, handler) => app[method](`/api/v1/:chain/${routePath}`, authenticateApiKey, asyncRoute(async (req, res) => {
    const provider = providers[req.params.chain];
    if (!provider) throw failure(400, 'Unsupported chain');
    const rpc = (method, ...args) => boundedCall(() => provider[method](...args), config.timeout);
    const result = await handler(req, rpc);
    store.recordRequest(req.key.id);
    res.json({ chain: req.params.chain, mode: config.demo ? 'demo' : 'live', ...result });
  }));
  route('get', 'balance/:address', async (req, rpc) => {
    const account = address(req.params.address);
    const balance = await rpc('getBalance', account);
    return { address: account, balance: ethers.formatEther(balance), wei: balance.toString() };
  });
  route('get', 'nonce/:address', async (req, rpc) => ({ address: address(req.params.address), nonce: await rpc('getTransactionCount', req.params.address) }));
  route('get', 'block/:blockNumber', async (req, rpc) => {
    const number = req.params.blockNumber;
    if (!/^\d+$/.test(number) || !Number.isSafeInteger(Number(number))) throw failure(400, 'Invalid block number');
    const block = await rpc('getBlock', Number(number));
    if (!block) throw failure(404, 'Block not found');
    return { blockNumber: block.number, hash: block.hash, timestamp: block.timestamp, transactions: block.transactions.length,
      gasUsed: block.gasUsed.toString(), gasLimit: block.gasLimit.toString() };
  });
  route('get', 'transaction/:txHash', async (req, rpc) => {
    const hash = req.params.txHash;
    if (!/^0x[a-fA-F0-9]{64}$/.test(hash)) throw failure(400, 'Invalid transaction hash');
    const tx = await rpc('getTransaction', hash);
    if (!tx) throw failure(404, 'Transaction not found');
    const receipt = await rpc('getTransactionReceipt', hash);
    return { hash: tx.hash, from: tx.from, to: tx.to, value: ethers.formatEther(tx.value), gasUsed: receipt?.gasUsed.toString(),
      status: receipt ? (receipt.status === 1 ? 'success' : 'failed') : 'pending', blockNumber: tx.blockNumber };
  });
  route('get', 'gas-price', async (req, rpc) => {
    const fees = await rpc('getFeeData');
    return Object.fromEntries(['gasPrice', 'maxFeePerGas', 'maxPriorityFeePerGas'].map(key => [key, fees[key] === null || fees[key] === undefined ? null : ethers.formatUnits(fees[key], 'gwei')]));
  });
  route('post', 'broadcast', async (req, rpc) => {
    if (!config.broadcast) throw failure(403, 'Transaction broadcasting is disabled');
    if (typeof req.body.signedTx !== 'string' || !/^0x(?:[a-fA-F0-9]{2})+$/.test(req.body.signedTx)) throw failure(400, 'Signed transaction must be hexadecimal bytes');
    const tx = await rpc('broadcastTransaction', req.body.signedTx);
    return { hash: tx.hash };
  });
  route('post', 'estimate-gas', async (req, rpc) => {
    const { to, from, value, data } = req.body;
    if (value !== undefined && (typeof value !== 'string' || !/^\d+(\.\d{1,18})?$/.test(value))) throw failure(400, 'Invalid transaction value');
    if (data !== undefined && (typeof data !== 'string' || !/^0x(?:[a-fA-F0-9]{2})*$/.test(data))) throw failure(400, 'Invalid transaction data');
    const gas = await rpc('estimateGas', { to: address(to), ...(from ? { from: address(from) } : {}), value: ethers.parseEther(value || '0'), data });
    return { gasEstimate: gas.toString() };
  });
  const build = path.join(__dirname, '../frontend/build');
  if (fs.existsSync(path.join(build, 'index.html'))) {
    app.use(express.static(build));
    app.get('*', (req, res, next) => req.path.startsWith('/api/') ? next() : res.sendFile(path.join(build, 'index.html')));
  }
  app.use((req, res) => res.status(404).json({ error: 'Endpoint not found' }));
  app.use((error, req, res, next) => {
    const status = error.status >= 400 && error.status <= 599 ? error.status : 500;
    res.status(status).json({ error: status >= 500 && ![502, 503, 504].includes(status) ? 'Internal server error' : error.message });
  });
  return { app, store, config, close: async () => {
    if (redisClient?.isOpen) await redisClient.disconnect();
    for (const provider of Object.values(providers)) provider.destroy?.();
    if (!options.store) store.close();
  } };
}

if (require.main === module) {
  require('dotenv').config();
  const service = createApp();
  const server = service.app.listen(process.env.PORT || 3300, process.env.HOST || '127.0.0.1', () => console.log(`Blockchain API: http://localhost:${process.env.PORT || 3300} (${service.config.demo ? 'DEMO data' : 'live RPC'})`));
  const shutdown = () => server.close(() => service.close().then(() => process.exit(0)));
  process.once('SIGINT', shutdown);
  process.once('SIGTERM', shutdown);
}
module.exports = { createApp, readConfig, boundedCall };
