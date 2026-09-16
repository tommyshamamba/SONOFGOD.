const express = require('express');
const cors = require('cors');
const { ethers } = require('ethers');
const axios = require('axios');
const { v4: uuidv4 } = require('uuid');
const redis = require('redis');
const { RateLimiterRedis } = require('rate-limiter-flexible');
const jwt = require('jsonwebtoken');
const bcrypt = require('bcryptjs');
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.json());

// Redis client for rate limiting and caching
let redisClient;
if (process.env.REDIS_URL) {
  redisClient = redis.createClient({ url: process.env.REDIS_URL });
  redisClient.connect().catch(console.error);
}

// Rate limiter
const rateLimiter = new RateLimiterRedis({
  storeClient: redisClient,
  keyPrefix: 'rate_limit',
  points: 100,
  duration: 60,
});

// In-memory API key storage (use database in production)
const apiKeys = new Map();
const users = new Map();

// JWT secret
const JWT_SECRET = process.env.JWT_SECRET || 'your-secret-key-change-in-production';

// Blockchain RPC endpoints (use public nodes or your own)
const RPC_ENDPOINTS = {
  ethereum: process.env.ETH_RPC_URL || 'https://eth.llamarpc.com',
  polygon: process.env.POLYGON_RPC_URL || 'https://polygon.llamarpc.com',
  arbitrum: process.env.ARBITRUM_RPC_URL || 'https://arbitrum.llamarpc.com',
  optimism: process.env.OPTIMISM_RPC_URL || 'https://optimism.llamarpc.com',
};

// Provider for each chain
const providers = {};
Object.entries(RPC_ENDPOINTS).forEach(([chain, url]) => {
  providers[chain] = new ethers.JsonRpcProvider(url);
});

// Middleware: API Key Authentication
const authenticateApiKey = async (req, res, next) => {
  const apiKey = req.headers['x-api-key'];
  
  if (!apiKey) {
    return res.status(401).json({ error: 'API key required' });
  }

  const keyData = apiKeys.get(apiKey);
  if (!keyData) {
    return res.status(401).json({ error: 'Invalid API key' });
  }

  if (keyData.revoked) {
    return res.status(401).json({ error: 'API key revoked' });
  }

  // Rate limiting
  try {
    await rateLimiter.consume(apiKey);
  } catch (rateLimiterRes) {
    return res.status(429).json({ 
      error: 'Rate limit exceeded',
      retryAfter: Math.round(rateLimiterRes.msBeforeNext / 1000)
    });
  }

  req.apiKey = apiKey;
  req.userId = keyData.userId;
  next();
};

// Middleware: JWT Authentication
const authenticateJwt = (req, res, next) => {
  const token = req.headers['authorization']?.split(' ')[1];
  
  if (!token) {
    return res.status(401).json({ error: 'Token required' });
  }

  try {
    const decoded = jwt.verify(token, JWT_SECRET);
    req.userId = decoded.userId;
    next();
  } catch (error) {
    return res.status(401).json({ error: 'Invalid token' });
  }
};

// Health check
app.get('/health', (req, res) => {
  res.json({ 
    status: 'healthy', 
    timestamp: new Date().toISOString(),
    chains: Object.keys(RPC_ENDPOINTS)
  });
});

// ===== USER MANAGEMENT =====

// Register user
app.post('/api/auth/register', async (req, res) => {
  try {
    const { email, password } = req.body;
    
    if (!email || !password) {
      return res.status(400).json({ error: 'Email and password required' });
    }

    if (users.has(email)) {
      return res.status(400).json({ error: 'User already exists' });
    }

    const hashedPassword = await bcrypt.hash(password, 10);
    const userId = uuidv4();
    
    users.set(email, {
      userId,
      email,
      password: hashedPassword,
      createdAt: new Date().toISOString()
    });

    const token = jwt.sign({ userId, email }, JWT_SECRET, { expiresIn: '7d' });

    res.json({ 
      message: 'User registered successfully',
      token,
      userId
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Login
app.post('/api/auth/login', async (req, res) => {
  try {
    const { email, password } = req.body;
    
    const user = users.get(email);
    if (!user) {
      return res.status(401).json({ error: 'Invalid credentials' });
    }

    const validPassword = await bcrypt.compare(password, user.password);
    if (!validPassword) {
      return res.status(401).json({ error: 'Invalid credentials' });
    }

    const token = jwt.sign({ userId: user.userId, email: user.email }, JWT_SECRET, { expiresIn: '7d' });

    res.json({ 
      message: 'Login successful',
      token,
      userId: user.userId
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Generate API key
app.post('/api/keys', authenticateJwt, (req, res) => {
  const { name } = req.body;
  const apiKey = `bk_${uuidv4().replace(/-/g, '')}`;
  
  apiKeys.set(apiKey, {
    userId: req.userId,
    name: name || 'Default Key',
    createdAt: new Date().toISOString(),
    revoked: false,
    requests: 0
  });

  res.json({ 
    message: 'API key created',
    apiKey,
    name
  });
});

// List API keys
app.get('/api/keys', authenticateJwt, (req, res) => {
  const userKeys = [];
  apiKeys.forEach((data, key) => {
    if (data.userId === req.userId) {
      userKeys.push({
        key: key.substring(0, 12) + '...',
        name: data.name,
        createdAt: data.createdAt,
        requests: data.requests,
        revoked: data.revoked
      });
    }
  });

  res.json({ keys: userKeys });
});

// Revoke API key
app.delete('/api/keys/:keyId', authenticateJwt, (req, res) => {
  const { keyId } = req.params;
  
  // Find full key by partial match
  let found = false;
  apiKeys.forEach((data, key) => {
    if (key.startsWith(keyId) && data.userId === req.userId) {
      data.revoked = true;
      found = true;
    }
  });

  if (found) {
    res.json({ message: 'API key revoked' });
  } else {
    res.status(404).json({ error: 'API key not found' });
  }
});

// ===== BLOCKCHAIN API ENDPOINTS =====

// Get balance
app.get('/api/v1/:chain/balance/:address', authenticateApiKey, async (req, res) => {
  try {
    const { chain, address } = req.params;
    
    if (!providers[chain]) {
      return res.status(400).json({ error: 'Unsupported chain' });
    }

    const balance = await providers[chain].getBalance(address);
    const formattedBalance = ethers.formatEther(balance);

    // Update request count
    const keyData = apiKeys.get(req.apiKey);
    if (keyData) keyData.requests++;

    res.json({
      chain,
      address,
      balance: formattedBalance,
      wei: balance.toString()
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Get transaction count (nonce)
app.get('/api/v1/:chain/nonce/:address', authenticateApiKey, async (req, res) => {
  try {
    const { chain, address } = req.params;
    
    if (!providers[chain]) {
      return res.status(400).json({ error: 'Unsupported chain' });
    }

    const nonce = await providers[chain].getTransactionCount(address);

    const keyData = apiKeys.get(req.apiKey);
    if (keyData) keyData.requests++;

    res.json({
      chain,
      address,
      nonce
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Get block
app.get('/api/v1/:chain/block/:blockNumber', authenticateApiKey, async (req, res) => {
  try {
    const { chain, blockNumber } = req.params;
    
    if (!providers[chain]) {
      return res.status(400).json({ error: 'Unsupported chain' });
    }

    const block = await providers[chain].getBlock(parseInt(blockNumber));

    const keyData = apiKeys.get(req.apiKey);
    if (keyData) keyData.requests++;

    res.json({
      chain,
      blockNumber: block.number,
      hash: block.hash,
      timestamp: block.timestamp,
      transactions: block.transactions.length,
      gasUsed: block.gasUsed.toString(),
      gasLimit: block.gasLimit.toString()
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Get transaction
app.get('/api/v1/:chain/transaction/:txHash', authenticateApiKey, async (req, res) => {
  try {
    const { chain, txHash } = req.params;
    
    if (!providers[chain]) {
      return res.status(400).json({ error: 'Unsupported chain' });
    }

    const tx = await providers[chain].getTransaction(txHash);
    const receipt = await providers[chain].getTransactionReceipt(txHash);

    const keyData = apiKeys.get(req.apiKey);
    if (keyData) keyData.requests++;

    res.json({
      chain,
      hash: tx.hash,
      from: tx.from,
      to: tx.to,
      value: ethers.formatEther(tx.value),
      gasUsed: receipt?.gasUsed.toString(),
      status: receipt?.status === 1 ? 'success' : 'failed',
      blockNumber: tx.blockNumber
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Get gas price
app.get('/api/v1/:chain/gas-price', authenticateApiKey, async (req, res) => {
  try {
    const { chain } = req.params;
    
    if (!providers[chain]) {
      return res.status(400).json({ error: 'Unsupported chain' });
    }

    const feeData = await providers[chain].getFeeData();

    const keyData = apiKeys.get(req.apiKey);
    if (keyData) keyData.requests++;

    res.json({
      chain,
      gasPrice: feeData.gasPrice ? ethers.formatUnits(feeData.gasPrice, 'gwei') : null,
      maxFeePerGas: feeData.maxFeePerGas ? ethers.formatUnits(feeData.maxFeePerGas, 'gwei') : null,
      maxPriorityFeePerGas: feeData.maxPriorityFeePerGas ? ethers.formatUnits(feeData.maxPriorityFeePerGas, 'gwei') : null
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Broadcast transaction
app.post('/api/v1/:chain/broadcast', authenticateApiKey, async (req, res) => {
  try {
    const { chain } = req.params;
    const { signedTx } = req.body;
    
    if (!providers[chain]) {
      return res.status(400).json({ error: 'Unsupported chain' });
    }

    if (!signedTx) {
      return res.status(400).json({ error: 'Signed transaction required' });
    }

    const tx = await providers[chain].broadcastTransaction(signedTx);

    const keyData = apiKeys.get(req.apiKey);
    if (keyData) keyData.requests++;

    res.json({
      chain,
      hash: tx.hash,
      message: 'Transaction broadcasted successfully'
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Estimate gas
app.post('/api/v1/:chain/estimate-gas', authenticateApiKey, async (req, res) => {
  try {
    const { chain } = req.params;
    const { to, from, value, data } = req.body;
    
    if (!providers[chain]) {
      return res.status(400).json({ error: 'Unsupported chain' });
    }

    const gasEstimate = await providers[chain].estimateGas({
      to,
      from,
      value: value ? ethers.parseEther(value) : '0',
      data
    });

    const keyData = apiKeys.get(req.apiKey);
    if (keyData) keyData.requests++;

    res.json({
      chain,
      gasEstimate: gasEstimate.toString()
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Get supported chains
app.get('/api/v1/chains', (req, res) => {
  res.json({
    chains: Object.keys(RPC_ENDPOINTS),
    endpoints: RPC_ENDPOINTS
  });
});

// API usage stats
app.get('/api/v1/usage', authenticateApiKey, (req, res) => {
  const keyData = apiKeys.get(req.apiKey);
  
  res.json({
    totalRequests: keyData?.requests || 0,
    rateLimit: {
      points: 100,
      duration: 60
    }
  });
});

app.listen(PORT, () => {
  console.log(`Blockchain API Service running on port ${PORT}`);
  console.log(`Supported chains: ${Object.keys(RPC_ENDPOINTS).join(', ')}`);
});
