import React, { useState, useEffect } from 'react';
import axios from 'axios';

const API_URL = process.env.REACT_APP_API_URL || 'http://localhost:3000';

function App() {
  const [token, setToken] = useState(localStorage.getItem('token'));
  const [view, setView] = useState('login');
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [apiKeys, setApiKeys] = useState([]);
  const [newKeyName, setNewKeyName] = useState('');
  const [balanceData, setBalanceData] = useState(null);
  const [testAddress, setTestAddress] = useState('');
  const [testChain, setTestChain] = useState('ethereum');
  const [message, setMessage] = useState('');

  useEffect(() => {
    if (token) {
      fetchApiKeys();
    }
  }, [token]);

  const showMessage = (msg, type = 'info') => {
    setMessage({ text: msg, type });
    setTimeout(() => setMessage(null), 3000);
  };

  // Auth functions
  const register = async () => {
    try {
      const res = await axios.post(`${API_URL}/api/auth/register`, { email, password });
      localStorage.setItem('token', res.data.token);
      setToken(res.data.token);
      setView('dashboard');
      showMessage('Registration successful!', 'success');
    } catch (error) {
      showMessage(error.response?.data?.error || 'Registration failed', 'error');
    }
  };

  const login = async () => {
    try {
      const res = await axios.post(`${API_URL}/api/auth/login`, { email, password });
      localStorage.setItem('token', res.data.token);
      setToken(res.data.token);
      setView('dashboard');
      showMessage('Login successful!', 'success');
    } catch (error) {
      showMessage(error.response?.data?.error || 'Login failed', 'error');
    }
  };

  const logout = () => {
    localStorage.removeItem('token');
    setToken(null);
    setView('login');
    setApiKeys([]);
  };

  // API Key functions
  const fetchApiKeys = async () => {
    try {
      const res = await axios.get(`${API_URL}/api/keys`, {
        headers: { Authorization: `Bearer ${token}` }
      });
      setApiKeys(res.data.keys);
    } catch (error) {
      showMessage('Failed to fetch API keys', 'error');
    }
  };

  const createApiKey = async () => {
    try {
      const res = await axios.post(`${API_URL}/api/keys`, 
        { name: newKeyName },
        { headers: { Authorization: `Bearer ${token}` } }
      );
      showMessage(`API Key created: ${res.data.apiKey}`, 'success');
      setNewKeyName('');
      fetchApiKeys();
    } catch (error) {
      showMessage(error.response?.data?.error || 'Failed to create API key', 'error');
    }
  };

  const revokeApiKey = async (keyId) => {
    try {
      await axios.delete(`${API_URL}/api/keys/${keyId}`, {
        headers: { Authorization: `Bearer ${token}` }
      });
      showMessage('API key revoked', 'success');
      fetchApiKeys();
    } catch (error) {
      showMessage('Failed to revoke API key', 'error');
    }
  };

  // Test API function
  const testBalance = async () => {
    if (!apiKeys.length) {
      showMessage('Create an API key first', 'error');
      return;
    }
    
    try {
      const fullKey = apiKeys[0].key.replace('...', '').substring(0, 32) + apiKeys[0].key.substring(32).replace('...', '');
      const res = await axios.get(
        `${API_URL}/api/v1/${testChain}/balance/${testAddress}`,
        { headers: { 'X-API-Key': apiKeys[0].fullKey || 'test' } }
      );
      setBalanceData(res.data);
      showMessage('Balance fetched successfully', 'success');
    } catch (error) {
      showMessage(error.response?.data?.error || 'Failed to fetch balance', 'error');
    }
  };

  // Render login/register
  if (!token) {
    return (
      <div style={{ maxWidth: '400px', margin: '100px auto', padding: '20px', fontFamily: 'Arial, sans-serif' }}>
        <h1 style={{ color: '#6366f1', textAlign: 'center' }}>Blockchain API</h1>
        
        {message && (
          <div style={{ 
            padding: '10px', 
            marginBottom: '20px', 
            borderRadius: '5px',
            background: message.type === 'error' ? '#fee2e2' : '#d1fae5',
            color: message.type === 'error' ? '#dc2626' : '#059669'
          }}>
            {message.text}
          </div>
        )}

        <div style={{ marginBottom: '20px' }}>
          <label style={{ display: 'block', marginBottom: '5px' }}>Email</label>
          <input
            type="email"
            value={email}
            onChange={(e) => setEmail(e.target.value)}
            style={{ width: '100%', padding: '10px', border: '1px solid #d1d5db', borderRadius: '5px' }}
          />
        </div>

        <div style={{ marginBottom: '20px' }}>
          <label style={{ display: 'block', marginBottom: '5px' }}>Password</label>
          <input
            type="password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            style={{ width: '100%', padding: '10px', border: '1px solid #d1d5db', borderRadius: '5px' }}
          />
        </div>

        <div style={{ display: 'flex', gap: '10px' }}>
          <button
            onClick={login}
            style={{ 
              flex: 1, 
              padding: '10px', 
              background: '#6366f1', 
              color: 'white', 
              border: 'none', 
              borderRadius: '5px',
              cursor: 'pointer'
            }}
          >
            Login
          </button>
          <button
            onClick={register}
            style={{ 
              flex: 1, 
              padding: '10px', 
              background: '#10b981', 
              color: 'white', 
              border: 'none', 
              borderRadius: '5px',
              cursor: 'pointer'
            }}
          >
            Register
          </button>
        </div>
      </div>
    );
  }

  // Render dashboard
  return (
    <div style={{ padding: '20px', fontFamily: 'Arial, sans-serif', maxWidth: '1200px', margin: '0 auto' }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '30px' }}>
        <h1 style={{ color: '#6366f1', margin: 0 }}>Blockchain API Dashboard</h1>
        <button
          onClick={logout}
          style={{ padding: '10px 20px', background: '#ef4444', color: 'white', border: 'none', borderRadius: '5px', cursor: 'pointer' }}
        >
          Logout
        </button>
      </div>

      {message && (
        <div style={{ 
          padding: '10px', 
          marginBottom: '20px', 
          borderRadius: '5px',
          background: message.type === 'error' ? '#fee2e2' : '#d1fae5',
          color: message.type === 'error' ? '#dc2626' : '#059669'
        }}>
          {message.text}
        </div>
      )}

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '20px' }}>
        {/* API Keys Section */}
        <div style={{ background: '#f9fafb', padding: '20px', borderRadius: '10px' }}>
          <h2 style={{ marginTop: 0 }}>API Keys</h2>
          
          <div style={{ marginBottom: '20px' }}>
            <input
              type="text"
              placeholder="Key name (e.g., Production)"
              value={newKeyName}
              onChange={(e) => setNewKeyName(e.target.value)}
              style={{ width: '70%', padding: '10px', border: '1px solid #d1d5db', borderRadius: '5px', marginRight: '10px' }}
            />
            <button
              onClick={createApiKey}
              style={{ padding: '10px 20px', background: '#10b981', color: 'white', border: 'none', borderRadius: '5px', cursor: 'pointer' }}
            >
              Create Key
            </button>
          </div>

          {apiKeys.length === 0 ? (
            <p style={{ color: '#6b7280' }}>No API keys yet. Create one to get started.</p>
          ) : (
            <div style={{ maxHeight: '300px', overflowY: 'auto' }}>
              {apiKeys.map((key, index) => (
                <div key={index} style={{ background: 'white', padding: '15px', borderRadius: '5px', marginBottom: '10px', border: '1px solid #e5e7eb' }}>
                  <div style={{ fontWeight: 'bold', marginBottom: '5px' }}>{key.name}</div>
                  <div style={{ fontSize: '12px', color: '#6b7280', marginBottom: '5px' }}>{key.key}</div>
                  <div style={{ fontSize: '12px', color: '#6b7280', marginBottom: '5px' }}>
                    Requests: {key.requests} | Created: {new Date(key.createdAt).toLocaleDateString()}
                  </div>
                  {!key.revoked && (
                    <button
                      onClick={() => revokeApiKey(key.key)}
                      style={{ padding: '5px 10px', background: '#ef4444', color: 'white', border: 'none', borderRadius: '3px', cursor: 'pointer', fontSize: '12px' }}
                    >
                      Revoke
                    </button>
                  )}
                  {key.revoked && (
                    <span style={{ color: '#ef4444', fontSize: '12px', fontWeight: 'bold' }}>REVOKED</span>
                  )}
                </div>
              ))}
            </div>
          )}
        </div>

        {/* API Test Section */}
        <div style={{ background: '#f9fafb', padding: '20px', borderRadius: '10px' }}>
          <h2 style={{ marginTop: 0 }}>Test API</h2>
          
          <div style={{ marginBottom: '15px' }}>
            <label style={{ display: 'block', marginBottom: '5px' }}>Chain</label>
            <select
              value={testChain}
              onChange={(e) => setTestChain(e.target.value)}
              style={{ width: '100%', padding: '10px', border: '1px solid #d1d5db', borderRadius: '5px' }}
            >
              <option value="ethereum">Ethereum</option>
              <option value="polygon">Polygon</option>
              <option value="arbitrum">Arbitrum</option>
              <option value="optimism">Optimism</option>
            </select>
          </div>

          <div style={{ marginBottom: '15px' }}>
            <label style={{ display: 'block', marginBottom: '5px' }}>Wallet Address</label>
            <input
              type="text"
              placeholder="0x..."
              value={testAddress}
              onChange={(e) => setTestAddress(e.target.value)}
              style={{ width: '100%', padding: '10px', border: '1px solid #d1d5db', borderRadius: '5px' }}
            />
          </div>

          <button
            onClick={testBalance}
            style={{ padding: '10px 20px', background: '#6366f1', color: 'white', border: 'none', borderRadius: '5px', cursor: 'pointer', marginBottom: '20px' }}
          >
            Get Balance
          </button>

          {balanceData && (
            <div style={{ background: 'white', padding: '15px', borderRadius: '5px', border: '1px solid #e5e7eb' }}>
              <h3 style={{ marginTop: 0 }}>Balance Result</h3>
              <pre style={{ background: '#f3f4f6', padding: '10px', borderRadius: '5px', overflow: 'auto' }}>
                {JSON.stringify(balanceData, null, 2)}
              </pre>
            </div>
          )}
        </div>
      </div>

      {/* API Documentation */}
      <div style={{ marginTop: '30px', background: '#f9fafb', padding: '20px', borderRadius: '10px' }}>
        <h2 style={{ marginTop: 0 }}>API Endpoints</h2>
        <div style={{ background: 'white', padding: '15px', borderRadius: '5px' }}>
          <pre style={{ margin: 0, fontSize: '13px' }}>
{`GET    /api/v1/:chain/balance/:address
GET    /api/v1/:chain/nonce/:address
GET    /api/v1/:chain/block/:blockNumber
GET    /api/v1/:chain/transaction/:txHash
GET    /api/v1/:chain/gas-price
POST   /api/v1/:chain/broadcast
POST   /api/v1/:chain/estimate-gas
GET    /api/v1/chains
GET    /api/v1/usage

Headers:
  X-API-Key: your-api-key

Supported chains: ethereum, polygon, arbitrum, optimism`}
          </pre>
        </div>
      </div>
    </div>
  );
}

export default App;
