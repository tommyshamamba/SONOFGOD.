import React, { useState, useEffect } from 'react';

function App() {
  const [data, setData] = useState(null);
  const [config, setConfig] = useState(null);
  const [secret, setSecret] = useState(null);
  const [loading, setLoading] = useState(true);

  const API_URL = process.env.REACT_APP_API_URL || 'http://localhost:3000';

  useEffect(() => {
    fetchData();
    fetchConfig();
    fetchSecret();
  }, []);

  const fetchData = async () => {
    try {
      const response = await fetch(`${API_URL}/api/data`);
      const json = await response.json();
      setData(json);
    } catch (error) {
      console.error('Error fetching data:', error);
    }
  };

  const fetchConfig = async () => {
    try {
      const response = await fetch(`${API_URL}/api/config`);
      const json = await response.json();
      setConfig(json);
    } catch (error) {
      console.error('Error fetching config:', error);
    }
  };

  const fetchSecret = async () => {
    try {
      const response = await fetch(`${API_URL}/api/secret`);
      const json = await response.json();
      setSecret(json);
    } catch (error) {
      console.error('Error fetching secret:', error);
    } finally {
      setLoading(false);
    }
  };

  if (loading) {
    return <div style={{ padding: '20px', textAlign: 'center' }}>Loading...</div>;
  }

  return (
    <div style={{ padding: '20px', fontFamily: 'Arial, sans-serif', maxWidth: '800px', margin: '0 auto' }}>
      <h1 style={{ color: '#326ce5' }}>Kubernetes Demo Application</h1>
      
      <div style={{ marginBottom: '30px' }}>
        <h2 style={{ borderBottom: '2px solid #326ce5', paddingBottom: '10px' }}>API Data</h2>
        {data ? (
          <pre style={{ background: '#f4f4f4', padding: '15px', borderRadius: '5px' }}>
            {JSON.stringify(data, null, 2)}
          </pre>
        ) : (
          <p>No data available</p>
        )}
      </div>

      <div style={{ marginBottom: '30px' }}>
        <h2 style={{ borderBottom: '2px solid #326ce5', paddingBottom: '10px' }}>ConfigMap Data</h2>
        {config ? (
          <pre style={{ background: '#f4f4f4', padding: '15px', borderRadius: '5px' }}>
            {JSON.stringify(config, null, 2)}
          </pre>
        ) : (
          <p>No config available</p>
        )}
      </div>

      <div style={{ marginBottom: '30px' }}>
        <h2 style={{ borderBottom: '2px solid #326ce5', paddingBottom: '10px' }}>Secret Data (Masked)</h2>
        {secret ? (
          <pre style={{ background: '#f4f4f4', padding: '15px', borderRadius: '5px' }}>
            {JSON.stringify(secret, null, 2)}
          </pre>
        ) : (
          <p>No secret data available</p>
        )}
      </div>

      <button 
        onClick={() => { fetchData(); fetchConfig(); fetchSecret(); }}
        style={{
          background: '#326ce5',
          color: 'white',
          border: 'none',
          padding: '10px 20px',
          borderRadius: '5px',
          cursor: 'pointer',
          fontSize: '16px'
        }}
      >
        Refresh Data
      </button>
    </div>
  );
}

export default App;
