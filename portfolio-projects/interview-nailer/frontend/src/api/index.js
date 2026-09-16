import axios from 'axios';

const LOCAL_HOSTS = new Set(['localhost', '127.0.0.1', '::1']);

function isLocalHost(hostname = '') {
  return LOCAL_HOSTS.has(hostname.toLowerCase());
}

function resolveApiBase() {
  const configuredBase = (process.env.REACT_APP_API_BASE_URL || '/api').replace(/\/$/, '');

  if (typeof window === 'undefined' || !configuredBase.startsWith('http')) {
    return configuredBase;
  }

  try {
    const apiUrl = new URL(configuredBase);
    const pageHost = window.location.hostname;

    // If the app is opened from another device, "localhost" would point at that
    // device instead of this dev machine. In that case, rely on the CRA proxy.
    if (isLocalHost(apiUrl.hostname) && !isLocalHost(pageHost)) {
      return '/api';
    }
  } catch {
    // Fall back to the configured value if it is not a valid URL.
  }

  return configuredBase;
}

const API_BASE = resolveApiBase();
const API_BASE_HINT = (() => {
  if (typeof window !== 'undefined' && API_BASE === '/api') {
    return `${window.location.origin}/api`;
  }

  return API_BASE.startsWith('http') ? API_BASE : 'http://localhost:5000/api';
})();

const API = axios.create({ baseURL: API_BASE });

function isNetworkError(error) {
  return !error?.response && (error?.code === 'ERR_NETWORK' || error?.message === 'Network Error' || !!error?.request);
}

export function getApiErrorMessage(error, fallback = 'Request failed') {
  if (error?.response?.data?.error) {
    return error.response.data.error;
  }

  if (isNetworkError(error)) {
    return `Cannot reach the Interview Nailer backend. Make sure it is running and reachable at ${API_BASE_HINT}.`;
  }

  return error?.message || fallback;
}

API.interceptors.request.use((config) => {
  const token = localStorage.getItem('token');
  if (token) {
    config.headers.Authorization = `Bearer ${token}`;
  }
  return config;
});

API.interceptors.response.use(
  (response) => response,
  (error) => {
    if (error.response?.status === 401) {
      localStorage.removeItem('token');
      localStorage.removeItem('user');
      window.location.href = '/login';
    }

    if (isNetworkError(error)) {
      error.message = getApiErrorMessage(error, 'Request failed');
    }

    return Promise.reject(error);
  }
);

export const register = (data) => API.post('/auth/register', data);
export const login = (data) => API.post('/auth/login', data);

export const uploadResume = (formData) => API.post('/resume/upload', formData, {
  headers: { 'Content-Type': 'multipart/form-data' },
});
export const matchResume = (data) => API.post('/resume/match', data);
export const getResume = () => API.get('/resume');

export const generateAnswer = (data) => API.post('/answers/generate', data);
export const scoreAnswer = (data) => API.post('/answers/score', data);
export const getSalaryCoach = (data) => API.post('/answers/salary', data);

export const startSession = (data) => API.post('/sessions/start', data);
export const saveAnswer = (id, data) => API.post(`/sessions/${id}/answer`, data);
export const completeSession = (id) => API.post(`/sessions/${id}/complete`);
export const getSessions = () => API.get('/sessions');
export const getSession = (id) => API.get(`/sessions/${id}`);

export const streamAnswer = async (data, onChunk, onDone) => {
  const token = localStorage.getItem('token');
  const response = await fetch(`${API_BASE}/answers/generate/stream`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: token ? `Bearer ${token}` : '',
    },
    body: JSON.stringify(data),
  });

  const reader = response.body.getReader();
  const decoder = new TextDecoder();

  while (true) {
    const { done, value } = await reader.read();
    if (done) {
      break;
    }

    const chunk = decoder.decode(value);
    const lines = chunk.split('\n').filter((line) => line.startsWith('data:'));
    for (const line of lines) {
      const raw = line.replace('data: ', '').trim();
      if (raw === '[DONE]') {
        onDone();
        return;
      }

      try {
        onChunk(JSON.parse(raw).text);
      } catch {
        // Ignore malformed chunks.
      }
    }
  }
};

export default API;
