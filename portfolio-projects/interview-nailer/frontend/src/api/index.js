import axios from 'axios';

import { resolveApiBase } from './base-url.mjs';

const API_BASE = resolveApiBase(process.env.REACT_APP_API_BASE_URL, typeof window === 'undefined' ? undefined : window.location.origin);
const API_BASE_HINT = (() => {
  if (typeof window !== 'undefined' && API_BASE === '/api') {
    return `${window.location.origin}/api`;
  }

  return API_BASE.startsWith('http') ? API_BASE : 'http://localhost:5000/api';
})();

const API = axios.create({ baseURL: API_BASE });
export const getServiceStatus = () => API.get('/status');

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

  if (!response.ok || !response.body) {
    const payload = await response.json().catch(() => ({}));
    throw new Error(payload.error || `Request failed (${response.status}).`);
  }
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let pending = '';

  while (true) {
    const { done, value } = await reader.read();
    if (done) {
      break;
    }

    pending += decoder.decode(value, { stream: true });
    const lines = pending.split('\n');
    pending = lines.pop();
    for (const line of lines) {
      if (!line.startsWith('data:')) continue;
      const raw = line.replace('data: ', '').trim();
      if (raw === '[DONE]') {
        onDone();
        await reader.cancel();
        return;
      }

      try {
        const data = JSON.parse(raw);
        if (typeof data.text === 'string') onChunk(data.text);
      } catch {
        // Ignore malformed chunks.
      }
    }
  }
  throw new Error('The answer stream ended before completion. Please try again.');
};

export default API;
