import test from 'node:test';
import assert from 'node:assert/strict';
import { resolveApiBase } from '../src/api/base-url.mjs';

test('default and same-origin paths work for single-service deployments', () => {
  assert.equal(resolveApiBase('', 'https://interview.example'), '/api');
  assert.equal(resolveApiBase('/api/', 'http://localhost:3001'), '/api');
});
test('split-host deployments preserve the configured backend and API path', () => {
  assert.equal(resolveApiBase('https://backend.example/api/', 'https://frontend.example'), 'https://backend.example/api');
});
test('loopback backend addresses fall back only for remote visitors', () => {
  for (const address of ['http://localhost:5000/api', 'http://127.0.0.1:5000/api', 'http://[::1]:5000/api']) {
    assert.equal(resolveApiBase(address, 'https://interview.example'), '/api');
    assert.equal(resolveApiBase(address, 'http://localhost:3001'), address);
  }
});
test('invalid backend configuration cannot silently send tokens to an unintended origin', () => {
  for (const value of ['//backend.example/api', 'api', 'ftp://backend.example', 'https://user:secret@backend.example/api', 'https://backend.example/api?token=secret']) {
    assert.throws(() => resolveApiBase(value, 'https://frontend.example'), /REACT_APP_API_BASE_URL/);
  }
});
