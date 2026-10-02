const LOCAL_HOSTS = new Set(['localhost', '127.0.0.1', '[::1]', '::1']);

export function resolveApiBase(configured, pageOrigin) {
  const base = (configured || '/api').trim().replace(/\/+$/, '') || '/api';
  if (base.startsWith('/') && !base.startsWith('//')) return base;
  let url;
  try { url = new URL(base); }
  catch { throw new Error('REACT_APP_API_BASE_URL must be an absolute HTTP(S) URL or a same-origin path.'); }
  if (!['http:', 'https:'].includes(url.protocol) || url.username || url.password || url.search || url.hash) {
    throw new Error('REACT_APP_API_BASE_URL must not include credentials, query parameters, or fragments.');
  }
  // A remote visitor cannot reach the developer's loopback address.
  if (pageOrigin && LOCAL_HOSTS.has(url.hostname) && !LOCAL_HOSTS.has(new URL(pageOrigin).hostname)) return '/api';
  return url.href.replace(/\/+$/, '');
}
