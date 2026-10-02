import { defineConfig, loadEnv } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), 'REACT_APP_');
  return {
    plugins: [react()],
    // No linked packages: preserve paths and avoid Windows drive-probing subprocesses.
    resolve: { preserveSymlinks: true },
    define: {
      'process.env.REACT_APP_API_URL': JSON.stringify(env.REACT_APP_API_URL || ''),
      'process.env.REACT_APP_API_BASE_URL': JSON.stringify(env.REACT_APP_API_BASE_URL || '/api'),
    },
    build: { outDir: 'build' },
    server: {
      port: 3003,
      strictPort: true,
      proxy: { '/api': 'http://127.0.0.1:3200', '/health': 'http://127.0.0.1:3200' },
    },
  };
});
