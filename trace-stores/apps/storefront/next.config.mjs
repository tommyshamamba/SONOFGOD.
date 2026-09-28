const localThreads = process.env.BUILD_WITH_THREADS === "1";

export default {
  // Optional mode for constrained Windows environments that cannot fork workers.
  ...(localThreads ? { experimental: { workerThreads: true, webpackBuildWorker: false, cpus: 2 } } : {}),
};
