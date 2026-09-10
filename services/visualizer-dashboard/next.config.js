/** @type {import('next').NextConfig} */
const isProd = process.env.NODE_ENV === 'production';

const nextConfig = {
  reactStrictMode: false,
  output: 'export',
  basePath: isProd ? '/Distributed_Saga_Orchestrator' : '',
  assetPrefix: isProd ? '/Distributed_Saga_Orchestrator/' : '',
  images: {
    unoptimized: true,
  },
};

module.exports = nextConfig;
