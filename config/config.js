require('dotenv').config();

const config = {
  hedera: {
    accountId: process.env.HEDERA_ACCOUNT_ID,
    privateKey: process.env.HEDERA_PRIVATE_KEY,
    network: process.env.HEDERA_NETWORK || 'testnet',
    timeout: 30000
  },
  postgres: {
    host: process.env.POSTGRES_HOST || 'localhost',
    port: parseInt(process.env.POSTGRES_PORT) || 5432,
    database: process.env.POSTGRES_DATABASE || 'hedera_db',
    user: process.env.POSTGRES_USER || 'postgres',
    password: process.env.POSTGRES_PASSWORD,
    maxConnections: 20,
    idleTimeout: 30000,
    connectionTimeout: 10000
  },
  app: {
    port: parseInt(process.env.PORT) || 3000,
    env: process.env.NODE_ENV || 'development'
  },
  syncEnabled: true,
  encryption: {
    enabled: false, // Enable when you implement encryption
    key: process.env.ENCRYPTION_KEY
  }
};

module.exports = config;