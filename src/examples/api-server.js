const express = require('express');
const { HederaAuditedDatabase } = require('../core/HederaAuditedDatabase');
const config = require('../../config/config');

const app = express();
app.use(express.json());

let db;

// Initialize database on server start
async function initializeServer() {
  db = new HederaAuditedDatabase(config);
  
  const schema = {
    users: {
      id: { type: 'uuid' },
      name: { type: 'string' },
      email: { type: 'string', unique: true },
      age: { type: 'integer' },
      active: { type: 'boolean' }
    }
  };

  try {
    // Try to connect to existing database or create new one
    await db.initialize('APIDatabase', schema, {
      existingTopicId: process.env.EXISTING_TOPIC_ID // Optional
    });
    console.log('✓ Database initialized for API');
  } catch (error) {
    console.error('Failed to initialize database:', error);
    process.exit(1);
  }
}

// Health check
app.get('/health', async (req, res) => {
  const metrics = await db.getMetrics();
  res.json({
    status: 'healthy',
    metrics: metrics
  });
});

// Create record
app.post('/api/:table', async (req, res) => {
  try {
    const { table } = req.params;
    const result = await db.insert(table, req.body, {
      userId: req.headers['x-user-id'],
      ipAddress: req.ip
    });
    res.status(201).json(result);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Query records
app.get('/api/:table', async (req, res) => {
  try {
    const { table } = req.params;
    const { limit, offset, orderBy, ...where } = req.query;
    
    const result = await db.query(table, {
      where,
      limit: limit ? parseInt(limit) : undefined,
      offset: offset ? parseInt(offset) : undefined,
      orderBy
    });
    
    res.json(result);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Update record
app.put('/api/:table/:txId', async (req, res) => {
  try {
    const { table, txId } = req.params;
    const result = await db.update(table, txId, req.body, {
      userId: req.headers['x-user-id'],
      ipAddress: req.ip
    });
    res.json(result);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Delete record
app.delete('/api/:table/:txId', async (req, res) => {
  try {
    const { table, txId } = req.params;
    const hardDelete = req.query.hard === 'true';
    
    const result = await db.delete(table, txId, {
      userId: req.headers['x-user-id'],
      ipAddress: req.ip
    }, hardDelete);
    
    res.json(result);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Get audit trail
app.get('/api/:table/:txId/audit', async (req, res) => {
  try {
    const { table, txId } = req.params;
    const result = await db.getAuditTrail(table, txId);
    res.json(result);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Verify integrity
app.get('/api/:table/:txId/verify', async (req, res) => {
  try {
    const { table, txId } = req.params;
    const result = await db.verifyIntegrity(table, txId);
    res.json(result);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Start server
initializeServer().then(() => {
  const PORT = config.app.port;
  app.listen(PORT, () => {
    console.log(`🚀 API Server running on http://localhost:${PORT}`);
    console.log(`\nAvailable endpoints:`);
    console.log(`  GET  /health`);
    console.log(`  POST /api/:table`);
    console.log(`  GET  /api/:table`);
    console.log(`  PUT  /api/:table/:txId`);
    console.log(`  DELETE /api/:table/:txId`);
    console.log(`  GET  /api/:table/:txId/audit`);
    console.log(`  GET  /api/:table/:txId/verify`);
  });
});

// Graceful shutdown
process.on('SIGINT', async () => {
  console.log('\n\nShutting down gracefully...');
  await db.close();
  process.exit(0);
});