// src/api/server.js
const express = require('express');
const { HederaAuditedDatabase } = require('../core/HederaAuditedDatabase');

const app = express();
app.use(express.json());

const db = new HederaAuditedDatabase(config);

// Initialize database
app.post('/api/init', async (req, res) => {
  try {
    const { dbName, schema } = req.body;
    const result = await db.initialize(dbName, schema);
    res.json(result);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Insert data
app.post('/api/:table', async (req, res) => {
  try {
    const { table } = req.params;
    const result = await db.insert(table, req.body);
    res.json(result);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Query data
app.get('/api/:table', async (req, res) => {
  try {
    const { table } = req.params;
    const results = await db.query(table, req.query);
    res.json(results);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Update data
app.put('/api/:table/:txId', async (req, res) => {
  try {
    const { table, txId } = req.params;
    const result = await db.update(table, txId, req.body);
    res.json(result);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Get audit trail
app.get('/api/:table/:txId/audit', async (req, res) => {
  try {
    const { table, txId } = req.params;
    const trail = await db.getAuditTrail(table, txId);
    res.json(trail);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Verify integrity
app.get('/api/:table/:txId/verify', async (req, res) => {
  try {
    const { table, txId } = req.params;
    const verification = await db.verifyIntegrity(table, txId);
    res.json(verification);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.listen(3000, () => {
  console.log('API server running on port 3000');
});