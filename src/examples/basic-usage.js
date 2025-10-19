const { HederaAuditedDatabase } = require('../core/HederaAuditedDatabase');
const config = require('../../config/config');

async function main() {
  console.log('🚀 Starting Hedera Decentralized Database Example...\n');

  // Initialize database instance
  const db = new HederaAuditedDatabase(config);

  try {
    // Step 1: Initialize database with schema
    console.log('Step 1: Initializing database...');
    
    const schema = {
      users: {
        id: { type: 'uuid', nullable: false },
        name: { type: 'string', nullable: false },
        email: { type: 'string', unique: true },
        age: { type: 'integer' },
        active: { type: 'boolean', default: 'TRUE' },
        created_date: { type: 'timestamp' }
      },
      orders: {
        id: { type: 'uuid', nullable: false },
        user_id: { type: 'uuid', nullable: false },
        amount: { type: 'number' },
        status: { type: 'string' },
        order_date: { type: 'timestamp' }
      }
    };

    const initResult = await db.initialize('MyDecentralizedDB', schema);
    console.log('✓ Database initialized:', initResult);
    console.log('✓ Topic ID:', initResult.topicId);
    console.log();

    // Step 2: Insert data
    console.log('Step 2: Inserting data...');
    
    const insertResult = await db.insert('users', {
      id: '550e8400-e29b-41d4-a716-446655440000',
      name: 'Alice Johnson',
      email: 'alice@example.com',
      age: 30,
      active: true,
      created_date: Date.now()
    }, {
      userId: 'admin',
      ipAddress: '192.168.1.1',
      source: 'api'
    });

    console.log('✓ Insert successful!');
    console.log('  TX ID:', insertResult.txId);
    console.log('  Data Hash:', insertResult.dataHash);
    console.log();

    // Step 3: Query data
    console.log('Step 3: Querying data...');
    
    const queryResult = await db.query('users', {
      where: { name: 'Alice Johnson' },
      orderBy: '_created_at DESC',
      limit: 10
    });

    console.log('✓ Query results:', queryResult.count, 'records found');
    console.log('  Data:', queryResult.data);
    console.log();

    // Step 4: Update data
    console.log('Step 4: Updating data...');
    
    const updateResult = await db.update(
      'users',
      insertResult.txId,
      {
        email: 'alice.updated@example.com',
        age: 31
      },
      { userId: 'admin', reason: 'User requested email change' }
    );

    console.log('✓ Update successful!');
    console.log('  New Version:', updateResult.version);
    console.log('  New Hash:', updateResult.newHash);
    console.log();

    // Step 5: Verify integrity
    console.log('Step 5: Verifying data integrity...');
    
    const verifyResult = await db.verifyIntegrity('users', insertResult.txId);
    
    console.log('✓ Integrity check:', verifyResult.valid ? 'PASSED' : 'FAILED');
    console.log('  Stored Hash:', verifyResult.storedHash);
    console.log('  Calculated Hash:', verifyResult.calculatedHash);
    console.log('  Match:', verifyResult.match);
    console.log();

    // Step 6: Get audit trail
    console.log('Step 6: Retrieving audit trail...');
    
    const auditResult = await db.getAuditTrail('users', insertResult.txId);
    
    console.log('✓ Audit trail:', auditResult.count, 'operations');
    auditResult.trail.forEach((entry, index) => {
      console.log(`  ${index + 1}. ${entry.operation} at ${new Date(parseInt(entry.hedera_timestamp)).toISOString()}`);
    });
    console.log();

    // Step 7: Advanced query with operators
    console.log('Step 7: Advanced query...');
    
    const advancedQuery = await db.query('users', {
      where: {
        age: { $gte: 25, $lte: 35 },
        active: true
      },
      orderBy: 'age DESC'
    });

    console.log('✓ Advanced query results:', advancedQuery.count, 'records');
    console.log();

    // Step 8: Get metrics
    console.log('Step 8: Database metrics...');
    
    const metrics = await db.getMetrics();
    console.log('✓ Metrics:', metrics);
    console.log();

    // Step 9: Listen for events
    console.log('Step 9: Setting up event listeners...');
    
    db.on('insert', (data) => {
      console.log('📥 INSERT event:', data.tableName, data.txId);
    });

    db.on('update', (data) => {
      console.log('📝 UPDATE event:', data.tableName, data.txId);
    });

    db.on('hedera-message', (data) => {
      console.log('🌐 Hedera message received:', data.operation.type);
    });

    console.log('✓ Event listeners registered');
    console.log();

    // Insert another record to trigger events
    await db.insert('users', {
      id: '660e8400-e29b-41d4-a716-446655440001',
      name: 'Bob Smith',
      email: 'bob@example.com',
      age: 28,
      active: true,
      created_date: Date.now()
    });

    console.log('\n🎉 All operations completed successfully!\n');

    // Keep the process running to receive Hedera messages
    console.log('⏳ Listening for Hedera messages... (Press Ctrl+C to exit)');

  } catch (error) {
    console.error('❌ Error:', error.message);
    console.error(error);
  }

  // Uncomment to close connections and exit
  // await db.close();
  // process.exit(0);
}

// Run the example
main();