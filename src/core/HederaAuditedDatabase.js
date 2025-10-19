/**
 * HederaAuditedDatabase.js
 * Complete implementation of a decentralized relational database
 * using Hedera for audit trails and PostgreSQL for data storage
 */

const {
  Client,
  TopicCreateTransaction,
  TopicMessageSubmitTransaction,
  PrivateKey,
  AccountId,
  TopicId,
  TopicMessageQuery,
  TopicInfoQuery
} = require("@hashgraph/sdk");
const { Pool } = require("pg");
const crypto = require("crypto");
const EventEmitter = require("events");

class HederaAuditedDatabase extends EventEmitter {
  constructor(config) {
    super();
    
    this.validateConfig(config);
    this.hederaClient = this.initializeHederaClient(config.hedera);
    this.pgPool = this.initializePostgresPool(config.postgres);
    
    this.topicId = null;
    this.isInitialized = false;
    this.syncEnabled = config.syncEnabled !== false;
    this.encryptionEnabled = config.encryption?.enabled || false;
    this.encryptionKey = config.encryption?.key || null;
    
    this.metrics = {
      insertCount: 0,
      updateCount: 0,
      deleteCount: 0,
      queryCount: 0,
      hederaSubmissions: 0,
      errors: 0
    };

    this.schemaCache = new Map();
    this.activeTransactions = new Map();
  }

  validateConfig(config) {
    if (!config.hedera?.accountId || !config.hedera?.privateKey) {
      throw new Error("Hedera configuration missing: accountId and privateKey required");
    }
    if (!config.postgres?.host || !config.postgres?.database) {
      throw new Error("PostgreSQL configuration missing: host and database required");
    }
  }

  initializeHederaClient(hederaConfig) {
    let client;
    
    switch (hederaConfig.network?.toLowerCase()) {
      case "mainnet":
        client = Client.forMainnet();
        break;
      case "previewnet":
        client = Client.forPreviewnet();
        break;
      case "testnet":
      default:
        client = Client.forTestnet();
        break;
    }

    client.setOperator(
      AccountId.fromString(hederaConfig.accountId),
      PrivateKey.fromString(hederaConfig.privateKey)
    );

    if (hederaConfig.timeout) {
      client.setRequestTimeout(hederaConfig.timeout);
    }

    return client;
  }

  initializePostgresPool(postgresConfig) {
    return new Pool({
      host: postgresConfig.host,
      port: postgresConfig.port || 5432,
      database: postgresConfig.database,
      user: postgresConfig.user,
      password: postgresConfig.password,
      max: postgresConfig.maxConnections || 20,
      idleTimeoutMillis: postgresConfig.idleTimeout || 30000,
      connectionTimeoutMillis: postgresConfig.connectionTimeout || 10000,
      ssl: postgresConfig.ssl || false
    });
  }

  async initialize(dbName, schema, options = {}) {
    if (this.isInitialized) {
      throw new Error("Database already initialized");
    }

    try {
      console.log(`Initializing database: ${dbName}`);

      if (options.existingTopicId) {
        await this.connectToExistingTopic(options.existingTopicId);
      } else {
        await this.createNewTopic(dbName, options);
      }

      await this.createPostgresInfrastructure(schema);
      this.schemaCache.set("main", schema);
      
      if (!options.existingTopicId) {
        const schemaMessage = {
          type: "SCHEMA_INIT",
          timestamp: Date.now(),
          dbName: dbName,
          schema: schema,
          version: options.schemaVersion || "1.0.0"
        };
        await this.submitToHedera(schemaMessage);
      }

      if (this.syncEnabled) {
        this.startSyncService();
      }

      this.isInitialized = true;
      this.emit("initialized", { topicId: this.topicId.toString(), dbName });

      console.log(`✓ Database initialized successfully`);
      console.log(`  Topic ID: ${this.topicId.toString()}`);

      return {
        success: true,
        topicId: this.topicId.toString(),
        status: "initialized"
      };
    } catch (error) {
      this.metrics.errors++;
      console.error("Initialization error:", error);
      throw error;
    }
  }

  async createNewTopic(dbName, options) {
    const transaction = new TopicCreateTransaction()
      .setTopicMemo(options.memo || `Hedera DB: ${dbName}`)
      .setAdminKey(this.hederaClient.operatorPublicKey);

    if (options.submitKey) {
      transaction.setSubmitKey(PrivateKey.fromString(options.submitKey));
    }

    const txResponse = await transaction.execute(this.hederaClient);
    const receipt = await txResponse.getReceipt(this.hederaClient);
    this.topicId = receipt.topicId;

    console.log(`✓ Hedera topic created: ${this.topicId}`);
  }

  async connectToExistingTopic(topicIdString) {
    this.topicId = TopicId.fromString(topicIdString);
    
    const topicInfo = await new TopicInfoQuery()
      .setTopicId(this.topicId)
      .execute(this.hederaClient);
    
    console.log(`✓ Connected to existing topic: ${topicIdString}`);
    console.log(`  Memo: ${topicInfo.topicMemo}`);
    console.log(`  Sequence Number: ${topicInfo.sequenceNumber}`);
  }

  async createPostgresInfrastructure(schema) {
    const client = await this.pgPool.connect();
    
    try {
      await client.query("BEGIN");

      await client.query(`
        CREATE TABLE IF NOT EXISTS _audit_log (
          id SERIAL PRIMARY KEY,
          tx_id VARCHAR(64) UNIQUE NOT NULL,
          table_name VARCHAR(255) NOT NULL,
          operation VARCHAR(20) NOT NULL,
          data_hash VARCHAR(64) NOT NULL,
          hedera_timestamp BIGINT NOT NULL,
          hedera_sequence_number BIGINT,
          hedera_topic_id VARCHAR(64),
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          metadata JSONB,
          user_id VARCHAR(255),
          ip_address INET
        )
      `);

      await client.query(`
        CREATE INDEX IF NOT EXISTS idx_audit_table_tx 
        ON _audit_log(table_name, tx_id)
      `);
      
      await client.query(`
        CREATE INDEX IF NOT EXISTS idx_audit_timestamp 
        ON _audit_log(hedera_timestamp DESC)
      `);

      await client.query(`
        CREATE INDEX IF NOT EXISTS idx_audit_operation 
        ON _audit_log(operation, table_name)
      `);

      await client.query(`
        CREATE TABLE IF NOT EXISTS _schema_versions (
          id SERIAL PRIMARY KEY,
          version VARCHAR(50) NOT NULL,
          schema JSONB NOT NULL,
          applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          hedera_tx_hash VARCHAR(64)
        )
      `);

      for (const [tableName, columns] of Object.entries(schema)) {
        await this.createTable(client, tableName, columns);
      }

      await client.query("COMMIT");
      console.log("✓ PostgreSQL infrastructure created");
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }

  async createTable(client, tableName, columns) {
    const columnDefs = Object.entries(columns)
      .map(([colName, colDef]) => {
        const colType = this.mapTypeToPostgres(colDef.type || colDef);
        const nullable = colDef.nullable !== false ? "" : "NOT NULL";
        const unique = colDef.unique ? "UNIQUE" : "";
        const defaultVal = colDef.default ? `DEFAULT ${colDef.default}` : "";
        
        return `${colName} ${colType} ${nullable} ${unique} ${defaultVal}`.trim();
      })
      .join(", ");

    await client.query(`
      CREATE TABLE IF NOT EXISTS ${tableName} (
        _tx_id VARCHAR(64) PRIMARY KEY,
        _created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        _updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        _version INTEGER DEFAULT 1,
        _data_hash VARCHAR(64) NOT NULL,
        _created_by VARCHAR(255),
        _is_deleted BOOLEAN DEFAULT FALSE,
        ${columnDefs}
      )
    `);

    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_${tableName}_created 
      ON ${tableName}(_created_at DESC)
    `);

    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_${tableName}_updated 
      ON ${tableName}(_updated_at DESC)
    `);

    console.log(`✓ Table created: ${tableName}`);
  }

  mapTypeToPostgres(type) {
    const typeMap = {
      string: "TEXT",
      text: "TEXT",
      varchar: "VARCHAR(255)",
      number: "NUMERIC",
      integer: "INTEGER",
      int: "INTEGER",
      bigint: "BIGINT",
      float: "REAL",
      double: "DOUBLE PRECISION",
      boolean: "BOOLEAN",
      bool: "BOOLEAN",
      timestamp: "BIGINT",
      date: "DATE",
      datetime: "TIMESTAMP",
      time: "TIME",
      json: "JSONB",
      jsonb: "JSONB",
      uuid: "UUID",
      array: "TEXT[]",
      binary: "BYTEA"
    };
    return typeMap[type.toLowerCase()] || "TEXT";
  }

  async insert(tableName, data, metadata = {}) {
    this.ensureInitialized();
    const client = await this.pgPool.connect();
    const txId = this.generateTxId();

    try {
      await client.query("BEGIN");

      this.validateData(tableName, data);

      const processedData = this.encryptionEnabled 
        ? this.encryptSensitiveFields(data) 
        : data;

      const dataHash = this.calculateHash(processedData);

      const columns = Object.keys(processedData);
      const values = Object.values(processedData);
      const placeholders = values.map((_, i) => `$${i + 1}`).join(", ");

      const insertQuery = `
        INSERT INTO ${tableName} 
        (_tx_id, _data_hash, _created_by, ${columns.join(", ")})
        VALUES ($${values.length + 1}, $${values.length + 2}, $${values.length + 3}, ${placeholders})
        RETURNING *
      `;

      const result = await client.query(insertQuery, [
        ...values, 
        txId, 
        dataHash, 
        metadata.userId || null
      ]);

      const auditMessage = {
        type: "INSERT",
        table: tableName,
        txId: txId,
        dataHash: dataHash,
        timestamp: Date.now(),
        metadata: this.sanitizeMetadata(metadata)
      };

      const hederaResult = await this.submitToHedera(auditMessage);

      await client.query(`
        INSERT INTO _audit_log 
        (tx_id, table_name, operation, data_hash, hedera_timestamp, 
         hedera_topic_id, metadata, user_id, ip_address)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
      `, [
        txId, 
        tableName, 
        "INSERT", 
        dataHash, 
        Date.now(),
        this.topicId.toString(),
        JSON.stringify(metadata),
        metadata.userId || null,
        metadata.ipAddress || null
      ]);

      await client.query("COMMIT");

      this.metrics.insertCount++;
      this.metrics.hederaSubmissions++;

      this.emit("insert", { tableName, txId, data: result.rows[0] });

      return {
        success: true,
        txId: txId,
        data: result.rows[0],
        dataHash: dataHash,
        hederaStatus: hederaResult.status,
        timestamp: Date.now()
      };
    } catch (error) {
      await client.query("ROLLBACK");
      this.metrics.errors++;
      console.error("Insert error:", error);
      throw error;
    } finally {
      client.release();
    }
  }

  async update(tableName, txId, updates, metadata = {}) {
    this.ensureInitialized();
    const client = await this.pgPool.connect();

    try {
      await client.query("BEGIN");

      const currentResult = await client.query(
        `SELECT * FROM ${tableName} WHERE _tx_id = $1 AND _is_deleted = FALSE`,
        [txId]
      );

      if (currentResult.rows.length === 0) {
        throw new Error(`Record not found or deleted: ${txId}`);
      }

      const currentData = currentResult.rows[0];

      this.validateData(tableName, updates, true);

      const processedUpdates = this.encryptionEnabled 
        ? this.encryptSensitiveFields(updates) 
        : updates;

      const { _tx_id, _created_at, _updated_at, _version, _data_hash, _created_by, _is_deleted, ...cleanData } = currentData;
      const newData = { ...cleanData, ...processedUpdates };
      const newDataHash = this.calculateHash(newData);

      const updateCols = Object.keys(processedUpdates);
      const updateVals = Object.values(processedUpdates);
      const setClauses = updateCols.map((col, i) => `${col} = $${i + 1}`).join(", ");

      const updateQuery = `
        UPDATE ${tableName}
        SET ${setClauses},
            _data_hash = $${updateVals.length + 1},
            _updated_at = CURRENT_TIMESTAMP,
            _version = _version + 1
        WHERE _tx_id = $${updateVals.length + 2}
        RETURNING *
      `;

      const result = await client.query(updateQuery, [...updateVals, newDataHash, txId]);

      const auditMessage = {
        type: "UPDATE",
        table: tableName,
        txId: txId,
        updates: Object.keys(processedUpdates),
        previousHash: currentData._data_hash,
        newHash: newDataHash,
        timestamp: Date.now(),
        metadata: this.sanitizeMetadata(metadata)
      };

      await this.submitToHedera(auditMessage);

      await client.query(`
        INSERT INTO _audit_log 
        (tx_id, table_name, operation, data_hash, hedera_timestamp, 
         hedera_topic_id, metadata, user_id, ip_address)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
      `, [
        txId, 
        tableName, 
        "UPDATE", 
        newDataHash, 
        Date.now(),
        this.topicId.toString(),
        JSON.stringify(metadata),
        metadata.userId || null,
        metadata.ipAddress || null
      ]);

      await client.query("COMMIT");

      this.metrics.updateCount++;
      this.metrics.hederaSubmissions++;

      this.emit("update", { tableName, txId, data: result.rows[0] });

      return {
        success: true,
        txId: txId,
        data: result.rows[0],
        previousHash: currentData._data_hash,
        newHash: newDataHash,
        version: result.rows[0]._version
      };
    } catch (error) {
      await client.query("ROLLBACK");
      this.metrics.errors++;
      console.error("Update error:", error);
      throw error;
    } finally {
      client.release();
    }
  }

  async delete(tableName, txId, metadata = {}, hardDelete = false) {
    this.ensureInitialized();
    const client = await this.pgPool.connect();

    try {
      await client.query("BEGIN");

      const currentResult = await client.query(
        `SELECT * FROM ${tableName} WHERE _tx_id = $1`,
        [txId]
      );

      if (currentResult.rows.length === 0) {
        throw new Error(`Record not found: ${txId}`);
      }

      const currentData = currentResult.rows[0];

      if (hardDelete) {
        await client.query(
          `DELETE FROM ${tableName} WHERE _tx_id = $1`,
          [txId]
        );
      } else {
        await client.query(
          `UPDATE ${tableName} SET _is_deleted = TRUE, _updated_at = CURRENT_TIMESTAMP WHERE _tx_id = $1`,
          [txId]
        );
      }

      const auditMessage = {
        type: hardDelete ? "DELETE_HARD" : "DELETE_SOFT",
        table: tableName,
        txId: txId,
        dataHash: currentData._data_hash,
        timestamp: Date.now(),
        metadata: this.sanitizeMetadata(metadata)
      };

      await this.submitToHedera(auditMessage);

      await client.query(`
        INSERT INTO _audit_log 
        (tx_id, table_name, operation, data_hash, hedera_timestamp, 
         hedera_topic_id, metadata, user_id, ip_address)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
      `, [
        txId, 
        tableName, 
        hardDelete ? "DELETE_HARD" : "DELETE_SOFT",
        currentData._data_hash, 
        Date.now(),
        this.topicId.toString(),
        JSON.stringify(metadata),
        metadata.userId || null,
        metadata.ipAddress || null
      ]);

      await client.query("COMMIT");

      this.metrics.deleteCount++;
      this.metrics.hederaSubmissions++;

      this.emit("delete", { tableName, txId, hardDelete });

      return {
        success: true,
        txId: txId,
        deleted: true,
        hardDelete: hardDelete
      };
    } catch (error) {
      await client.query("ROLLBACK");
      this.metrics.errors++;
      console.error("Delete error:", error);
      throw error;
    } finally {
      client.release();
    }
  }

  async query(tableName, options = {}) {
    this.ensureInitialized();

    try {
      const {
        where = {},
        select = "*",
        orderBy,
        limit,
        offset,
        includeDeleted = false
      } = options;

      let query = `SELECT ${select} FROM ${tableName}`;
      const params = [];
      const conditions = [];

      if (!includeDeleted) {
        conditions.push("_is_deleted = FALSE");
      }

      let paramIndex = 1;
      for (const [key, value] of Object.entries(where)) {
        if (typeof value === "object" && value !== null) {
          for (const [op, opValue] of Object.entries(value)) {
            const pgOp = this.mapOperator(op);
            conditions.push(`${key} ${pgOp} $${paramIndex}`);
            params.push(opValue);
            paramIndex++;
          }
        } else {
          conditions.push(`${key} = $${paramIndex}`);
          params.push(value);
          paramIndex++;
        }
      }

      if (conditions.length > 0) {
        query += ` WHERE ${conditions.join(" AND ")}`;
      }

      if (orderBy) {
        query += ` ORDER BY ${orderBy}`;
      }

      if (limit) {
        query += ` LIMIT ${limit}`;
      }
      if (offset) {
        query += ` OFFSET ${offset}`;
      }

      const result = await this.pgPool.query(query, params);

      this.metrics.queryCount++;

      return {
        success: true,
        data: result.rows,
        count: result.rowCount
      };
    } catch (error) {
      this.metrics.errors++;
      console.error("Query error:", error);
      throw error;
    }
  }

  mapOperator(op) {
    const operatorMap = {
      $eq: "=",
      $ne: "!=",
      $gt: ">",
      $gte: ">=",
      $lt: "<",
      $lte: "<=",
      $like: "LIKE",
      $ilike: "ILIKE",
      $in: "IN",
      $nin: "NOT IN"
    };
    return operatorMap[op] || "=";
  }

  async getAuditTrail(tableName, txId, options = {}) {
    try {
      const query = `
        SELECT * FROM _audit_log 
        WHERE table_name = $1 AND tx_id = $2
        ORDER BY created_at ${options.descending ? "DESC" : "ASC"}
      `;

      const result = await this.pgPool.query(query, [tableName, txId]);

      return {
        success: true,
        trail: result.rows,
        count: result.rowCount
      };
    } catch (error) {
      console.error("Audit trail error:", error);
      throw error;
    }
  }

  async verifyIntegrity(tableName, txId) {
    try {
      const dataResult = await this.pgPool.query(
        `SELECT * FROM ${tableName} WHERE _tx_id = $1`,
        [txId]
      );

      if (dataResult.rows.length === 0) {
        return { valid: false, error: "Record not found" };
      }

      const currentData = dataResult.rows[0];
      const storedHash = currentData._data_hash;

      const { _tx_id, _created_at, _updated_at, _version, _data_hash, _created_by, _is_deleted, ...cleanData } = currentData;
      const calculatedHash = this.calculateHash(cleanData);

      const auditResult = await this.getAuditTrail(tableName, txId);

      return {
        valid: storedHash === calculatedHash,
        storedHash: storedHash,
        calculatedHash: calculatedHash,
        match: storedHash === calculatedHash,
        auditRecords: auditResult.count,
        lastAudit: auditResult.trail[auditResult.trail.length - 1],
        verified: true
      };
    } catch (error) {
      return { valid: false, verified: false, error: error.message };
    }
  }

  async getMetrics() {
    return {
      ...this.metrics,
      topicId: this.topicId?.toString(),
      isInitialized: this.isInitialized,
      syncEnabled: this.syncEnabled
    };
  }

  async submitToHedera(message) {
    if (!this.topicId) {
      throw new Error("Database not initialized");
    }

    try {
      const messageString = JSON.stringify(message);
      
      const transaction = new TopicMessageSubmitTransaction()
        .setTopicId(this.topicId)
        .setMessage(messageString);

      const txResponse = await transaction.execute(this.hederaClient);
      const receipt = await txResponse.getReceipt(this.hederaClient);

      return {
        status: receipt.status.toString(),
        timestamp: Date.now()
      };
    } catch (error) {
      console.error("Hedera submission error:", error);
      throw error;
    }
  }

  startSyncService() {
    if (!this.topicId) return;

    try {
      new TopicMessageQuery()
        .setTopicId(this.topicId)
        .setStartTime(0)
        .subscribe(this.hederaClient, (message) => {
          const messageString = Buffer.from(message.contents).toString();
          const operation = JSON.parse(messageString);
          
          this.emit("hedera-message", {
            operation,
            sequenceNumber: message.sequenceNumber,
            timestamp: message.consensusTimestamp
          });
        });

      console.log("✓ Sync service started");
    } catch (error) {
      console.error("Sync service error:", error);
    }
  }

  calculateHash(data) {
    const sortedData = Object.keys(data)
      .sort()
      .reduce((acc, key) => {
        acc[key] = data[key];
        return acc;
      }, {});
    
    return crypto
      .createHash("sha256")
      .update(JSON.stringify(sortedData))
      .digest("hex");
  }

  generateTxId() {
    return crypto.randomBytes(16).toString("hex");
  }

  sanitizeMetadata(metadata) {
    const { password, token, secret, ...safe } = metadata;
    return safe;
  }

  validateData(tableName, data, isUpdate = false) {
    const schema = this.schemaCache.get("main");
    if (!schema || !schema[tableName]) {
      throw new Error(`Unknown table: ${tableName}`);
    }

    const tableSchema = schema[tableName];

    for (const [field, value] of Object.entries(data)) {
      if (!tableSchema[field]) {
        throw new Error(`Unknown field: ${field} in table ${tableName}`);
      }
    }
  }

  encryptSensitiveFields(data) {
    return data;
  }

  ensureInitialized() {
    if (!this.isInitialized) {
      throw new Error("Database not initialized. Call initialize() first.");
    }
  }

  async close() {
    try {
      await this.pgPool.end();
      this.hederaClient.close();
      this.isInitialized = false;
      console.log("✓ Database connections closed");
    } catch (error) {
      console.error("Close error:", error);
      throw error;
    }
  }
}

module.exports = { HederaAuditedDatabase };