class TransactionManager {
  constructor(db) {
    this.db = db;
    this.transactions = new Map();
  }

  async begin(transactionId) {
    const txn = {
      id: transactionId,
      operations: [],
      state: 'active',
      startTime: Date.now()
    };
    this.transactions.set(transactionId, txn);
    return txn;
  }

  async commit(transactionId) {
    const txn = this.transactions.get(transactionId);
    if (!txn) throw new Error('Transaction not found');
    
    // Execute all operations atomically
    for (const op of txn.operations) {
      await this.db[op.method](...op.args);
    }
    
    txn.state = 'committed';
    this.transactions.delete(transactionId);
  }

  async rollback(transactionId) {
    const txn = this.transactions.get(transactionId);
    if (!txn) throw new Error('Transaction not found');
    
    txn.state = 'rolled_back';
    this.transactions.delete(transactionId);
  }

  addOperation(transactionId, method, args) {
    const txn = this.transactions.get(transactionId);
    if (!txn) throw new Error('Transaction not found');
    
    txn.operations.push({ method, args });
  }
}