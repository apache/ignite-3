package org.apache.ignite.cache;

import org.apache.ignite.tx.TransactionException;

public interface CacheTransaction {
    void commit() throws TransactionException;
    void rollback() throws TransactionException;
}
