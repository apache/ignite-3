/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.tx.impl;

import static org.apache.ignite.internal.tx.TransactionLogUtils.formatTxInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.ignite.tx.TransactionTimeoutException;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.TransactionIds;
import org.apache.ignite.internal.util.IgniteStripedReadWriteLock;

class TransactionExpirationRegistry {
    private static final IgniteLogger LOG = Loggers.forClass(TransactionExpirationRegistry.class);

    /** Volatile transaction state meta storage. */
    private final VolatileTxStateMetaStorage volatileTxStateMetaStorage;

    /**
     * Map from expiration timestamp (number of millis since Unix epoch) to transactions expiring at the timestamp.
     * Each value is either a transaction or a set of at least 2 transactions.
     */
    private final NavigableMap<Long, Object> txsByExpirationTime = new ConcurrentSkipListMap<>();

    private final IgniteStripedReadWriteLock watermarkLock = new IgniteStripedReadWriteLock();

    /** Watermark at which expiration has already happened (millis since Unix epoch). */
    private volatile long watermark = Long.MIN_VALUE;

    /** Constructor. */
    TransactionExpirationRegistry(VolatileTxStateMetaStorage volatileTxStateMetaStorage) {
        this.volatileTxStateMetaStorage = volatileTxStateMetaStorage;
    }

    private static long physicalExpirationTimeMillis(HybridTimestamp beginTimestamp, long effectiveTimeoutMillis) {
        return sumWithSaturation(beginTimestamp.getPhysical(), effectiveTimeoutMillis);
    }

    private static long sumWithSaturation(long a, long b) {
        assert a >= 0 : a;
        assert b >= 0 : b;

        long sum = a + b;

        if (sum < 0) {
            // Overflow.
            return Long.MAX_VALUE;
        } else {
            return sum;
        }
    }

    void register(InternalTransaction tx) {
        register(tx, physicalExpirationTimeMillis(TransactionIds.beginTimestamp(tx.id()), tx.getTimeout()));
    }

    void register(InternalTransaction tx, long txExpirationTime) {
        if (isExpired(txExpirationTime)) {
            abortTransaction(tx);
            return;
        }

        watermarkLock.readLock().lock();

        try {
            if (isExpired(txExpirationTime)) {
                abortTransaction(tx);
                return;
            }

            txsByExpirationTime.compute(
                    txExpirationTime,
                    (k, txOrSet) -> {
                        if (txOrSet == null) {
                            return tx;
                        }

                        Set<InternalTransaction> txsExpiringAtTs;
                        if (txOrSet instanceof Set) {
                            txsExpiringAtTs = (Set<InternalTransaction>) txOrSet;
                        } else {
                            // Using a concurrent set because txsByExpirationTime does not guarantee linearization of compute() calls,
                            // so the set might be accessed/updated concurrently. It's not a problem to use a mutable set here as both
                            // addition to it and removal from it are idempotent.
                            txsExpiringAtTs = ConcurrentHashMap.newKeySet();
                            txsExpiringAtTs.add((InternalTransaction) txOrSet);
                        }

                        txsExpiringAtTs.add(tx);

                        return txsExpiringAtTs;
                    }
            );
        } finally {
            watermarkLock.readLock().unlock();
        }
    }

    private boolean isExpired(long expirationTime) {
        return expirationTime <= watermark;
    }

    private void abortTransaction(InternalTransaction tx) {
        tx.rollbackWithExceptionAsync(new TransactionTimeoutException()).whenComplete((res, ex) -> {
            if (ex != null) {
                LOG.error("Transaction has aborted due to timeout {}.", ex,
                        formatTxInfo(tx.id(), volatileTxStateMetaStorage));
            }
        });
    }

    void expireUpTo(long expirationTime) {
        List<Object> transactionsAndSetsToExpire;

        watermarkLock.writeLock().lock();

        try {
            NavigableMap<Long, Object> headMap = txsByExpirationTime.headMap(expirationTime, true);
            transactionsAndSetsToExpire = new ArrayList<>(headMap.values());
            headMap.clear();

            watermark = expirationTime;
        } finally {
            watermarkLock.writeLock().unlock();
        }

        for (Object txOrSet : transactionsAndSetsToExpire) {
            if (txOrSet instanceof Set) {
                for (InternalTransaction tx : (Set<InternalTransaction>) txOrSet) {
                    abortTransaction(tx);
                }
            } else {
                InternalTransaction tx = (InternalTransaction) txOrSet;

                abortTransaction(tx);
            }
        }
    }

    void abortAllRegistered() {
        expireUpTo(Long.MAX_VALUE);
    }

    void unregister(InternalTransaction tx) {
        unregister(tx, physicalExpirationTimeMillis(TransactionIds.beginTimestamp(tx.id()), tx.getTimeout()));
    }

    void unregister(InternalTransaction tx, long expirationTime) {
        txsByExpirationTime.computeIfPresent(expirationTime, (k, txOrSet) -> {
            if (txOrSet instanceof Set) {
                Set<InternalTransaction> set = (Set<InternalTransaction>) txOrSet;

                set.remove(tx);

                int newSize = set.size();

                if (newSize == 0) {
                    return null;
                } else if (newSize == 1) {
                    try {
                        return set.iterator().next();
                    } catch (NoSuchElementException e) {
                        return null;
                    }
                } else {
                    return set;
                }
            } else {
                InternalTransaction registeredTx = (InternalTransaction) txOrSet;

                if (registeredTx.id().equals(tx.id())) {
                    return null;
                }

                return registeredTx;
            }
        });
    }
}
