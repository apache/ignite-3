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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.tx.InternalTransaction;

class TransactionExpirationRegistry {
    private static final IgniteLogger LOG = Loggers.forClass(TransactionExpirationRegistry.class);

    /**
     * Map from expiration timestamp (number of millis since Unix epoch) to transactions expiring at the timestamp.
     * Each value is either a transaction or a set of at least 2 transactions.
     */
    private final NavigableMap<Long, Object> txsByExpirationTime = new ConcurrentSkipListMap<>();

    private final ReadWriteLock watermarkLock = new ReentrantReadWriteLock();

    /** Watermark at which expiration has already happened (millis since Unix epoch). */
    private volatile long watermark = Long.MIN_VALUE;

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
        register(tx, physicalExpirationTimeMillis(tx.startTimestamp(), tx.getTimeout()));
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
                            txsExpiringAtTs = new HashSet<>();
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

    private static void abortTransaction(InternalTransaction tx) {
        tx.rollbackTimeoutExceededAsync().whenComplete((res, ex) -> {
            if (ex != null) {
                LOG.error("Transaction has aborted due to timeout [txId={}]", ex, tx.id());
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
        unregister(tx, physicalExpirationTimeMillis(tx.startTimestamp(), tx.getTimeout()));
    }

    void unregister(InternalTransaction tx, long expirationTime) {
        txsByExpirationTime.computeIfPresent(expirationTime, (k, txOrSet) -> {
            if (txOrSet instanceof Set) {
                Set<InternalTransaction> set = (Set<InternalTransaction>) txOrSet;

                set.remove(tx);

                return set.size() == 1 ? set.iterator().next() : set;
            } else {
                InternalTransaction tx0 = (InternalTransaction) txOrSet;

                if (tx0.id().equals(tx.id())) {
                    return null;
                }

                return tx0;
            }
        });
    }
}
