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
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.tx.InternalTransaction;

class TransactionExpirationRegistry {
    private static final IgniteLogger LOG = Loggers.forClass(TransactionExpirationRegistry.class);

    /** Map from expiration timestamp (number of millis since Unix epoch) to set of transactions expiring at the timestamp. */
    private final NavigableMap<Long, Set<InternalTransaction>> txsByExpirationTime = new ConcurrentSkipListMap<>();
    /** Map from registered transaction to its expiration timestamp. */
    private final Map<InternalTransaction, Long> expirationTimeByTx = new ConcurrentHashMap<>();

    private final ReadWriteLock watermarkLock = new ReentrantReadWriteLock();

    /** Watermark at which expiration has already happened (millis since Unix epoch). */
    private volatile long watermark = Long.MIN_VALUE;

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

            Set<InternalTransaction> txsExpiringAtTs = txsByExpirationTime.computeIfAbsent(
                    txExpirationTime,
                    k -> ConcurrentHashMap.newKeySet()
            );
            txsExpiringAtTs.add(tx);

            expirationTimeByTx.put(tx, txExpirationTime);
        } finally {
            watermarkLock.readLock().unlock();
        }
    }

    private boolean isExpired(long expirationTime) {
        return expirationTime <= watermark;
    }

    private static void abortTransaction(InternalTransaction tx) {
        tx.rollbackAsync().whenComplete((res, ex) -> {
            if (ex != null) {
                LOG.error("Transaction abort due to timeout failed [txId={}]", ex, tx.id());
            }
        });
    }

    void expireUpTo(long expirationTime) {
        List<Set<InternalTransaction>> transactionSetsToExpire;

        watermarkLock.writeLock().lock();

        try {
            NavigableMap<Long, Set<InternalTransaction>> headMap = txsByExpirationTime.headMap(expirationTime, true);
            transactionSetsToExpire = new ArrayList<>(headMap.values());
            headMap.clear();

            watermark = expirationTime;
        } finally {
            watermarkLock.writeLock().unlock();
        }

        for (Set<InternalTransaction> set : transactionSetsToExpire) {
            for (InternalTransaction tx : set) {
                expirationTimeByTx.remove(tx);

                abortTransaction(tx);
            }
        }
    }

    void abortAllRegistered() {
        expireUpTo(Long.MAX_VALUE);
    }

    void unregister(InternalTransaction tx) {
        Long expirationTime = expirationTimeByTx.remove(tx);

        if (expirationTime != null) {
            txsByExpirationTime.compute(expirationTime, (k, set) -> {
                if (set == null) {
                    return null;
                }

                set.remove(tx);

                return set.isEmpty() ? null : set;
            });
        }
    }
}
