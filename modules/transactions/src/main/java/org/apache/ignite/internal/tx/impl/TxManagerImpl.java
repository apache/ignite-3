/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.Timestamp;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.network.ClusterService;

/**
 * TODO asch do we need the interface ?
 */
public class TxManagerImpl implements TxManager {
    /** */
    private ClusterService clusterService;

    /** */
    private final ConcurrentHashMap<Timestamp, TxState> states = new ConcurrentHashMap<>();

    /**
     * @param clusterService Cluster service.
     */
    public TxManagerImpl(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    @Override public InternalTransaction begin() {
        Timestamp ts = Timestamp.nextVersion();

        states.put(ts, TxState.PENDING);

        return new TransactionImpl(this, ts);
    }

    @Override public TxState state(Timestamp ts) {
        return states.get(ts);
    }

    @Override public void forget(Timestamp ts) {
        states.remove(ts);
    }

    @Override public CompletableFuture<Void> commitAsync(TransactionImpl tx) {
        changeState(tx.timestamp(), TxState.PENDING, TxState.COMMITED);

        return CompletableFuture.completedFuture(null);
    }

    @Override public CompletableFuture<Void> rollbackAsync(TransactionImpl tx) {
        changeState(tx.timestamp(), TxState.PENDING, TxState.ABORTED);

        return CompletableFuture.completedFuture(null);
    }

    /**
     * @param ts The timestamp.
     * @param before Before state.
     * @param after After state.
     * @return {@code} True if the state was changed.
     */
    @Override public boolean changeState(Timestamp ts, TxState before, TxState after) {
        return states.replace(ts, before, after);
    }
}
