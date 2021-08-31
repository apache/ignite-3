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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.Timestamp;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.tx.TransactionException;

/** */
public class TransactionImpl implements InternalTransaction {
    /** The timestamp. */
    private final Timestamp timestamp;

    /** TX manager. */
    private final TxManager txManager;

    /** */
    private List<NetworkAddress> nodes = new CopyOnWriteArrayList<>();

    /**
     * @param txManager The tx managert.
     * @param timestamp The timestamp.
     */
    public TransactionImpl(TxManager txManager, Timestamp timestamp) {
        this.txManager = txManager;
        this.timestamp = timestamp;
    }

    /** {@inheritDoc} */
    @Override public Timestamp timestamp() {
        return timestamp;
    }

    @Override public List<NetworkAddress> nodes() {
        return nodes;
    }

    /** {@inheritDoc} */
    @Override public TxState state() {
        return txManager.state(timestamp);
    }

    /** {@inheritDoc} */
    @Override public synchronized boolean enlist(NetworkAddress node) {
        boolean newNode = !nodes.contains(node);

        if (newNode)
            nodes.add(node);

        return newNode;
    }

    /** {@inheritDoc} */
    @Override public void commit() throws TransactionException {
        try {
            commitAsync().get();
        }
        catch (Exception e) {
            throw new TransactionException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> commitAsync() {
        CompletableFuture[] futs = new CompletableFuture[nodes.size()];

        for (int i = 0; i < nodes.size(); i++) {
            NetworkAddress node = nodes.get(i);

            futs[i] = (txManager.isLocal(node) ? txManager.commitAsync(timestamp) :
                txManager.sendFinishMessage(node, timestamp, true));
        }

        return CompletableFuture.allOf(futs);
    }

    /** {@inheritDoc} */
    @Override public void rollback() throws TransactionException {
        try {
            rollbackAsync().get();
        }
        catch (Exception e) {
            throw new TransactionException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> rollbackAsync() {
        return txManager.rollbackAsync(this.timestamp);
    }
}
