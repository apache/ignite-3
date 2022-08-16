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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.apache.ignite.tx.TransactionException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The implementation of an internal transaction.
 *
 * <p>Delegates state management to tx manager.
 */
public class TransactionImpl implements InternalTransaction {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(TransactionImpl.class);

    /** The id. */
    private final UUID id;

    /** The transaction manager. */
    private final TxManager txManager;

    /** The originator. */
    private final NetworkAddress address;

    /** Enlisted raft groups. */
    private Set<RaftGroupService> enlistedRafts = Collections.newSetFromMap(new ConcurrentHashMap<>());

    /** Enlisted replication groups: replication group id -> (primary replica node, raft term). */
    private Map<String, IgniteBiTuple<ClusterNode, Long>> enlisted = new ConcurrentHashMap<>();

    /**
     * The constructor.
     *
     * @param txManager The tx manager.
     * @param id The id.
     * @param address   The local address.
     */
    public TransactionImpl(TxManager txManager, @NotNull UUID id, NetworkAddress address) {
        this.txManager = txManager;
        this.id = id;
        this.address = address;
    }

    /** {@inheritDoc} */
    @NotNull
    @Override
    public UUID id() {
        return id;
    }

    /** {@inheritDoc} */
    @Override
    public IgniteBiTuple<ClusterNode, Long> enlistedNodeAndTerm(String partGroupId) {
        return enlisted.get(partGroupId);
    }

    /** {@inheritDoc} */
    @Nullable
    @Override
    public TxState state() {
        return txManager.state(id);
    }

    /** {@inheritDoc} */
    @Override
    public IgniteBiTuple<ClusterNode, Long> enlist(String repicationGroupId, IgniteBiTuple<ClusterNode, Long> nodeAndTerm) {
        enlisted.put(repicationGroupId, nodeAndTerm);

        return nodeAndTerm;
    }

    /** {@inheritDoc} */
    @Override
    public void commit() throws TransactionException {
        try {
            commitAsync().get();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof TransactionException) {
                throw (TransactionException) e.getCause();
            } else {
                throw new TransactionException(e.getCause());
            }
        } catch (Exception e) {
            throw new TransactionException(e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> commitAsync() {
        return finish(true);
    }

    /** {@inheritDoc} */
    @Override
    public void rollback() throws TransactionException {
        try {
            rollbackAsync().get();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof TransactionException) {
                throw (TransactionException) e.getCause();
            } else {
                throw new TransactionException(e.getCause());
            }
        } catch (Exception e) {
            throw new TransactionException(e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> rollbackAsync() {
        return finish(false);
    }

    /**
     * Finishes a transaction.
     *
     * @param commit {@code true} to commit, false to rollback.
     * @return The future.
     */
    private CompletableFuture<Void> finish(boolean commit) {
        Map<NetworkAddress, Set<String>> tmp = new HashMap<>();

        // Group by common leader addresses.
        for (RaftGroupService svc : enlistedRafts) {
            NetworkAddress addr = svc.leader().address();

            tmp.computeIfAbsent(addr, k -> new HashSet<>()).add(svc.groupId());
        }

        CompletableFuture[] futs = new CompletableFuture[tmp.size() + 1];

        int i = 0;

        for (Map.Entry<NetworkAddress, Set<String>> entry : tmp.entrySet()) {
            boolean local = address.equals(entry.getKey());

            futs[i++] = txManager.finishRemote(entry.getKey(), commit, entry.getValue(), id);

            LOG.debug("finish [addr={}, commit={}, txId={}, local={}, groupIds={}",
                    address, commit, id, local, entry.getValue());
        }

        Set<NetworkAddress> allEnlistedNodes = new HashSet<>();

        for (RaftGroupService svc : enlistedRafts) {
            for (Peer peer : svc.peers()) {
                allEnlistedNodes.add(peer.address());
            }
        }

        // Handle coordinator's tx.
        futs[i] = allEnlistedNodes.contains(address) ? CompletableFuture.completedFuture(null) :
                commit ? txManager.commitAsync(id) : txManager.rollbackAsync(id);

        return CompletableFuture.allOf(futs);
    }
}
