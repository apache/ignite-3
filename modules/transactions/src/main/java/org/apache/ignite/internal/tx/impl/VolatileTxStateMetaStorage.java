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

import static org.apache.ignite.internal.tx.TxState.checkTransitionCorrectness;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.TxStateMeta;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;

/**
 * The class represents volatile transaction state storage that stores a transaction state meta until the node stops.
 */
public class VolatileTxStateMetaStorage {
    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** The local map for tx states. */
    private ConcurrentHashMap<UUID, TxStateMeta> txStateMap;

    /**
     * Starts the storage.
     */
    public void start() {
        txStateMap = new ConcurrentHashMap<>();
    }

    /**
     * Stops the detector.
     */
    public void stop() {
        busyLock.block();

        txStateMap.clear();
    }

    /**
     * Atomically changes the state meta of a transaction.
     *
     * @param txId Transaction id.
     * @param updater Transaction meta updater.
     * @return Updated transaction state.
     * @throws NodeStoppingException If the node is stopping.
     */
    public TxStateMeta updateMeta(UUID txId, Function<TxStateMeta, TxStateMeta> updater) throws NodeStoppingException {
        if (busyLock.enterBusy()) {
            try {
                return updateMetaInternal(txId, updater);
            } finally {
                busyLock.leaveBusy();
            }
        }

        throw new NodeStoppingException();
    }

    /**
     * The internal method for atomically changing the state meta of a transaction.
     *
     * @param txId Transaction id.
     * @param updater Transaction meta updater.
     * @return Updated transaction state.
     */
    private TxStateMeta updateMetaInternal(UUID txId, Function<TxStateMeta, TxStateMeta> updater) {
        return txStateMap.compute(txId, (k, oldMeta) -> {
            TxStateMeta newMeta = updater.apply(oldMeta);

            if (newMeta == null) {
                return null;
            }

            TxState oldState = oldMeta == null ? null : oldMeta.txState();

            return checkTransitionCorrectness(oldState, newMeta.txState()) ? newMeta : oldMeta;
        });
    }

    /**
     * Returns a transaction state meta.
     *
     * @param txId Transaction id.
     * @return The state meta or null if the state is unknown.
     * @throws NodeStoppingException If the node is stopping.
     */
    public TxStateMeta state(UUID txId) throws NodeStoppingException {
        if (busyLock.enterBusy()) {
            try {
                return txStateMap.get(txId);
            } finally {
                busyLock.leaveBusy();
            }
        }

        throw new NodeStoppingException();
    }

    /**
     * Gets all defined transactions meta states.
     *
     * @return Collection of transaction meta states.
     * @throws NodeStoppingException If the node is stopping.
     */
    public Collection<TxStateMeta> states() throws NodeStoppingException {
        if (busyLock.enterBusy()) {
            try {
                return txStateMap.values();
            } finally {
                busyLock.leaveBusy();
            }
        }

        throw new NodeStoppingException();
    }
}
