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

package org.apache.ignite.internal.tx.storage.state.test;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.internal.configuration.storage.StorageException;
import org.apache.ignite.internal.tx.storage.state.TxStateStorage;
import org.apache.ignite.internal.tx.storage.state.TxStateTableStorage;
import org.jetbrains.annotations.Nullable;

/**
 * Table tx state storage for {@link TestConcurrentHashMapTxStateStorage}.
 */
public class TestConcurrentHashMapTxStateTableStorage implements TxStateTableStorage {
    private final Map<Integer, TxStateStorage> storages = new ConcurrentHashMap<>();

    /** {@inheritDoc} */
    @Override public TxStateStorage getOrCreateTxStateStorage(int partitionId) throws StorageException {
        return storages.computeIfAbsent(partitionId, k -> new TestConcurrentHashMapTxStateStorage());
    }

    /** {@inheritDoc} */
    @Override public @Nullable TxStateStorage getTxStateStorage(int partitionId) {
        return storages.get(partitionId);
    }

    /** {@inheritDoc} */
    @Override public void destroyTxStateStorage(int partitionId) throws StorageException {
        TxStateStorage storage = storages.get(partitionId);

        if (storage != null) {
            storage.destroy();
        }
    }

    /** {@inheritDoc} */
    @Override public TableConfiguration configuration() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void start() throws StorageException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void stop() throws StorageException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void destroy() throws StorageException {
        storages.clear();
    }

    /** {@inheritDoc} */
    @Override public void close() throws Exception {
        stop();
    }
}
