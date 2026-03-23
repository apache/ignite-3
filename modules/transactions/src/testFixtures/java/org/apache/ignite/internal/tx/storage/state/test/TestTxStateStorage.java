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

package org.apache.ignite.internal.tx.storage.state.test;

import static org.mockito.Mockito.spy;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.tx.storage.state.TxStatePartitionStorage;
import org.apache.ignite.internal.tx.storage.state.TxStateStorage;
import org.jetbrains.annotations.Nullable;

/**
 * Table tx state storage for {@link TestTxStatePartitionStorage}.
 */
public class TestTxStateStorage implements TxStateStorage {
    private final Map<Integer, TxStatePartitionStorage> storages = new ConcurrentHashMap<>();

    @Override public TxStatePartitionStorage getOrCreatePartitionStorage(int partitionId) {
        return storages.computeIfAbsent(partitionId, k -> spy(new TestTxStatePartitionStorage()));
    }

    @Override
    public @Nullable TxStatePartitionStorage getPartitionStorage(int partitionId) {
        return storages.get(partitionId);
    }

    @Override
    public void destroyPartitionStorage(int partitionId) {
        TxStatePartitionStorage storage = storages.remove(partitionId);

        if (storage != null) {
            storage.destroy();
        }
    }

    @Override
    public void start() {
        // No-op.
    }

    @Override
    public void destroy() {
        storages.clear();
    }

    @Override
    public void close() {
        // No-op.
    }
}
