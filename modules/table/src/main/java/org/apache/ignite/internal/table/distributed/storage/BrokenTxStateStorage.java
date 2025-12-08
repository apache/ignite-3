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

package org.apache.ignite.internal.table.distributed.storage;

import org.apache.ignite.internal.tx.storage.state.TxStatePartitionStorage;
import org.apache.ignite.internal.tx.storage.state.TxStateStorage;
import org.jetbrains.annotations.Nullable;

/**
 * {@link TxStateStorage} implementation that returns broken partition storages.
 * It is intended to make sure that with enabled colocation, table-scoped tx state storages are not used.
 */
// TODO: https://issues.apache.org/jira/browse/IGNITE-22522 - remove this.
// TODO sanpwc Remove
public class BrokenTxStateStorage implements TxStateStorage {
    @Override
    public TxStatePartitionStorage getOrCreatePartitionStorage(int partitionId) {
        return new BrokenTxStatePartitionStorage();
    }

    @Override
    public @Nullable TxStatePartitionStorage getPartitionStorage(int partitionId) {
        return new BrokenTxStatePartitionStorage();
    }

    @Override
    public void destroyPartitionStorage(int partitionId) {
        // No-op.
    }

    @Override
    public void start() {
        // No-op.
    }

    @Override
    public void close() {
        // No-op.
    }

    @Override
    public void destroy() {
        // No-op.
    }
}
