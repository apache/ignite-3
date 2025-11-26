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

package org.apache.ignite.internal.tx.storage.state;

import static org.apache.ignite.internal.worker.ThreadAssertions.assertThreadAllowsToWrite;

import org.apache.ignite.internal.worker.ThreadAssertions;
import org.jetbrains.annotations.Nullable;

/**
 * {@link TxStateStorage} that performs thread assertions when doing read/write operations.
 *
 * @see ThreadAssertions
 */
public class ThreadAssertingTxStateStorage implements TxStateStorage {
    private final TxStateStorage wrappedStorage;

    /** Constructor. */
    public ThreadAssertingTxStateStorage(TxStateStorage wrappedStorage) {
        this.wrappedStorage = wrappedStorage;
    }

    @Override
    public TxStatePartitionStorage getOrCreatePartitionStorage(int partitionId) {
        assertThreadAllowsToWrite();

        return wrapTxStatePartitionStorage(wrappedStorage.getOrCreatePartitionStorage(partitionId));
    }

    private static ThreadAssertingTxStatePartitionStorage wrapTxStatePartitionStorage(TxStatePartitionStorage storage) {
        return storage instanceof ThreadAssertingTxStatePartitionStorage
                ? (ThreadAssertingTxStatePartitionStorage) storage
                : new ThreadAssertingTxStatePartitionStorage(storage);
    }

    @Override
    public @Nullable TxStatePartitionStorage getPartitionStorage(int partitionId) {
        TxStatePartitionStorage storage = wrappedStorage.getPartitionStorage(partitionId);

        return storage == null ? null : wrapTxStatePartitionStorage(storage);
    }

    @Override
    public void destroyPartitionStorage(int partitionId) {
        assertThreadAllowsToWrite();

        wrappedStorage.destroyPartitionStorage(partitionId);
    }

    @Override
    public void start() {
        wrappedStorage.start();
    }

    @Override
    public void close() {
        wrappedStorage.close();
    }

    @Override
    public void destroy() {
        assertThreadAllowsToWrite();

        wrappedStorage.destroy();
    }
}
