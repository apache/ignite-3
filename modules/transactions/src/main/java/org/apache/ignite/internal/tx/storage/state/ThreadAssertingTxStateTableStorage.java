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
 * {@link TxStateTableStorage} that performs thread assertions when doing read/write operations.
 *
 * @see ThreadAssertions
 */
public class ThreadAssertingTxStateTableStorage implements TxStateTableStorage {
    private final TxStateTableStorage tableStorage;

    /** Constructor. */
    public ThreadAssertingTxStateTableStorage(TxStateTableStorage tableStorage) {
        this.tableStorage = tableStorage;
    }

    @Override
    public TxStateStorage getOrCreateTxStateStorage(int partitionId) {
        assertThreadAllowsToWrite();

        return new ThreadAssertingTxStateStorage(tableStorage.getOrCreateTxStateStorage(partitionId));
    }

    @Override
    public @Nullable TxStateStorage getTxStateStorage(int partitionId) {
        TxStateStorage storage = tableStorage.getTxStateStorage(partitionId);

        return storage == null ? null : new ThreadAssertingTxStateStorage(storage);
    }

    @Override
    public void destroyTxStateStorage(int partitionId) {
        assertThreadAllowsToWrite();

        tableStorage.destroyTxStateStorage(partitionId);
    }

    @Override
    public void start() {
        tableStorage.start();
    }

    @Override
    public void stop() {
        tableStorage.stop();
    }

    @Override
    public void close() {
        tableStorage.close();
    }

    @Override
    public void destroy() {
        assertThreadAllowsToWrite();

        tableStorage.destroy();
    }
}
