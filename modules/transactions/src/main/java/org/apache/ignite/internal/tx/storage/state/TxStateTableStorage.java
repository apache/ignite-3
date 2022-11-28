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

import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.configuration.storage.StorageException;
import org.apache.ignite.internal.schema.configuration.TableConfiguration;
import org.jetbrains.annotations.Nullable;

/**
 * Transaction state storage for a table.
 */
public interface TxStateTableStorage extends ManuallyCloseable {
    /**
     * Get or create transaction state storage for partition.
     *
     * @param partitionId Partition id.
     * @return Transaction state storage.
     * @throws StorageException  In case when the operation has failed.
     */
    TxStateStorage getOrCreateTxStateStorage(int partitionId) throws StorageException;

    /**
     * Get transaction state storage.
     *
     * @param partitionId Partition id.
     * @return Transaction state storage.
     */
    @Nullable
    TxStateStorage getTxStateStorage(int partitionId);

    /**
     * Destroy transaction state storage.
     *
     * @param partitionId Partition id.
     * @throws StorageException In case when the operation has failed.
     */
    void destroyTxStateStorage(int partitionId) throws StorageException;

    /**
     * Table configuration.
     *
     * @return Table configuration.
     */
    TableConfiguration configuration();

    /**
     * Start the storage.
     *
     * @throws StorageException In case when the operation has failed.
     */
    void start() throws StorageException;

    /**
     * Stop the storage.
     *
     * @throws StorageException In case when the operation has failed.
     */
    void stop() throws StorageException;

    /**
     * Closes the storage.
     */
    @Override
    void close();

    /**
     * Removes all data from the storage and frees all resources.
     *
     * @throws StorageException In case when the operation has failed.
     */
    void destroy() throws StorageException;
}
