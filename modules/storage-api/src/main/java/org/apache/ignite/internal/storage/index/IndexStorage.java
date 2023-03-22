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

package org.apache.ignite.internal.storage.index;

import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.util.Cursor;
import org.jetbrains.annotations.Nullable;

/**
 * Common interface for all Index Storage implementations.
 */
public interface IndexStorage {
    /**
     * Returns a cursor over {@code RowId}s associated with the given index key.
     *
     * @throws StorageException If failed to read data.
     */
    Cursor<RowId> get(BinaryTuple key) throws StorageException;

    /**
     * Adds the given index row to the index.
     *
     * @apiNote This method <b>must</b> always be called inside the corresponding partition's
     *     {@link org.apache.ignite.internal.storage.MvPartitionStorage#runConsistently} closure.
     *
     * @throws StorageException If failed to put data.
     */
    void put(IndexRow row) throws StorageException;

    /**
     * Removes the given row from the index.
     *
     * <p>Removing a non-existent row is a no-op.
     *
     * @apiNote This method <b>must</b> always be called inside the corresponding partition's
     *     {@link org.apache.ignite.internal.storage.MvPartitionStorage#runConsistently} closure.
     *
     * @throws StorageException If failed to remove data.
     */
    void remove(IndexRow row) throws StorageException;

    /**
     * Returns the last row ID that has been processed by an ongoing index build process or {@code null} if the process has finished.
     *
     * @throws StorageException If failed to get the last row ID.
     */
    @Nullable RowId getLastBuildRowId();

    /**
     * Sets last row ID for which the index was built, {@code null} means index building is finished.
     *
     * @apiNote This method <b>must</b> always be called inside the corresponding partition's
     *     {@link org.apache.ignite.internal.storage.MvPartitionStorage#runConsistently} closure.
     *
     * @param rowId Row ID.
     * @throws StorageException If failed to set the last row ID.
     */
    void setLastBuildRowId(@Nullable RowId rowId);
}
