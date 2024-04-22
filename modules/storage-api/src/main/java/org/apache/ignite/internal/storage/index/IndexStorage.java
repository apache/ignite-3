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
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.util.StorageUtils;
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
     * @throws IndexNotBuiltException If the index has not yet been built.
     */
    Cursor<RowId> get(BinaryTuple key) throws StorageException;

    /**
     * Adds the given index row to the index.
     *
     * @apiNote This method <b>must</b> always be called inside the corresponding partition's
     *     {@link MvPartitionStorage#runConsistently} closure.
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
     *     {@link MvPartitionStorage#runConsistently} closure.
     *
     * @throws StorageException If failed to remove data.
     */
    void remove(IndexRow row) throws StorageException;

    /**
     * Returns the row ID for which the index needs to be built, {@code null} means that the index building has completed.
     *
     * <p>If index building has not started yet, it will return {@link StorageUtils#initialRowIdToBuild}.
     *
     * @throws StorageException If failed to get the row ID.
     */
    @Nullable RowId getNextRowIdToBuild() throws StorageException;

    /**
     * Sets the row ID for which the index needs to be built, {@code null} means that the index is built.
     *
     * @apiNote This method <b>must</b> always be called inside the corresponding partition's
     *      {@link MvPartitionStorage#runConsistently} closure.
     *
     * @param rowId Row ID.
     * @throws StorageException If failed to set the row ID.
     */
    void setNextRowIdToBuild(@Nullable RowId rowId) throws StorageException;
}
