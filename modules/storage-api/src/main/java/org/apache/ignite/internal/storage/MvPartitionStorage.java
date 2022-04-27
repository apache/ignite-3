/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
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

package org.apache.ignite.internal.storage;

import java.util.UUID;
import java.util.function.Predicate;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.tx.Timestamp;
import org.apache.ignite.internal.util.Cursor;
import org.jetbrains.annotations.Nullable;

/**
 * Multi-versioned partition storage.
 * POC version, that represents a combination between a replicated TX-aware MV storage and physical MV storage. Their API are very similar,
 * although there are very important differences that will be addressed in the future.
 */
public interface MvPartitionStorage {
    /**
     * Reads the value from the storage as it was at the given timestamp. {@code null} timestamp means reading the latest value.
     *
     * @param key Key.
     * @param timestamp Timestamp.
     * @return Binary row that corresponds to the key or {@code null} if value is not found.
     */
    @Nullable
    BinaryRow read(BinaryRow key, @Nullable Timestamp timestamp);

    /**
     * Creates an uncommitted version, assigned to the given transaction id.
     *
     * @param row Binary row to update. Key only row means value removal.
     * @param txId Transaction id.
     * @throws TxIdMismatchException If there's another pending update associated with different transaction id.
     * @throws StorageException If failed to write data to the storage.
     */
    void addWrite(BinaryRow row, UUID txId) throws TxIdMismatchException, StorageException;

    /**
     * Aborts a pending update of the ongoing uncommitted transaction. Invoked during rollback.
     *
     * @param key Key.
     * @throws StorageException If failed to write data to the storage.
     */
    void abortWrite(BinaryRow key) throws StorageException;

    /**
     * Commits a pending update of the ongoing transaction. Invoked during commit. Committed value will be versioned by the given timestamp.
     *
     * @param key Key.
     * @param timestamp Timestamp to associate with committed value.
     * @throws StorageException If failed to write data to the storage.
     */
    void commitWrite(BinaryRow key, Timestamp timestamp) throws StorageException;

    /**
     * Scans the partition and returns a cursor of values at the given timestamp.
     *
     * @param keyFilter Key filter. Binary rows passed to the filter may or may not have a value, filter should only check keys.
     * @param timestamp Timestamp.
     * @return Cursor.
     */
    Cursor<BinaryRow> scan(Predicate<BinaryRow> keyFilter, @Nullable Timestamp timestamp) throws StorageException;
}
