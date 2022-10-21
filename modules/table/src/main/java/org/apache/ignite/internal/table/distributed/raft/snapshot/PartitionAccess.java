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

package org.apache.ignite.internal.table.distributed.raft.snapshot;

import java.util.List;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageException;
import org.jetbrains.annotations.Nullable;

/**
 * Small abstractions for partition storages that includes only methods, mandatory for the snapshot storage.
 */
public interface PartitionAccess {
    /**
     * Returns the key that uniquely identifies the corresponding partition.
     *
     * @return Partition key.
     */
    PartitionKey key();

    /**
     * Returns persisted RAFT index for the partition.
     */
    long persistedIndex();

    /**
     * Returns a row id, existing in the storage, that's greater or equal than the lower bound. {@code null} if not found.
     *
     * @param lowerBound Lower bound.
     * @throws StorageException If failed to read data from the storage.
     */
    @Nullable
    RowId closestRowId(RowId lowerBound);

    /**
     * Returns all versions of a row identified with the given {@link RowId}.
     * The returned versions are in newest-to-oldest order.
     *
     * @param rowId Id of the row.
     * @return All versions of the row.
     */
    List<ReadResult> rowVersions(RowId rowId);
}
