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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.storage.StorageException;
import org.jetbrains.annotations.Nullable;

/**
 * Small abstractions for partition storages that includes only methods, mandatory for the snapshot storage.
 */
public interface PartitionAccess {
    /**
     * Returns persisted RAFT index for the partition.
     */
    long persistedIndex();

    /**
     * Destroys and recreates the partition.
     *
     * @throws StorageException If an error has occurred during the partition destruction.
     */
    CompletableFuture<Void> reCreatePartition() throws StorageException;

    /**
     * Sets the last applied index value.
     *
     * @throws StorageException If an error occurs while setting the last applied index.
     */
    void lastAppliedIndex(long lastAppliedIndex) throws StorageException;

    /**
     * Writes the version chain to the partition.
     *
     * @param rowId Row id.
     * @param rowVersions List of {@link BinaryRow}s for a given {@code rowId}.
     * @param timestamps List of commit timestamps for all committed versions. Might be smaller than {@code rowVersions} if there's a
     *      write-intent in the chain.
     * @param txId Transaction id for write-intent if it's present.
     * @param commitTableId Commit table id for write-intent if it's present.
     * @param commitPartitionId Commit partition id for write-intent if it's present. {@link ReadResult#UNDEFINED_COMMIT_PARTITION_ID}
     *      otherwise.
     * @throws StorageException If an error occurs while writing the version chain.
     */
    void writeVersionChain(
            UUID rowId,
            List<ByteBuffer> rowVersions,
            List<HybridTimestamp> timestamps,
            @Nullable UUID txId,
            @Nullable UUID commitTableId,
            int commitPartitionId
    ) throws StorageException;
}
