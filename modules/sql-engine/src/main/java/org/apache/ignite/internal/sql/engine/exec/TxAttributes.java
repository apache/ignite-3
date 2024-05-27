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

package org.apache.ignite.internal.sql.engine.exec;

import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.jetbrains.annotations.Nullable;

/**
 * A holder of attributes of particular transaction.
 *
 * <p>Contains attributes that are required to execute query (like txId and time to initialize transactional cursor properly).
 * These attributes will be shared among the nodes that should execute the query.
 */
public class TxAttributes implements Serializable {
    private static final long serialVersionUID = 3933878724800694086L;

    private final UUID id;
    private final String coordinatorId;
    private final boolean readOnly;
    private final @Nullable HybridTimestamp readTimestamp;
    private final @Nullable ZonePartitionId zoneCommitPartition;

    /**
     * Derives transactional attributes from the given transaction.
     *
     * @param tx A transaction to derive attributes from.
     * @return An attributes of the transaction.
     * @throws IllegalArgumentException If read time is not set for RO transaction.
     */
    public static TxAttributes fromTx(InternalTransaction tx) {
        if (tx.isReadOnly()) {
            HybridTimestamp readTime = tx.readTimestamp();

            if (readTime == null) {
                throw new IllegalArgumentException("Read time is not set for RO transaction");
            }

            return new TxAttributes(tx.id(), readTime, tx.coordinatorId());
        }

        return new TxAttributes(tx.id(), tx.zoneCommitPartition(), tx.coordinatorId());
    }

    private TxAttributes(
            UUID id,
            HybridTimestamp readTimestamp,
            String coordinatorId
    ) {
        this.id = Objects.requireNonNull(id, "id");
        this.readTimestamp = Objects.requireNonNull(readTimestamp, "timestamp");
        this.coordinatorId = Objects.requireNonNull(coordinatorId, "tx coordinator id");

        this.readOnly = true;
        this.zoneCommitPartition = null;
    }

    private TxAttributes(
            UUID id,
            @Nullable ZonePartitionId zoneCommitPartition,
            String coordinatorId
    ) {
        this.id = Objects.requireNonNull(id, "id");
        this.zoneCommitPartition = zoneCommitPartition;
        this.coordinatorId = Objects.requireNonNull(coordinatorId, "tx coordinator id");

        this.readOnly = false;
        this.readTimestamp = null;
    }

    /**
     * Returns an identifier of zone commit partition.
     *
     * <p>Commit partition is always {@code null} for RO transaction, and may be {@code null} for RW transaction
     * in case no tables were yet involved (for example, the very first query is SELECT 42).
     *
     * @return An identifier of zone commit partition, or {@code null} if zone commit partition was not yet assigned.
     */
    public @Nullable ZonePartitionId zoneCommitPartition() {
        return zoneCommitPartition;
    }

    /** Returns an identifier of the transaction. */
    public UUID id() {
        return id;
    }

    /**
     * Returns a read time of the transaction.
     *
     * <p>Read time is always {@code null} for RW transaction, and never {@code null} for RO transaction.
     *
     * @return A read time of the transaction, or {@code null} if this is RW transaction.
     */
    public @Nullable HybridTimestamp time() {
        return readTimestamp;
    }

    /**
     * Get the transaction coordinator inconsistent ID.
     *
     * @return Transaction coordinator inconsistent ID.
     */
    public String coordinatorId() {
        return coordinatorId;
    }

    /** Returns {@code true} if this is RO transaction. */
    public boolean readOnly() {
        return readOnly;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(this);
    }
}
