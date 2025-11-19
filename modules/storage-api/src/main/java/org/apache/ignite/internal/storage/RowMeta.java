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

package org.apache.ignite.internal.storage;

import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.Nullable;

/**
 * Represents metadata of a row stored in the storage.
 */
public class RowMeta {
    private final RowId rowId;
    private final @Nullable UUID transactionId;

    private final @Nullable Integer commitTableOrZoneId;
    private final int commitPartitionId;

    /** Creates a RowMeta instance for a row without a write intent. */
    public static RowMeta withoutWriteIntent(RowId rowId) {
        return new RowMeta(rowId, null, null, ReadResult.UNDEFINED_COMMIT_PARTITION_ID);
    }

    /** Constructor. */
    public RowMeta(RowId rowId, @Nullable UUID transactionId, @Nullable Integer commitTableOrZoneId, int commitPartitionId) {
        this.rowId = rowId;
        this.transactionId = transactionId;
        this.commitTableOrZoneId = commitTableOrZoneId;
        this.commitPartitionId = commitPartitionId;
    }

    /** Returns the row ID. */
    public RowId rowId() {
        return rowId;
    }

    /** Returns the transaction ID if this row is a write intent, or {@code null} otherwise. */
    public @Nullable UUID transactionId() {
        return transactionId;
    }

    /** Returns {@code true} if this row is a write intent. */
    public boolean isWriteIntent() {
        return transactionId != null;
    }

    /** Returns the commit table or zone ID if this row has a write intent, or {@code null} otherwise. */
    public @Nullable Integer commitTableOrZoneId() {
        return commitTableOrZoneId;
    }

    /** Returns the commit partition ID. If row has no write intent, it's {@link ReadResult#UNDEFINED_COMMIT_PARTITION_ID}. */
    public int commitPartitionId() {
        return commitPartitionId;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RowMeta rowMeta = (RowMeta) o;

        return commitPartitionId == rowMeta.commitPartitionId
                && Objects.equals(rowId, rowMeta.rowId)
                && Objects.equals(transactionId, rowMeta.transactionId)
                && Objects.equals(commitTableOrZoneId, rowMeta.commitTableOrZoneId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rowId, transactionId, commitTableOrZoneId, commitPartitionId);
    }

    @Override
    public String toString() {
        return S.toString(this);
    }
}
