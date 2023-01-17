/*
 * Copyright 2022 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.table.distributed.raft.snapshot.outgoing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.UUID;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.ByteBufferRow;
import org.apache.ignite.internal.storage.BaseMvPartitionStorageTest;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.tx.Timestamp;
import org.junit.jupiter.api.Test;

public abstract class MvPartitionStorageIdempotencyAbstractTest extends BaseMvPartitionStorageTest {
    @Test
    public void testMvPartitionStorageIdempotency() {
        RowId rowId = new RowId(PARTITION_ID);
        BinaryRow row = new ByteBufferRow(new byte[1]);
        UUID tx0 = Timestamp.nextVersion().toUuid();
        UUID commitTableId = UUID.randomUUID();
        int commitPartitionId = 0;

        BinaryRow prev = storage.runConsistently(() -> storage.addWrite(rowId, row, tx0, commitTableId, commitPartitionId));
        assertNull(prev);
        prev = storage.runConsistently(() -> storage.addWrite(rowId, row, tx0, commitTableId, commitPartitionId));
        assertEquals(row.byteBuffer(), prev.byteBuffer());

        prev = storage.runConsistently(() -> storage.addWrite(rowId, null, tx0, commitTableId, commitPartitionId));
        assertEquals(row.byteBuffer(), prev.byteBuffer());
        prev = storage.runConsistently(() -> storage.addWrite(rowId, row, tx0, commitTableId, commitPartitionId));
        assertNull(prev);
    }
}
