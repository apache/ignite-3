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

package org.apache.ignite.internal.table.distributed.raft;

import static org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestamp;
import static org.apache.ignite.internal.storage.BinaryRowAndRowIdMatcher.equalToBinaryRowAndRowId;
import static org.apache.ignite.internal.storage.RowId.lowestRowId;
import static org.apache.ignite.internal.tx.TransactionIds.transactionId;
import static org.apache.ignite.internal.type.NativeTypes.INT32;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.mockito.Mockito.spy;

import java.util.UUID;
import org.apache.ignite.distributed.TestPartitionDataStorage;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaTestUtils;
import org.apache.ignite.internal.storage.BinaryRowAndRowId;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.impl.TestMvPartitionStorage;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.hamcrest.Matcher;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;

/** For {@link BuildIndexRowVersionChooser} testing. */
public class BuildIndexRowVersionChooserTest extends IgniteAbstractTest {
    private static final int ZONE_ID = 11;
    private static final int TABLE_ID = 1;

    private static final int PARTITION_ID = 0;

    private static final long CREATE_INDEX_ACTIVATION_TS_MILLS = 100;

    private static final long START_BUILDING_INDEX_ACTIVATION_TS_MILLS = 200;

    private static final SchemaDescriptor SCHEMA = new SchemaDescriptor(
            1,
            new Column[]{new Column("key", INT32, false)},
            new Column[]{new Column("value", INT32, false)}
    );

    private final MvPartitionStorage mvPartitionStorage = spy(new TestMvPartitionStorage(PARTITION_ID));

    private final PartitionDataStorage partitionDataStorage = spy(
            new TestPartitionDataStorage(ZONE_ID, TABLE_ID, PARTITION_ID, mvPartitionStorage)
    );

    private final BuildIndexRowVersionChooser chooser = new BuildIndexRowVersionChooser(
            partitionDataStorage,
            CREATE_INDEX_ACTIVATION_TS_MILLS,
            START_BUILDING_INDEX_ACTIVATION_TS_MILLS
    );

    @Test
    void testEmptyStorage() {
        assertThat(chooser.chooseForBuildIndex(lowestRowId(PARTITION_ID)), empty());
    }

    @Test
    void testWriteIntentOnCreateIndexActivationTs() {
        RowId rowId = lowestRowId(PARTITION_ID);
        BinaryRow row = binaryRow(1, 2);
        UUID txId = txId(CREATE_INDEX_ACTIVATION_TS_MILLS);

        addWrite(rowId, row, txId);

        assertThat(chooser.chooseForBuildIndex(rowId), empty());
    }

    @Test
    void testWriteIntentAfterCreateIndexActivationTs() {
        RowId rowId = lowestRowId(PARTITION_ID);
        BinaryRow row = binaryRow(1, 2);
        UUID txId = txId(CREATE_INDEX_ACTIVATION_TS_MILLS + 1);

        addWrite(rowId, row, txId);

        assertThat(chooser.chooseForBuildIndex(rowId), empty());
    }

    @Test
    void testWriteIntentBeforeCreateIndexActivationTs() {
        RowId rowId = lowestRowId(PARTITION_ID);
        BinaryRow row = binaryRow(1, 2);
        UUID txId = txId(CREATE_INDEX_ACTIVATION_TS_MILLS - 1);

        addWrite(rowId, row, txId);

        assertThat(chooser.chooseForBuildIndex(rowId), contains(expBinaryRowAndRowId(rowId, row)));
    }

    @Test
    void testWriteCommittedAfterStartBuildingActivationTs() {
        RowId rowId = lowestRowId(PARTITION_ID);
        BinaryRow row = binaryRow(1, 2);
        long commitTs = START_BUILDING_INDEX_ACTIVATION_TS_MILLS + 1;

        addWriteCommitted(rowId, row, commitTs);

        assertThat(chooser.chooseForBuildIndex(rowId), empty());
    }

    @Test
    void testWriteCommittedOnStartBuildingActivationTs() {
        RowId rowId = lowestRowId(PARTITION_ID);
        BinaryRow row = binaryRow(1, 2);
        long commitTs = START_BUILDING_INDEX_ACTIVATION_TS_MILLS;

        addWriteCommitted(rowId, row, commitTs);

        assertThat(chooser.chooseForBuildIndex(rowId), contains(expBinaryRowAndRowId(rowId, row)));
    }

    @Test
    void testWriteCommittedBeforeStartBuildingActivationTs() {
        RowId rowId = lowestRowId(PARTITION_ID);
        BinaryRow row = binaryRow(1, 2);
        long commitTs = START_BUILDING_INDEX_ACTIVATION_TS_MILLS - 1;

        addWriteCommitted(rowId, row, commitTs);

        assertThat(chooser.chooseForBuildIndex(rowId), contains(expBinaryRowAndRowId(rowId, row)));
    }

    @Test
    void testWriteCommittedBeforeStartBuildingActivationTsLatest() {
        RowId rowId = lowestRowId(PARTITION_ID);

        BinaryRow row0 = binaryRow(1, 2);
        BinaryRow row1 = binaryRow(3, 4);
        BinaryRow row2 = binaryRow(5, 6);

        long commitTs0 = START_BUILDING_INDEX_ACTIVATION_TS_MILLS - 2;
        long commitTs1 = START_BUILDING_INDEX_ACTIVATION_TS_MILLS - 1;
        long commitTs2 = START_BUILDING_INDEX_ACTIVATION_TS_MILLS;

        addWriteCommitted(rowId, row0, commitTs0);
        addWriteCommitted(rowId, row1, commitTs1);
        addWriteCommitted(rowId, row2, commitTs2);

        assertThat(chooser.chooseForBuildIndex(rowId), contains(expBinaryRowAndRowId(rowId, row2)));
    }

    @Test
    void testWriteCommittedMixed() {
        RowId rowId = lowestRowId(PARTITION_ID);

        BinaryRow row0 = binaryRow(1, 2);
        BinaryRow row1 = binaryRow(3, 4);
        BinaryRow row2 = binaryRow(5, 6);

        long commitTs0 = START_BUILDING_INDEX_ACTIVATION_TS_MILLS - 1;
        long commitTs1 = START_BUILDING_INDEX_ACTIVATION_TS_MILLS;
        long commitTs2 = START_BUILDING_INDEX_ACTIVATION_TS_MILLS + 1;

        addWriteCommitted(rowId, row0, commitTs0);
        addWriteCommitted(rowId, row1, commitTs1);
        addWriteCommitted(rowId, row2, commitTs2);

        assertThat(chooser.chooseForBuildIndex(rowId), contains(expBinaryRowAndRowId(rowId, row1)));
    }

    @Test
    void testWriteMixed() {
        RowId rowId = lowestRowId(PARTITION_ID);

        BinaryRow row0 = binaryRow(1, 2);
        BinaryRow row1 = binaryRow(3, 4);

        long commitTs = START_BUILDING_INDEX_ACTIVATION_TS_MILLS;
        long beginTs = CREATE_INDEX_ACTIVATION_TS_MILLS - 1;

        addWriteCommitted(rowId, row0, commitTs);
        addWrite(rowId, row1, txId(beginTs));

        assertThat(
                chooser.chooseForBuildIndex(rowId),
                contains(expBinaryRowAndRowId(rowId, row1), expBinaryRowAndRowId(rowId, row0))
        );
    }

    private void addWrite(RowId rowId, BinaryRow row, UUID txId) {
        partitionDataStorage.runConsistently(locker -> {
            locker.lock(rowId);

            partitionDataStorage.addWrite(rowId, row, txId, ZONE_ID, TABLE_ID, PARTITION_ID);

            return null;
        });
    }

    private void addWriteCommitted(RowId rowId, BinaryRow row, long commitTs) {
        partitionDataStorage.runConsistently(locker -> {
            locker.lock(rowId);

            partitionDataStorage.addWriteCommitted(rowId, row, hybridTimestamp(commitTs));

            return null;
        });
    }

    private static UUID txId(long beginTs) {
        return transactionId(hybridTimestamp(beginTs), 1);
    }

    private static BinaryRow binaryRow(int key, int val) {
        return SchemaTestUtils.binaryRow(SCHEMA, key, val);
    }

    private static Matcher<BinaryRowAndRowId> expBinaryRowAndRowId(RowId rowId, @Nullable BinaryRow row) {
        return equalToBinaryRowAndRowId(new BinaryRowAndRowId(row, rowId));
    }
}
