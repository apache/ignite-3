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

import java.util.UUID;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.configuration.TableConfiguration;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.engine.StorageEngine;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Base test for MV partition storages.
 */
@ExtendWith(ConfigurationExtension.class)
public abstract class BaseMvPartitionStorageTest extends BaseMvStoragesTest {
    protected static final int PARTITION_ID = 1;

    protected static final UUID COMMIT_TABLE_ID = UUID.randomUUID();

    protected static final UUID TX_ID = newTransactionId();

    protected static final RowId ROW_ID = new RowId(PARTITION_ID);

    protected static final TestKey KEY = new TestKey(10, "foo");

    protected static final BinaryRow TABLE_ROW = binaryRow(KEY, new TestValue(20, "bar"));

    protected static final BinaryRow TABLE_ROW2 = binaryRow(KEY, new TestValue(30, "bar"));

    protected @InjectConfiguration("mock.tables.foo = {}") TablesConfiguration tablesCfg;

    protected StorageEngine engine;

    protected MvTableStorage table;

    protected MvPartitionStorage storage;

    /**
     * Creates a new transaction id.
     */
    protected static UUID newTransactionId() {
        return UUID.randomUUID();
    }

    protected abstract StorageEngine createEngine();

    @BeforeEach
    protected void setUp() {
        TableConfiguration tableCfg = tablesCfg.tables().get("foo");

        engine = createEngine();

        engine.start();

        tableCfg.dataStorage().change(ds -> ds.convert(engine.name())).join();

        table = engine.createMvTable(tableCfg, tablesCfg);

        table.start();

        storage = table.getOrCreateMvPartition(PARTITION_ID);
    }

    @AfterEach
    protected void tearDown() throws Exception {
        IgniteUtils.closeAll(
                storage == null ? null : storage::close,
                table == null ? null : table::stop,
                engine == null ? null : engine::stop
        );
    }

    /**
     * Reads a row.
     */
    @Nullable
    protected BinaryRow read(RowId rowId, HybridTimestamp timestamp) {
        ReadResult readResult = storage.read(rowId, timestamp);

        return readResult.binaryRow();
    }

    /**
     * Scans partition.
     */
    protected PartitionTimestampCursor scan(HybridTimestamp timestamp) {
        return storage.scan(timestamp);
    }

    /**
     * Scans versions.
     */
    protected Cursor<ReadResult> scan(RowId rowId) {
        return storage.scanVersions(rowId);
    }

    /**
     * Inserts a row inside of consistency closure.
     */
    protected RowId insert(@Nullable BinaryRow binaryRow, UUID txId) {
        return insert(binaryRow, txId, null);
    }

    /**
     * Inserts a row inside of consistency closure.
     */
    protected RowId insert(@Nullable BinaryRow binaryRow, UUID txId, @Nullable UUID explicitRowId) {
        RowId rowId = explicitRowId == null ? new RowId(PARTITION_ID) : new RowId(PARTITION_ID, explicitRowId);

        storage.runConsistently(() -> storage.addWrite(rowId, binaryRow, txId, UUID.randomUUID(), 0));

        return rowId;
    }

    /**
     * Adds/updates a write-intent inside of consistency closure.
     */
    protected BinaryRow addWrite(RowId rowId, @Nullable BinaryRow binaryRow, UUID txId) {
        return storage.runConsistently(() -> storage.addWrite(rowId, binaryRow, txId, COMMIT_TABLE_ID, PARTITION_ID));
    }

    /**
     * Commits write-intent inside of consistency closure.
     */
    protected void commitWrite(RowId rowId, HybridTimestamp tsExact) {
        storage.runConsistently(() -> {
            storage.commitWrite(rowId, tsExact);

            return null;
        });
    }

    /**
     * Writes a row to storage like if it was first added using {@link MvPartitionStorage#addWrite(RowId, BinaryRow, UUID, UUID, int)}
     * and immediately committed with {@link MvPartitionStorage#commitWrite(RowId, HybridTimestamp)}.
     */
    protected void addWriteCommitted(RowId rowId, @Nullable BinaryRow row, HybridTimestamp commitTimestamp) {
        storage.runConsistently(() -> {
            storage.addWriteCommitted(rowId, row, commitTimestamp);

            return null;
        });
    }

    protected HybridTimestamp addAndCommit(@Nullable BinaryRow binaryRow) {
        HybridTimestamp commitTs = clock.now();

        addWrite(ROW_ID, binaryRow, TX_ID);
        commitWrite(ROW_ID, commitTs);

        return commitTs;
    }

    /**
     * Aborts write-intent inside of consistency closure.
     */
    protected BinaryRow abortWrite(RowId rowId) {
        return storage.runConsistently(() -> storage.abortWrite(rowId));
    }

    protected BinaryRowAndRowId pollForVacuum(HybridTimestamp lowWatermark) {
        return storage.runConsistently(() -> storage.pollForVacuum(lowWatermark));
    }
}
