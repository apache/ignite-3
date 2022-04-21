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

import static org.apache.ignite.internal.storage.index.SortedIndexMvStorage.BACKWARDS;
import static org.apache.ignite.internal.storage.index.SortedIndexMvStorage.FORWARD;
import static org.apache.ignite.internal.storage.index.SortedIndexMvStorage.GREATER;
import static org.apache.ignite.internal.storage.index.SortedIndexMvStorage.GREATER_OR_EQUAL;
import static org.apache.ignite.internal.storage.index.SortedIndexMvStorage.LESS;
import static org.apache.ignite.internal.storage.index.SortedIndexMvStorage.LESS_OR_EQUAL;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.ignite.configuration.schemas.table.ColumnChange;
import org.apache.ignite.configuration.schemas.table.SortedIndexChange;
import org.apache.ignite.configuration.schemas.table.SortedIndexConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.configuration.schemas.table.TableView;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.marshaller.MarshallerException;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.storage.chm.TestConcurrentHashMapStorageEngine;
import org.apache.ignite.internal.storage.chm.schema.TestConcurrentHashMapDataStorageConfigurationSchema;
import org.apache.ignite.internal.storage.index.IndexRowPrefix;
import org.apache.ignite.internal.storage.index.SortedIndexMvStorage;
import org.apache.ignite.internal.storage.index.SortedIndexMvStorage.IndexRowEx;
import org.apache.ignite.internal.tx.Timestamp;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.lang.IgniteException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Base test for MV index storages.
 */
@ExtendWith(ConfigurationExtension.class)
public abstract class AbstractSortedIndexMvStorageTest extends BaseMvStoragesTest {
    protected static final String INDEX1 = "asc_asc";
    protected static final String INDEX2 = "asc_desc";

    protected TableConfiguration tableCfg;

    @BeforeEach
    void setUp(@InjectConfiguration(
            polymorphicExtensions = {SortedIndexConfigurationSchema.class, TestConcurrentHashMapDataStorageConfigurationSchema.class},
            // This value only required for configuration validity, it's not used otherwise.
            value = "mock.dataStorage.name = " + TestConcurrentHashMapStorageEngine.ENGINE_NAME
    ) TableConfiguration tableCfg) {
        tableCfg.change(tableChange -> tableChange
                .changePartitions(1)
                .changePrimaryKey(pk -> pk.changeColumns("intKey", "strKey"))
                .changeColumns(columns -> columns
                        .create("intKey", column("INT32"))
                        .create("strKey", column("STRING"))
                        .create("intVal", column("INT32"))
                        .create("strVal", column("STRING"))
                )
                .changeIndices(indexes -> indexes
                        .create(INDEX1, idx -> idx.convert(SortedIndexChange.class).changeColumns(idxColumns -> idxColumns
                                .create("strVal", c -> c.changeAsc(true))
                                .create("intVal", c -> c.changeAsc(true))
                        ))
                        .create(INDEX2, idx -> idx.convert(SortedIndexChange.class).changeColumns(idxColumns -> idxColumns
                                .create("strVal", c -> c.changeAsc(true))
                                .create("intVal", c -> c.changeAsc(false))
                        ))
                )
        ).join();

        this.tableCfg = tableCfg;
    }

    private static Consumer<ColumnChange> column(String typeName) {
        return c -> c.changeNullable(false).changeType(type -> type.changeType(typeName));
    }

    /**
     * Creates a storage instance for testing.
     */
    protected abstract MvPartitionStorage partitionStorage();

    /**
     * Creates a storage instanc efor testing.
     */
    protected abstract SortedIndexMvStorage createIndexStorage(String name, TableView tableCfg);

    @Test
    public void testEmpty() throws Exception {
        SortedIndexMvStorage index1 = createIndexStorage(INDEX1, tableCfg.value());
        SortedIndexMvStorage index2 = createIndexStorage(INDEX2, tableCfg.value());

        assertEquals(List.of(), convert(index1.scan(null, null, (byte) 0, null, null)));
        assertEquals(List.of(), convert(index2.scan(null, null, (byte) 0, null, null)));
    }

    @Test
    public void testBoundsAndOrder() throws Exception {
        SortedIndexMvStorage index1 = createIndexStorage(INDEX1, tableCfg.value());
        SortedIndexMvStorage index2 = createIndexStorage(INDEX2, tableCfg.value());

        TestValue val9010 = new TestValue(90, "10");
        TestValue val8010 = new TestValue(80, "10");
        TestValue val9020 = new TestValue(90, "20");
        TestValue val8020 = new TestValue(80, "20");

        insert(new TestKey(1, "1"), val9010, null);
        insert(new TestKey(2, "2"), val8010, null);
        insert(new TestKey(3, "3"), val9020, null);
        insert(new TestKey(4, "4"), val8020, null);

        // Test without bounds.
        assertEquals(List.of(val8010, val9010, val8020, val9020), convert(index1.scan(
                null, null, FORWARD, null, null
        )));

        assertEquals(List.of(val9020, val8020, val9010, val8010), convert(index1.scan(
                null, null, BACKWARDS, null, null
        )));

        assertEquals(List.of(val9010, val8010, val9020, val8020), convert(index2.scan(
                null, null, FORWARD, null, null
        )));

        assertEquals(List.of(val8020, val9020, val8010, val9010), convert(index2.scan(
                null, null, BACKWARDS, null, null
        )));

        // Lower bound exclusive.
        assertEquals(List.of(val8020, val9020), convert(index1.scan(
                prefix("10"), null, GREATER | FORWARD, null, null
        )));

        assertEquals(List.of(val9020, val8020), convert(index1.scan(
                prefix("10"), null, GREATER | BACKWARDS, null, null
        )));

        assertEquals(List.of(val9020, val8020), convert(index2.scan(
                prefix("10"), null, GREATER | FORWARD, null, null
        )));

        assertEquals(List.of(val8020, val9020), convert(index2.scan(
                prefix("10"), null, GREATER | BACKWARDS, null, null
        )));

        // Lower bound inclusive.
        assertEquals(List.of(val8010, val9010, val8020, val9020), convert(index1.scan(
                prefix("10"), null, GREATER_OR_EQUAL | FORWARD, null, null
        )));

        assertEquals(List.of(val9020, val8020, val9010, val8010), convert(index1.scan(
                prefix("10"), null, GREATER_OR_EQUAL | BACKWARDS, null, null
        )));

        assertEquals(List.of(val9010, val8010, val9020, val8020), convert(index2.scan(
                prefix("10"), null, GREATER_OR_EQUAL | FORWARD, null, null
        )));

        assertEquals(List.of(val8020, val9020, val8010, val9010), convert(index2.scan(
                prefix("10"), null, GREATER_OR_EQUAL | BACKWARDS, null, null
        )));

        // Upper bound exclusive.
        assertEquals(List.of(val8010, val9010), convert(index1.scan(
                null, prefix("20"), LESS | FORWARD, null, null
        )));

        assertEquals(List.of(val9010, val8010), convert(index1.scan(
                null, prefix("20"), LESS | BACKWARDS, null, null
        )));

        assertEquals(List.of(val9010, val8010), convert(index2.scan(
                null, prefix("20"), LESS | FORWARD, null, null
        )));

        assertEquals(List.of(val8010, val9010), convert(index2.scan(
                null, prefix("20"), LESS | BACKWARDS, null, null
        )));

        // Upper bound inclusive.
        assertEquals(List.of(val8010, val9010, val8020, val9020), convert(index1.scan(
                null, prefix("20"), LESS_OR_EQUAL | FORWARD, null, null
        )));

        assertEquals(List.of(val9020, val8020, val9010, val8010), convert(index1.scan(
                null, prefix("20"), LESS_OR_EQUAL | BACKWARDS, null, null
        )));

        assertEquals(List.of(val9010, val8010, val9020, val8020), convert(index2.scan(
                null, prefix("20"), LESS_OR_EQUAL | FORWARD, null, null
        )));

        assertEquals(List.of(val8020, val9020, val8010, val9010), convert(index2.scan(
                null, prefix("20"), LESS_OR_EQUAL | BACKWARDS, null, null
        )));
    }

    @Test
    public void testAbort() throws Exception {
        MvPartitionStorage pk = partitionStorage();

        SortedIndexMvStorage index = createIndexStorage(INDEX1, tableCfg.value());

        TestKey key = new TestKey(1, "1");
        TestValue val = new TestValue(10, "10");
        IgniteRowId rowId = UuidIgniteRowId.randomRowId(0);

        pk.addWrite(rowId, binaryRow(key, val), UUID.randomUUID());

        // Timestamp is null.
        assertEquals(List.of(val), convert(index.scan(null, null, 0, null, null)));

        // Timestamp is not null.
        assertEquals(List.of(), convert(index.scan(null, null, 0, Timestamp.nextVersion(), null)));

        // Abort write.
        pk.abortWrite(rowId);

        // Timestamp is null.
        assertEquals(List.of(), convert(index.scan(null, null, 0, null, null)));

        // Timestamp is not null.
        assertEquals(List.of(), convert(index.scan(null, null, 0, Timestamp.nextVersion(), null)));
    }

    @Test
    public void testAbortRemove() throws Exception {
        MvPartitionStorage pk = partitionStorage();

        SortedIndexMvStorage index = createIndexStorage(INDEX1, tableCfg.value());

        TestKey key = new TestKey(1, "1");
        TestValue val = new TestValue(10, "10");
        IgniteRowId rowId = UuidIgniteRowId.randomRowId(0);

        Timestamp insertTs = Timestamp.nextVersion();

        pk.addWrite(rowId, binaryRow(key, val), UUID.randomUUID());

        pk.commitWrite(rowId, insertTs);

        // Remove.
        pk.addWrite(rowId, null, UUID.randomUUID());

        // Timestamp is null.
        assertEquals(List.of(), convert(index.scan(null, null, 0, null, null)));

        // Timestamp is not null.
        assertEquals(List.of(val), convert(index.scan(null, null, 0, insertTs, null)));
        assertEquals(List.of(val), convert(index.scan(null, null, 0, Timestamp.nextVersion(), null)));

        // Abort remove.
        pk.abortWrite(rowId);

        // Timestamp is null.
        assertEquals(List.of(val), convert(index.scan(null, null, 0, null, null)));

        // Timestamp is not null.
        assertEquals(List.of(val), convert(index.scan(null, null, 0, insertTs, null)));
        assertEquals(List.of(val), convert(index.scan(null, null, 0, Timestamp.nextVersion(), null)));
    }

    @Test
    public void testCommit() throws Exception {
        MvPartitionStorage pk = partitionStorage();

        SortedIndexMvStorage index = createIndexStorage(INDEX1, tableCfg.value());

        TestKey key = new TestKey(1, "1");
        TestValue val = new TestValue(10, "10");
        IgniteRowId rowId = UuidIgniteRowId.randomRowId(0);

        pk.addWrite(rowId, binaryRow(key, val), UUID.randomUUID());

        // Timestamp is null.
        assertEquals(List.of(val), convert(index.scan(null, null, 0, null, null)));

        // Timestamp is not null.
        assertEquals(List.of(), convert(index.scan(null, null, 0, Timestamp.nextVersion(), null)));

        // Commit write.
        Timestamp commitTs = Timestamp.nextVersion();
        pk.commitWrite(rowId, commitTs);

        // Timestamp is null.
        assertEquals(List.of(val), convert(index.scan(null, null, 0, null, null)));

        // Timestamp is not null.
        assertEquals(List.of(val), convert(index.scan(null, null, 0, commitTs, null)));
        assertEquals(List.of(val), convert(index.scan(null, null, 0, Timestamp.nextVersion(), null)));
    }

    @Test
    public void testCommitRemove() throws Exception {
        MvPartitionStorage pk = partitionStorage();

        SortedIndexMvStorage index = createIndexStorage(INDEX1, tableCfg.value());

        TestKey key = new TestKey(1, "1");
        TestValue val = new TestValue(10, "10");
        IgniteRowId rowId = UuidIgniteRowId.randomRowId(0);

        Timestamp insertTs = Timestamp.nextVersion();

        pk.addWrite(rowId, binaryRow(key, val), UUID.randomUUID());

        pk.commitWrite(rowId, insertTs);

        // Remove.
        pk.addWrite(rowId, null, UUID.randomUUID());

        // Timestamp is null.
        assertEquals(List.of(), convert(index.scan(null, null, 0, null, null)));

        // Timestamp is not null.
        assertEquals(List.of(val), convert(index.scan(null, null, 0, insertTs, null)));
        assertEquals(List.of(val), convert(index.scan(null, null, 0, Timestamp.nextVersion(), null)));

        // Commit remove.
        Timestamp removeTs = Timestamp.nextVersion();
        pk.commitWrite(rowId, removeTs);

        // Timestamp is null.
        assertEquals(List.of(), convert(index.scan(null, null, 0, null, null)));

        // Timestamp is not null.
        assertEquals(List.of(val), convert(index.scan(null, null, 0, insertTs, null)));

        assertEquals(List.of(), convert(index.scan(null, null, 0, removeTs, null)));
        assertEquals(List.of(), convert(index.scan(null, null, 0, Timestamp.nextVersion(), null)));
    }

    protected void insert(TestKey key, TestValue value, Timestamp ts) {
        MvPartitionStorage pk = partitionStorage();

        BinaryRow binaryRow = binaryRow(key, value);

        IgniteRowId rowId = UuidIgniteRowId.randomRowId(0);

        pk.addWrite(rowId, binaryRow, UUID.randomUUID());

        pk.commitWrite(rowId, ts == null ? Timestamp.nextVersion() : ts);
    }

    protected IndexRowPrefix prefix(String val) {
        return () -> new Object[]{val};
    }

    protected List<TestValue> convert(Cursor<IndexRowEx> cursor) throws Exception {
        try (cursor) {
            return StreamSupport.stream(cursor.spliterator(), false)
                    .map(indexRowEx -> {
                        try {
                            return kvMarshaller.unmarshalValue(new Row(schemaDescriptor, indexRowEx.row()));
                        } catch (MarshallerException e) {
                            throw new IgniteException(e);
                        }
                    })
                    .collect(Collectors.toList());
        }
    }
}
