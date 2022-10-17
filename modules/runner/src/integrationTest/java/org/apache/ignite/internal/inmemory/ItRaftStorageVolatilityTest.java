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

package org.apache.ignite.internal.inmemory;

import static ca.seinesoftware.hamcrest.path.PathMatcher.exists;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;
import static org.rocksdb.RocksDB.DEFAULT_COLUMN_FAMILY;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.ignite.internal.AbstractClusterIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.raft.configuration.EntryCountBudgetChange;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.schema.testutils.SchemaConfigurationConverter;
import org.apache.ignite.internal.schema.testutils.builder.SchemaBuilders;
import org.apache.ignite.internal.schema.testutils.definition.ColumnType;
import org.apache.ignite.internal.schema.testutils.definition.TableDefinition;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.VolatilePageMemoryDataStorageChange;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.junit.jupiter.api.Test;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;

/**
 * Tests for making sure that RAFT groups corresponding to partition stores of in-memory tables use volatile
 * storages for storing RAFT meta and RAFT log, while they are persistent for persistent storages.
 */
class ItRaftStorageVolatilityTest extends AbstractClusterIntegrationTest {
    private static final String TABLE_NAME = "test";

    @Override
    protected int nodes() {
        return 1;
    }

    @Test
    void raftMetaStorageIsVolatileForVolatilePartitions() {
        createInMemoryTable();

        IgniteImpl ignite = node(0);

        assertThat(partitionRaftMetaPaths(ignite), everyItem(not(exists())));
    }

    private void createInMemoryTable() {
        executeSql("CREATE TABLE " + TABLE_NAME + " (k int, v int, CONSTRAINT PK PRIMARY KEY (k)) ENGINE aimem");
    }

    /**
     * Returns paths for 'meta' directories corresponding to Raft meta storages for partitions of the test table.
     *
     * @param ignite Ignite instance.
     * @return Paths for 'meta' directories corresponding to Raft meta storages for partitions of the test table.
     */
    private List<Path> partitionRaftMetaPaths(IgniteImpl ignite) {
        try (Stream<Path> paths = Files.list(WORK_DIR.resolve(ignite.name()))) {
            return paths
                    .filter(path -> isPartitionDir(path, ignite))
                    .map(path -> path.resolve("meta"))
                    .collect(toList());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean isPartitionDir(Path path, IgniteImpl ignite) {
        return path.getFileName().toString().startsWith(testTablePartitionPrefix(ignite));
    }

    private String testTablePartitionPrefix(IgniteImpl ignite) {
        return testTableId(ignite) + "_part_";
    }

    private UUID testTableId(IgniteImpl ignite) {
        TableManager tables = (TableManager) ignite.tables();
        return tables.tableImpl(TABLE_NAME).tableId();
    }

    @Test
    void raftLogStorageIsVolatileForVolatilePartitions() throws Exception {
        createInMemoryTable();

        IgniteImpl ignite = node(0);
        String nodeName = ignite.name();
        String tablePartitionPrefix = testTablePartitionPrefix(ignite);

        node(0).close();

        Path logRocksDbDir = WORK_DIR.resolve(nodeName).resolve("log");

        List<ColumnFamilyDescriptor> cfDescriptors = List.of(
                // Column family to store configuration log entry.
                new ColumnFamilyDescriptor("Configuration".getBytes(UTF_8)),
                // Default column family to store user data log entry.
                new ColumnFamilyDescriptor(DEFAULT_COLUMN_FAMILY)
        );

        List<ColumnFamilyHandle> cfHandles = new ArrayList<>();

        try (RocksDB db = RocksDB.open(logRocksDbDir.toString(), cfDescriptors, cfHandles)) {
            assertThatFamilyHasNoDataForPartition(db, tablePartitionPrefix, cfHandles.get(0));
            assertThatFamilyHasNoDataForPartition(db, tablePartitionPrefix, cfHandles.get(1));
        }
    }

    private void assertThatFamilyHasNoDataForPartition(RocksDB db, String tablePartitionPrefix, ColumnFamilyHandle cfHandle) {
        try (
                ReadOptions readOptions = new ReadOptions().setIterateLowerBound(new Slice(tablePartitionPrefix.getBytes(UTF_8)));
                RocksIterator iterator = db.newIterator(cfHandle, readOptions)
        ) {
            iterator.seekToFirst();

            if (iterator.isValid()) {
                String key = new String(iterator.key(), UTF_8);
                assertThat(key, not(startsWith(tablePartitionPrefix)));
            }
        }
    }

    @Test
    void raftMetaStorageIsPersistentForPersistentPartitions() {
        createPersistentTable();

        IgniteImpl ignite = node(0);

        assertThat(partitionRaftMetaPaths(ignite), everyItem(exists()));
    }

    private void createPersistentTable() {
        executeSql("CREATE TABLE " + TABLE_NAME + " (k int, v int, CONSTRAINT PK PRIMARY KEY (k)) ENGINE rocksdb");
    }

    @Test
    void raftLogStorageIsPersistentForPersistentPartitions() throws Exception {
        createPersistentTable();

        IgniteImpl ignite = node(0);
        String nodeName = ignite.name();
        String tablePartitionPrefix = testTablePartitionPrefix(ignite);

        node(0).close();

        Path logRocksDbDir = WORK_DIR.resolve(nodeName).resolve("log");

        List<ColumnFamilyDescriptor> cfDescriptors = List.of(
                // Column family to store configuration log entry.
                new ColumnFamilyDescriptor("Configuration".getBytes(UTF_8)),
                // Default column family to store user data log entry.
                new ColumnFamilyDescriptor(DEFAULT_COLUMN_FAMILY)
        );

        List<ColumnFamilyHandle> cfHandles = new ArrayList<>();

        try (RocksDB db = RocksDB.open(logRocksDbDir.toString(), cfDescriptors, cfHandles)) {
            assertThatFamilyHasDataForPartition(db, tablePartitionPrefix, cfHandles.get(0));
            assertThatFamilyHasDataForPartition(db, tablePartitionPrefix, cfHandles.get(1));
        }
    }

    private void assertThatFamilyHasDataForPartition(RocksDB db, String tablePartitionPrefix, ColumnFamilyHandle cfHandle) {
        try (
                ReadOptions readOptions = new ReadOptions().setIterateLowerBound(new Slice(tablePartitionPrefix.getBytes(UTF_8)));
                RocksIterator iterator = db.newIterator(cfHandle, readOptions)
        ) {
            iterator.seekToFirst();

            assertThat(iterator.isValid(), is(true));

            String key = new String(iterator.key(), UTF_8);
            assertThat(key, startsWith(tablePartitionPrefix));
        }
    }

    @Test
    void inMemoryTableWorks() {
        createInMemoryTable();

        executeSql("INSERT INTO " + TABLE_NAME + "(k, v) VALUES (1, 101)");

        List<List<Object>> tuples = executeSql("SELECT k, v FROM " + TABLE_NAME);

        assertThat(tuples, equalTo(List.of(List.of(1, 101))));
    }

    @Test
    void logSpillsOutToDisk() {
        node(0).nodeConfiguration().getConfiguration(RaftConfiguration.KEY).change(cfg -> {
            cfg.changeVolatileRaft(change -> {
                change.changeLogStorage(budgetChange -> budgetChange.convert(EntryCountBudgetChange.class).changeEntriesCountLimit(1));
            });
        });

        createTableWithMaxOneInMemoryEntryAllowed("PERSON");

        executeSql("INSERT INTO PERSON(ID, NAME) VALUES (1, 'JOHN')");
        executeSql("INSERT INTO PERSON(ID, NAME) VALUES (2, 'JANE')");
    }

    private void createTableWithMaxOneInMemoryEntryAllowed(String tableName) {
        TableDefinition tableDef = SchemaBuilders.tableBuilder("PUBLIC", tableName).columns(
                SchemaBuilders.column("ID", ColumnType.INT32).build(),
                SchemaBuilders.column("NAME", ColumnType.string()).asNullable(true).build()
        ).withPrimaryKey("ID").build();

        await(((TableManager) node(0).tables()).createTableAsync(tableName, tableChange -> {
            SchemaConfigurationConverter.convert(tableDef, tableChange)
                    .changePartitions(1)
                    .changeDataStorage(storageChange -> {
                        storageChange.convert(VolatilePageMemoryDataStorageChange.class);
                    });
        }));
    }
}
