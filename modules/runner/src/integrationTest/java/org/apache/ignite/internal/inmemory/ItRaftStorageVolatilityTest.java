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
import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_AIMEM_PROFILE_NAME;
import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_ROCKSDB_PROFILE_NAME;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.TestWrappers.unwrapTableManager;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;
import static org.rocksdb.RocksDB.DEFAULT_COLUMN_FAMILY;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.raft.configuration.EntryCountBudgetChange;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.raft.configuration.RaftExtensionConfiguration;
import org.apache.ignite.internal.raft.util.SharedLogStorageFactoryUtils;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.testframework.WithSystemProperty;
import org.apache.ignite.table.QualifiedName;
import org.junit.jupiter.api.Test;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;

/**
 * Tests for making sure that RAFT groups corresponding to partition stores of in-memory tables use volatile storages for storing RAFT meta
 * and RAFT log, while they are persistent for persistent storages.
 */
@WithSystemProperty(key = SharedLogStorageFactoryUtils.LOGIT_STORAGE_ENABLED_PROPERTY, value = "false")
class ItRaftStorageVolatilityTest extends ClusterPerTestIntegrationTest {
    private static final String TABLE_NAME = "test";

    @Override
    protected int initialNodes() {
        return 1;
    }

    @Test
    void raftMetaStorageIsVolatileForVolatilePartitions() {
        createInMemoryTable();

        IgniteImpl ignite = unwrapIgniteImpl(node(0));

        int zoneId = testZoneId(ignite);

        // Check that there are no meta files for partitions of the table.
        assertThat(
                partitionRaftMetaPaths(ignite, p -> p.getFileName().toString().startsWith(zoneId + "_part_")),
                everyItem(not(exists())));

        // The default zone still exists and uses persistent profile.
        assertThat(partitionRaftMetaPaths(ignite, p -> p.getFileName().toString().startsWith("0_part_")), everyItem(exists()));
    }

    private void createInMemoryTable() {
        executeSql("CREATE ZONE ZONE_" + TABLE_NAME + " STORAGE PROFILES ['" + DEFAULT_AIMEM_PROFILE_NAME + "']");

        executeSql("CREATE TABLE " + TABLE_NAME
                + " (k int, v int, CONSTRAINT PK PRIMARY KEY (k)) ZONE ZONE_" + TABLE_NAME + " STORAGE PROFILE '"
                + DEFAULT_AIMEM_PROFILE_NAME + "'");
    }

    /**
     * Returns paths for 'meta' directories corresponding to Raft meta storages for partitions of the test table.
     *
     * @param ignite Ignite instance.
     * @return Paths for 'meta' directories corresponding to Raft meta storages for partitions of the test table.
     */
    private static List<Path> partitionRaftMetaPaths(IgniteImpl ignite) {
        return partitionRaftMetaPaths(ignite, p -> true);
    }

    /**
     * Returns paths for 'meta' directories corresponding to Raft meta storages for partitions of the test table.
     *
     * @param ignite Ignite instance.
     * @param predicate Predicate to filter paths.
     * @return Paths for 'meta' directories corresponding to Raft meta storages for partitions of the test table.
     */
    private static List<Path> partitionRaftMetaPaths(IgniteImpl ignite, Predicate<Path> predicate) {
        try (Stream<Path> paths = Files.list(ignite.partitionsWorkDir().metaPath())) {
            return paths.filter(predicate).collect(toList());
        } catch (NoSuchFileException e) {
            return List.of();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static String testZonePartitionPrefix(IgniteImpl ignite) {
        return testZoneId(ignite) + "_part_";
    }

    private static int testZoneId(IgniteImpl ignite) {
        TableManager tables = unwrapTableManager(ignite.tables());
        return tables.tableView(QualifiedName.fromSimple(TABLE_NAME)).zoneId();
    }

    @Test
    void raftLogStorageIsVolatileForVolatilePartitions() throws Exception {
        createInMemoryTable();

        IgniteImpl ignite = unwrapIgniteImpl(node(0));
        String zonePartitionPrefix = testZonePartitionPrefix(ignite);

        stopNode(0);

        Path logRocksDbDir = ignite.partitionsWorkDir().raftLogPath();

        List<ColumnFamilyDescriptor> cfDescriptors = cfDescriptors();

        List<ColumnFamilyHandle> cfHandles = new ArrayList<>();

        try (RocksDB db = RocksDB.open(logRocksDbDir.toString(), cfDescriptors, cfHandles)) {
            assertThatFamilyHasNoDataForPartition(db, zonePartitionPrefix, cfHandles.get(1));
            assertThatFamilyHasNoDataForPartition(db, zonePartitionPrefix, cfHandles.get(2));
        }
    }

    private static void assertThatFamilyHasNoDataForPartition(RocksDB db, String tablePartitionPrefix, ColumnFamilyHandle cfHandle) {
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

        IgniteImpl ignite = unwrapIgniteImpl(node(0));

        assertThat(partitionRaftMetaPaths(ignite), everyItem(exists()));
    }

    private void createPersistentTable() {
        executeSql("CREATE ZONE ZONE_" + TABLE_NAME
                + " STORAGE PROFILES ['" + DEFAULT_ROCKSDB_PROFILE_NAME + "']");

        executeSql("CREATE TABLE " + TABLE_NAME
                + " (k int, v int, CONSTRAINT PK PRIMARY KEY (k)) "
                + "ZONE ZONE_" + TABLE_NAME.toUpperCase() + " "
                + "STORAGE PROFILE '" + DEFAULT_ROCKSDB_PROFILE_NAME + "' ");
    }

    @Test
    void raftLogStorageIsPersistentForPersistentPartitions() throws Exception {
        createPersistentTable();

        IgniteImpl ignite = unwrapIgniteImpl(node(0));
        String partitionPrefix = testZonePartitionPrefix(ignite);

        stopNode(0);

        Path logRocksDbDir = ignite.partitionsWorkDir().raftLogPath();

        List<ColumnFamilyDescriptor> cfDescriptors = cfDescriptors();

        List<ColumnFamilyHandle> cfHandles = new ArrayList<>();

        try (RocksDB db = RocksDB.open(logRocksDbDir.toString(), cfDescriptors, cfHandles)) {
            assertThatFamilyHasDataForPartition(db, partitionPrefix, cfHandles.get(1));
            assertThatFamilyHasDataForPartition(db, partitionPrefix, cfHandles.get(2));
        }
    }

    private static List<ColumnFamilyDescriptor> cfDescriptors() {
        return List.of(
                new ColumnFamilyDescriptor("Meta".getBytes(UTF_8)),
                // Column family to store configuration log entry.
                new ColumnFamilyDescriptor("Configuration".getBytes(UTF_8)),
                // Default column family to store user data log entry.
                new ColumnFamilyDescriptor(DEFAULT_COLUMN_FAMILY)
        );
    }

    private static void assertThatFamilyHasDataForPartition(RocksDB db, String tablePartitionPrefix, ColumnFamilyHandle cfHandle) {
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
        createTableWithMaxOneInMemoryEntryAllowed("PERSON");

        executeSql("INSERT INTO PERSON(ID, NAME) VALUES (1, 'JOHN')");
        executeSql("INSERT INTO PERSON(ID, NAME) VALUES (2, 'JANE')");
    }

    @SuppressWarnings("resource")
    private void createTableWithMaxOneInMemoryEntryAllowed(String tableName) {
        RaftConfiguration raftConfiguration = unwrapIgniteImpl(node(0)).nodeConfiguration()
                .getConfiguration(RaftExtensionConfiguration.KEY).raft();
        CompletableFuture<Void> configUpdateFuture = raftConfiguration.change(cfg -> {
            cfg.changeVolatileRaft().changeLogStorageBudget().convert(EntryCountBudgetChange.class).changeEntriesCountLimit(1);
        });
        assertThat(configUpdateFuture, willCompleteSuccessfully());

        cluster.doInSession(0, session -> {
            session.execute(
                    "create zone zone1 (partitions 1, replicas 1) "
                            + "storage profiles ['" + DEFAULT_AIMEM_PROFILE_NAME + "']"
            );
            session.execute("create table " + tableName
                    + " (id int primary key, name varchar) zone ZONE1 storage profile '"
                    + DEFAULT_AIMEM_PROFILE_NAME + "'");
        });
    }
}
