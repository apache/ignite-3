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
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.not;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.raft.configuration.EntryCountBudgetChange;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.junit.jupiter.api.Test;

/**
 * Tests for making sure that RAFT groups corresponding to partition stores of in-memory tables use volatile
 * storages for storing RAFT meta and RAFT log, while they are persistent for persistent storages.
 */
class ItRaftStorageVolatilityTest extends ClusterPerTestIntegrationTest {
    private static final String TABLE_NAME = "test";

    @Override
    protected int initialNodes() {
        return 1;
    }

    @Test
    void raftMetaStorageIsVolatileForVolatilePartitions() {
        createInMemoryTable();

        IgniteImpl ignite = node(0);

        assertThat(partitionRaftMetaPaths(ignite), everyItem(not(exists())));
    }

    private void createInMemoryTable() {
        executeSql("CREATE ZONE ZONE_" + TABLE_NAME + " ENGINE aimem");

        executeSql("CREATE TABLE " + TABLE_NAME
                + " (k int, v int, CONSTRAINT PK PRIMARY KEY (k)) WITH PRIMARY_ZONE='ZONE_"
                + TABLE_NAME.toUpperCase() + "'");
    }

    /**
     * Returns paths for 'meta' directories corresponding to Raft meta storages for partitions of the test table.
     *
     * @param ignite Ignite instance.
     * @return Paths for 'meta' directories corresponding to Raft meta storages for partitions of the test table.
     */
    private List<Path> partitionRaftMetaPaths(IgniteImpl ignite) {
        try (Stream<Path> paths = Files.list(workDir.resolve(ignite.name()))) {
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

    private int testTableId(IgniteImpl ignite) {
        TableManager tables = (TableManager) ignite.tables();
        return tables.tableImpl(TABLE_NAME).tableId();
    }

    @Test
    void raftMetaStorageIsPersistentForPersistentPartitions() {
        createPersistentTable();

        IgniteImpl ignite = node(0);

        assertThat(partitionRaftMetaPaths(ignite), everyItem(exists()));
    }

    private void createPersistentTable() {
        executeSql("CREATE ZONE ZONE_" + TABLE_NAME + " ENGINE rocksdb");

        executeSql("CREATE TABLE " + TABLE_NAME
                + " (k int, v int, CONSTRAINT PK PRIMARY KEY (k)) WITH PRIMARY_ZONE='ZONE_"
                + TABLE_NAME.toUpperCase() + "'");
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
        CompletableFuture<Void> configUpdateFuture = node(0).nodeConfiguration().getConfiguration(RaftConfiguration.KEY).change(cfg -> {
            cfg.changeVolatileRaft(change -> {
                change.changeLogStorage(budgetChange -> budgetChange.convert(EntryCountBudgetChange.class).changeEntriesCountLimit(1));
            });
        });
        assertThat(configUpdateFuture, willCompleteSuccessfully());

        cluster.doInSession(0, session -> {
            session.execute(null, "create zone zone1 engine aimem with partitions=1, replicas=1");
            session.execute(null, "create table " + tableName + " (id int primary key, name varchar) with primary_zone='ZONE1'");
        });
    }
}
