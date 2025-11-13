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

package org.apache.ignite.internal;

import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.TestWrappers.unwrapTableViewInternal;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.testframework.flow.TestFlowUtils.subscribeToList;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.CompletableFutures.allOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.raft.storage.LogStorageFactory;
import org.apache.ignite.internal.raft.storage.impl.IgniteJraftServiceFactory;
import org.apache.ignite.internal.raft.util.SharedLogStorageFactoryUtils;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.table.OperationContext;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.table.TxContext;
import org.apache.ignite.internal.table.distributed.storage.InternalTableImpl;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.tostring.IgniteToStringInclude;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.raft.jraft.conf.ConfigurationManager;
import org.apache.ignite.raft.jraft.core.NodeImpl;
import org.apache.ignite.raft.jraft.option.LogStorageOptions;
import org.apache.ignite.raft.jraft.option.NodeOptions;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.storage.LogStorage;
import org.apache.ignite.tx.TransactionOptions;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Class for testing various scenarios with raft log truncation and node restarts that emulate the situation with fsync disabled for raft
 * groups associated with tables.
 */
// TODO: IGNITE-25501 Fix partition state after snapshot
public class ItTruncateRaftLogAndRestartNodesTest extends ClusterPerTestIntegrationTest {
    private static final IgniteLogger LOG = Loggers.forClass(ItTruncateRaftLogAndRestartNodesTest.class);

    private static final String ZONE_NAME = "TEST_ZONE";

    private static final String TABLE_NAME = "TEST_TABLE";

    @Override
    protected int initialNodes() {
        return 0;
    }

    @Disabled("https://issues.apache.org/jira/browse/IGNITE-25502")
    @Test
    void enterNodeWithIndexGreaterThanCurrentMajority() throws Exception {
        cluster.startAndInit(3);

        createZoneAndTablePerson(ZONE_NAME, TABLE_NAME, 3, 1);

        cluster.transferLeadershipTo(2, cluster.solePartitionId(ZONE_NAME, TABLE_NAME));

        var closableResources = new ArrayList<ManuallyCloseable>();

        try {
            ReplicationGroupId replicationGroup = cluster.solePartitionId(ZONE_NAME, TABLE_NAME);

            TestLogStorageFactory testLogStorageFactoryNode0 = createTestLogStorageFactory(0, replicationGroup);

            closableResources.add(testLogStorageFactoryNode0);

            TestLogStorageFactory testLogStorageFactoryNode1 = createTestLogStorageFactory(1, replicationGroup);
            closableResources.add(testLogStorageFactoryNode1);

            long lastLogIndexBeforeInsertNode0 = raftNodeImpl(0, replicationGroup).lastLogIndex();
            long lastLogIndexBeforeInsertNode1 = raftNodeImpl(1, replicationGroup).lastLogIndex();

            Person[] people = generatePeople(10);
            insertPeople(TABLE_NAME, people);

            // Let's flush the state machine so that the raft log on a node cannot be truncated.
            flushMvPartitionStorage(2, TABLE_NAME, 0);

            stopNodes(0, 1, 2);

            LogStorage logStorageNode0 = testLogStorageFactoryNode0.createLogStorage();
            closableResources.add(logStorageNode0::shutdown);

            LogStorage logStorageNode1 = testLogStorageFactoryNode1.createLogStorage();
            closableResources.add(logStorageNode1::shutdown);

            truncateRaftLogSuffixHalfOfChanges(logStorageNode0, lastLogIndexBeforeInsertNode0);
            truncateRaftLogSuffixHalfOfChanges(logStorageNode1, lastLogIndexBeforeInsertNode1);

            closeAllReversed(closableResources);
            closableResources.clear();

            startNodes(0, 1);

            awaitMajority(cluster.solePartitionId(ZONE_NAME, TABLE_NAME));

            startNode(2);

            assertThat(
                    toPeopleFromSqlRows(executeSql(selectPeopleDml(TABLE_NAME))),
                    arrayWithSize(Matchers.allOf(greaterThan(0), lessThan(people.length)))
            );

            for (int nodeIndex = 0; nodeIndex < 3; nodeIndex++) {
                assertThat(
                        "nodeIndex=" + nodeIndex,
                        scanPeopleFromAllPartitions(nodeIndex, TABLE_NAME),
                        arrayWithSize(Matchers.allOf(greaterThan(0), lessThan(people.length)))
                );
            }
        } finally {
            closeAllReversed(closableResources);
        }
    }

    private void createZoneAndTablePerson(String zoneName, String tableName, int replicas, int partitions) {
        executeSql(createZoneDdl(zoneName, replicas, partitions));
        executeSql(createTablePersonDdl(zoneName, tableName));
    }

    private void insertPeople(String tableName, Person... people) {
        for (Person person : people) {
            executeSql(insertPersonDml(tableName, person));
        }
    }

    private NodeImpl raftNodeImpl(int nodeIndex, ReplicationGroupId replicationGroupId) {
        NodeImpl[] node = {null};

        igniteImpl(nodeIndex).raftManager().forEach((raftNodeId, raftGroupService) -> {
            if (replicationGroupId.equals(raftNodeId.groupId())) {
                assertNull(
                        node[0],
                        String.format("NodeImpl already found: [nodeIndex=%s, replicationGroupId=%s]", nodeIndex, replicationGroupId)
                );

                node[0] = (NodeImpl) raftGroupService.getRaftNode();
            }
        });

        NodeImpl res = node[0];

        assertNotNull(res, String.format("Can't find NodeImpl: [nodeIndex=%s, replicationGroupId=%s]", nodeIndex, replicationGroupId));

        return res;
    }

    /**
     * Creates and prepares {@link TestLogStorageFactory} for {@link TestLogStorageFactory#createLogStorage} creation after the
     * corresponding node is stopped, so that there are no errors.
     */
    private TestLogStorageFactory createTestLogStorageFactory(int nodeIndex, ReplicationGroupId replicationGroupId) {
        IgniteImpl ignite = igniteImpl(nodeIndex);

        LogStorageFactory logStorageFactory = SharedLogStorageFactoryUtils.create(
                ignite.name(),
                ignite.partitionsWorkDir().raftLogPath()
        );

        NodeImpl nodeImpl = raftNodeImpl(nodeIndex, replicationGroupId);

        return new TestLogStorageFactory(logStorageFactory, nodeImpl.getOptions(), nodeImpl.getRaftOptions());
    }

    private void awaitMajority(ReplicationGroupId replicationGroupId) {
        IgniteImpl ignite = unwrapIgniteImpl(cluster.aliveNode());

        assertThat(
                ignite.placementDriver().awaitPrimaryReplica(replicationGroupId, ignite.clock().now(), 10, TimeUnit.SECONDS),
                willCompleteSuccessfully()
        );
    }

    private void flushMvPartitionStorage(int nodeIndex, String tableName, int partitionId) {
        TableViewInternal tableViewInternal = unwrapTableViewInternal(igniteImpl(nodeIndex).tables().table(tableName));

        MvPartitionStorage mvPartition = tableViewInternal.internalTable().storage().getMvPartition(partitionId);

        assertNotNull(
                mvPartition,
                String.format(
                        "Missing MvPartitionStorage: [nodeIndex=%s, tableName=%s, partitionId=%s]",
                        nodeIndex, tableName, partitionId
                )
        );

        assertThat(
                IgniteTestUtils.runAsync(() -> mvPartition.flush(true)).thenCompose(Function.identity()),
                willCompleteSuccessfully()
        );
    }

    private static String selectPeopleDml(String tableName) {
        return String.format(
                "select %s, %s, %s from %s",
                Person.ID_COLUMN_NAME, Person.NAME_COLUMN_NAME, Person.SALARY_COLUMN_NAME,
                tableName
        );
    }

    private static String insertPersonDml(String tableName, Person person) {
        return String.format(
                "insert into %s(%s, %s, %s) values(%s, '%s', %s)",
                tableName,
                Person.ID_COLUMN_NAME, Person.NAME_COLUMN_NAME, Person.SALARY_COLUMN_NAME,
                person.id, person.name, person.salary
        );
    }

    private static String createTablePersonDdl(String zoneName, String tableName) {
        return String.format(
                "create table if not exists %s (%s bigint primary key, %s varchar, %s bigint) zone %s",
                tableName,
                Person.ID_COLUMN_NAME, Person.NAME_COLUMN_NAME, Person.SALARY_COLUMN_NAME,
                zoneName
        );
    }

    private static String createZoneDdl(String zoneName, int replicas, int partitions) {
        return String.format(
                "create zone %s with replicas=%s, partitions=%s, storage_profiles='%s'",
                zoneName, replicas, partitions, DEFAULT_STORAGE_PROFILE
        );
    }

    private static Person[] generatePeople(int count) {
        assertThat(count, greaterThanOrEqualTo(0));

        return IntStream.range(0, count)
                .mapToObj(i -> new Person(i, "name-" + i, i + 1_000))
                .toArray(Person[]::new);
    }

    private static Person[] toPeopleFromSqlRows(List<List<Object>> sqlResult) {
        return sqlResult.stream()
                .map(ItTruncateRaftLogAndRestartNodesTest::toPersonFromSqlRow)
                .toArray(Person[]::new);
    }

    private static Person toPersonFromSqlRow(List<Object> sqlRow) {
        assertThat(sqlRow, hasSize(3));
        assertThat(sqlRow.get(0), instanceOf(Long.class));
        assertThat(sqlRow.get(1), instanceOf(String.class));
        assertThat(sqlRow.get(2), instanceOf(Long.class));

        return new Person((Long) sqlRow.get(0), (String) sqlRow.get(1), (Long) sqlRow.get(2));
    }

    private Person[] scanPeopleFromAllPartitions(int nodeIndex, String tableName) {
        IgniteImpl ignite = igniteImpl(nodeIndex);

        TableViewInternal tableViewInternal = unwrapTableViewInternal(ignite.tables().table(tableName));

        InternalTableImpl table = (InternalTableImpl) tableViewInternal.internalTable();

        InternalTransaction roTx = (InternalTransaction) ignite.transactions().begin(new TransactionOptions().readOnly(true));

        var scanFutures = new ArrayList<CompletableFuture<List<BinaryRow>>>();

        try {
            for (int partitionId = 0; partitionId < table.partitions(); partitionId++) {
                scanFutures.add(subscribeToList(scan(table, roTx, partitionId, ignite.node())));
            }

            assertThat(allOf(scanFutures), willCompleteSuccessfully());

            SchemaDescriptor schemaDescriptor = tableViewInternal.schemaView().lastKnownSchema();

            return scanFutures.stream()
                    .map(CompletableFuture::join)
                    .flatMap(Collection::stream)
                    .map(binaryRow -> toPersonFromBinaryRow(schemaDescriptor, binaryRow))
                    .toArray(Person[]::new);
        } finally {
            roTx.commit();
        }
    }

    private static Person[] half(Person... people) {
        return Arrays.copyOfRange(people, 0, people.length / 2);
    }

    private static Publisher<BinaryRow> scan(
            InternalTableImpl internalTableImpl,
            InternalTransaction roTx,
            int partitionId,
            InternalClusterNode recipientNode
    ) {
        assertTrue(roTx.isReadOnly(), roTx.toString());

        return internalTableImpl.scan(
                partitionId,
                recipientNode,
                OperationContext.create(TxContext.readOnly(roTx))
        );
    }

    private static Person toPersonFromBinaryRow(SchemaDescriptor schemaDescriptor, BinaryRow binaryRow) {
        var binaryTupleReader = new BinaryTupleReader(schemaDescriptor.length(), binaryRow.tupleSlice());

        Column idColumn = findColumnByName(schemaDescriptor, Person.ID_COLUMN_NAME);
        Column nameColumn = findColumnByName(schemaDescriptor, Person.NAME_COLUMN_NAME);
        Column salaryColumn = findColumnByName(schemaDescriptor, Person.SALARY_COLUMN_NAME);

        return new Person(
                binaryTupleReader.longValue(idColumn.positionInRow()),
                binaryTupleReader.stringValue(nameColumn.positionInRow()),
                binaryTupleReader.longValue(salaryColumn.positionInRow())
        );
    }

    private static Column findColumnByName(SchemaDescriptor schemaDescriptor, String columnName) {
        return schemaDescriptor.columns().stream()
                .filter(column -> columnName.equalsIgnoreCase(column.name()))
                .findFirst()
                .orElseThrow(() -> new AssertionError(
                        String.format("Can't find column by name: [columnName=%s, schema=%s]", columnName, schemaDescriptor)
                ));
    }

    private static void truncateRaftLogSuffixHalfOfChanges(LogStorage logStorage, long startRaftLogIndex) {
        long lastLogIndex = logStorage.getLastLogIndex();
        long lastIndexKept = lastLogIndex - (lastLogIndex - startRaftLogIndex) / 2;

        assertTrue(
                logStorage.truncateSuffix(lastIndexKept),
                String.format(
                        "Failed to truncate raft log suffix: [startRaftLogIndex=%s, lastLogIndex=%s, lastIndexKept=%s]",
                        startRaftLogIndex, lastLogIndex, lastIndexKept
                )
        );

        LOG.info(
                "Successfully truncated raft log suffix: [startRaftLogIndex={}, oldLastLogIndex={}, lastIndexKept={}, term={}]",
                startRaftLogIndex, lastLogIndex, lastIndexKept, logStorage.getEntry(lastIndexKept).getId().getTerm()
        );
    }

    private static void closeAllReversed(List<ManuallyCloseable> manuallyCloseables) throws Exception {
        Collections.reverse(manuallyCloseables);

        IgniteUtils.closeAllManually(manuallyCloseables);
    }

    private static class TestLogStorageFactory implements ManuallyCloseable {
        private final LogStorageFactory logStorageFactory;

        private final NodeOptions nodeOptions;

        private final RaftOptions raftOptions;

        private final AtomicBoolean closeGuard = new AtomicBoolean();

        private TestLogStorageFactory(
                LogStorageFactory logStorageFactory,
                NodeOptions nodeOptions,
                RaftOptions raftOptions
        ) {
            this.logStorageFactory = logStorageFactory;
            this.nodeOptions = nodeOptions;
            this.raftOptions = raftOptions;
        }

        /**
         * Creates and initializes {@link LogStorage}. Should be created only after the corresponding node is stopped to avoid errors.
         * Currently can only create one instance before it and {@link TestLogStorageFactory} are closed.
         */
        LogStorage createLogStorage() {
            assertThat(logStorageFactory.startAsync(new ComponentContext()), willCompleteSuccessfully());

            LogStorage storage = logStorageFactory.createLogStorage(nodeOptions.getLogUri(), raftOptions);

            var igniteJraftServiceFactory = new IgniteJraftServiceFactory(logStorageFactory);

            var logStorageOptions = new LogStorageOptions();
            logStorageOptions.setConfigurationManager(new ConfigurationManager());
            logStorageOptions.setLogEntryCodecFactory(igniteJraftServiceFactory.createLogEntryCodecFactory());

            storage.init(logStorageOptions);

            LOG.info(
                    "Successfully created LogStorage: [firstLogIndex={}, lastLogIndex={}, term={}]",
                    storage.getFirstLogIndex(), storage.getLastLogIndex(), storage.getEntry(storage.getLastLogIndex()).getId().getTerm()
            );

            return storage;
        }

        @Override
        public void close() {
            if (!closeGuard.compareAndSet(false, true)) {
                return;
            }

            assertThat(logStorageFactory.stopAsync(), willCompleteSuccessfully());
        }
    }

    private static class Person {
        static final String ID_COLUMN_NAME = "ID";

        static final String NAME_COLUMN_NAME = "NAME";

        static final String SALARY_COLUMN_NAME = "SALARY";

        @IgniteToStringInclude
        final long id;

        @IgniteToStringInclude
        final String name;

        @IgniteToStringInclude
        final long salary;

        private Person(long id, String name, long salary) {
            this.id = id;
            this.name = name;
            this.salary = salary;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Person p = (Person) o;

            return id == p.id && salary == p.salary && name.equals(p.name);
        }

        @Override
        public int hashCode() {
            int result = (int) (id ^ (id >>> 32));
            result = 31 * result + (int) (salary ^ (salary >>> 32));
            result = 31 * result + name.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return S.toString(Person.class, this);
        }
    }
}
