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
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Publisher;
import java.util.stream.IntStream;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.table.distributed.storage.InternalTableImpl;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.tx.TransactionOptions;
import org.junit.jupiter.api.Test;

/**
 * Class for testing various scenarios with raft log truncation and node restarts that emulate the situation with fsync disabled for raft
 * groups associated with tables.
 */
public class ItTruncateRaftLogAndRestartNodesTest extends ClusterPerTestIntegrationTest {
    private static final String ZONE_NAME = "TEST_ZONE";

    private static final String TABLE_NAME = "TEST_TABLE";

    @Override
    protected int initialNodes() {
        return 0;
    }

    @Test
    void enterNodeWithIndexGreaterThanCurrentMajority() throws Exception {
        cluster.startAndInit(3, new int[]{0, 1, 2});

        createZoneAndTablePerson(ZONE_NAME, TABLE_NAME, 3, 1);

        cluster.transferLeadershipTo(2, cluster.solePartitionId());

        Person[] people = generatePeople(10);
        insertPeople(TABLE_NAME, people);

        // TODO: IGNITE-24785 остановить все узлы и обрезать логи

        verifyThatPeopleAvailableViaSql(TABLE_NAME, people);
        verifyThatPeoplePresentOnNodes(3, TABLE_NAME, people);
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

    private void verifyThatPeopleAvailableViaSql(String tableName, Person... expPeople) {
        assertThat(
                toPeopleFromSqlRows(executeSql(selectPeopleDml(tableName))),
                arrayContainingInAnyOrder(expPeople)
        );
    }

    private void verifyThatPeoplePresentOnNodes(int nodeCount, String tableName, Person... expPeople) {
        for (int i = 0; i < nodeCount; i++) {
            verifyThatPeoplePresentOnNode(i, tableName, expPeople);
        }
    }

    private void verifyThatPeoplePresentOnNode(int nodeIndex, String tableName, Person... expPeople) {
        IgniteImpl node = unwrapIgniteImpl(cluster.node(nodeIndex));

        assertThat(
                scanPeopleFromAllPartitions(node, tableName),
                arrayContainingInAnyOrder(expPeople)
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

    private static Person[] scanPeopleFromAllPartitions(IgniteImpl node, String tableName) {
        TableViewInternal tableViewInternal = unwrapTableViewInternal(node.tables().table(tableName));

        InternalTableImpl table = (InternalTableImpl) tableViewInternal.internalTable();

        InternalTransaction roTx = (InternalTransaction) node.transactions().begin(new TransactionOptions().readOnly(true));

        var scanFutures = new ArrayList<CompletableFuture<List<BinaryRow>>>();

        try {
            for (int partitionId = 0; partitionId < table.partitions(); partitionId++) {
                scanFutures.add(subscribeToList(scan(table, roTx, partitionId, node.node())));
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

    private static Publisher<BinaryRow> scan(
            InternalTableImpl internalTableImpl,
            InternalTransaction roTx,
            int partitionId,
            ClusterNode recipientNode
    ) {
        assertTrue(roTx.isReadOnly(), roTx.toString());

        return internalTableImpl.scan(
                partitionId,
                roTx.id(),
                roTx.readTimestamp(),
                recipientNode,
                roTx.coordinatorId()
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

    private static class Person {
        static final String ID_COLUMN_NAME = "ID";

        static final String NAME_COLUMN_NAME = "NAME";

        static final String SALARY_COLUMN_NAME = "SALARY";

        final long id;

        final String name;

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
            return S.toString(this);
        }
    }
}
