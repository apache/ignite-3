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

import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;
import java.util.stream.IntStream;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.tostring.IgniteToStringInclude;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.raft.jraft.core.NodeImpl;

/** Base class for raft log truncating related integration tests, containing useful methods, classes, and methods. */
class BaseTruncateRaftLogAbstractTest extends ClusterPerTestIntegrationTest {
    static final String ZONE_NAME = "TEST_ZONE";

    static final String TABLE_NAME = "TEST_TABLE";

    void createZoneAndTablePerson(String zoneName, String tableName, int replicas, int partitions) {
        executeSql(createZoneDdl(zoneName, replicas, partitions));
        executeSql(createTablePersonDdl(zoneName, tableName));
    }

    void insertPeople(String tableName, Person... people) {
        for (Person person : people) {
            executeSql(insertPersonDml(tableName, person));
        }
    }

    NodeImpl raftNodeImpl(int nodeIndex, ReplicationGroupId replicationGroupId) {
        return raftNodeImpl(igniteImpl(nodeIndex), replicationGroupId);
    }

    NodeImpl raftNodeImpl(IgniteImpl ignite, ReplicationGroupId replicationGroupId) {
        NodeImpl[] node = {null};

        ignite.raftManager().forEach((raftNodeId, raftGroupService) -> {
            if (replicationGroupId.equals(raftNodeId.groupId())) {
                assertNull(
                        node[0],
                        String.format("NodeImpl already found: [node=%s, replicationGroupId=%s]", ignite.name(), replicationGroupId)
                );

                node[0] = (NodeImpl) raftGroupService.getRaftNode();
            }
        });

        NodeImpl res = node[0];

        assertNotNull(res, String.format("Can't find NodeImpl: [node=%s, replicationGroupId=%s]", ignite.name(), replicationGroupId));

        return res;
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

    private static String insertPersonDml(String tableName, Person person) {
        return String.format(
                "insert into %s(%s, %s, %s) values(%s, '%s', %s)",
                tableName,
                Person.ID_COLUMN_NAME, Person.NAME_COLUMN_NAME, Person.SALARY_COLUMN_NAME,
                person.id, person.name, person.salary
        );
    }

    static Person[] generatePeople(int count) {
        assertThat(count, greaterThanOrEqualTo(0));

        return IntStream.range(0, count)
                .mapToObj(i -> new Person(i, "name-" + i, i + 1_000))
                .toArray(Person[]::new);
    }

    static Person[] toPeopleFromSqlRows(List<List<Object>> sqlResult) {
        return sqlResult.stream()
                .map(BaseTruncateRaftLogAbstractTest::toPersonFromSqlRow)
                .toArray(Person[]::new);
    }

    private static Person toPersonFromSqlRow(List<Object> sqlRow) {
        assertThat(sqlRow, hasSize(3));
        assertThat(sqlRow.get(0), instanceOf(Long.class));
        assertThat(sqlRow.get(1), instanceOf(String.class));
        assertThat(sqlRow.get(2), instanceOf(Long.class));

        return new Person((Long) sqlRow.get(0), (String) sqlRow.get(1), (Long) sqlRow.get(2));
    }

    static Person toPersonFromBinaryRow(SchemaDescriptor schemaDescriptor, BinaryRow binaryRow) {
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

    static class Person {
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
