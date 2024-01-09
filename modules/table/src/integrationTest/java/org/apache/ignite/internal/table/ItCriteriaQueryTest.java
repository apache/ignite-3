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

package org.apache.ignite.internal.table;

import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.table.criteria.Criteria.columnValue;
import static org.apache.ignite.table.criteria.Criteria.equalTo;
import static org.apache.ignite.table.criteria.Criteria.greaterThan;
import static org.apache.ignite.table.criteria.Criteria.greaterThanOrEqualTo;
import static org.apache.ignite.table.criteria.Criteria.in;
import static org.apache.ignite.table.criteria.Criteria.lessThan;
import static org.apache.ignite.table.criteria.Criteria.lessThanOrEqualTo;
import static org.apache.ignite.table.criteria.Criteria.notEqualTo;
import static org.apache.ignite.table.criteria.Criteria.notIn;
import static org.apache.ignite.table.criteria.Criteria.notNullValue;
import static org.apache.ignite.table.criteria.Criteria.nullValue;
import static org.apache.ignite.table.criteria.CriteriaQueryOptions.builder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Spliterator;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.ignite.Ignite;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.AsyncCursor;
import org.apache.ignite.lang.Cursor;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.criteria.CriteriaQuerySource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for the criteria query API.
 */
public class ItCriteriaQueryTest extends ClusterPerClassIntegrationTest {
    private static IgniteClient CLIENT;

    private static final PersonEx PERSON_0 = new PersonEx(0, null, 0.0d, "hash0".getBytes());
    private static final PersonEx PERSON_1 = new PersonEx(1, "name1", 10.0d, "hash1".getBytes());
    private static final PersonEx PERSON_2 = new PersonEx(2, "name2", 20.0d, "hash2".getBytes());


    /** {@inheritDoc} */
    @Override
    protected int initialNodes() {
        return 1;
    }

    /** {@inheritDoc} */
    @BeforeAll
    @Override
    protected void beforeAll(TestInfo testInfo) {
        super.beforeAll(testInfo);

        CLIENT = IgniteClient.builder()
                .addresses("127.0.0.1:" + CLUSTER.aliveNode().clientAddress().port()).build();

        sql(format(
                "CREATE TABLE {} (id INT PRIMARY KEY, name VARCHAR, salary DOUBLE, hash VARBINARY)",
                DEFAULT_TABLE_NAME
        ));

        insertData(
                DEFAULT_TABLE_NAME,
                List.of("ID", "name", "salary", "hash"),
                Stream.of(PERSON_0, PERSON_1, PERSON_2)
                        .map(person -> new Object[]{person.id, person.name, person.salary, person.hash}).toArray(Object[][]::new)
        );
    }

    @AfterAll
    void stopClient() throws Exception {
        IgniteUtils.closeAll(CLIENT);
    }

    private static <T> void checkQuery(CriteriaQuerySource<T> view, Function<Cursor<T>, Stream<PersonEx>> mapper) {
        IgniteTestUtils.assertThrows(
                IgniteException.class,
                () -> view.query(null, columnValue("id", equalTo("2"))),
                "Dynamic parameter requires adding explicit type cast"
        );

        try (Cursor<T> cur = view.query(null, null)) {
            assertThat(mapper.apply(cur).collect(toList()), containsInAnyOrder(PERSON_0, PERSON_1, PERSON_2));
        }

        try (Cursor<T> cur = view.query(null, columnValue("id", equalTo(2)))) {
            assertThat(mapper.apply(cur).collect(toList()), containsInAnyOrder(PERSON_2));
        }

        try (Cursor<T> cur = view.query(null, columnValue("hash", equalTo(PERSON_2.hash)))) {
            assertThat(mapper.apply(cur).collect(toList()), containsInAnyOrder(PERSON_2));
        }

        try (Cursor<T> cur = view.query(null, columnValue("id", notEqualTo(2)))) {
            assertThat(mapper.apply(cur).collect(toList()), containsInAnyOrder(PERSON_0, PERSON_1));
        }

        try (Cursor<T> cur = view.query(null, columnValue("hash", notEqualTo(PERSON_2.hash)))) {
            assertThat(mapper.apply(cur).collect(toList()), containsInAnyOrder(PERSON_0, PERSON_1));
        }

        try (Cursor<T> cur = view.query(null, columnValue("id", greaterThan(1)))) {
            assertThat(mapper.apply(cur).collect(toList()), containsInAnyOrder(PERSON_2));
        }

        try (Cursor<T> cur = view.query(null, columnValue("id", greaterThanOrEqualTo(1)))) {
            assertThat(mapper.apply(cur).collect(toList()), containsInAnyOrder(PERSON_1, PERSON_2));
        }

        try (Cursor<T> cur = view.query(null, columnValue("id", lessThan(1)))) {
            assertThat(mapper.apply(cur).collect(toList()), containsInAnyOrder(PERSON_0));
        }

        try (Cursor<T> cur = view.query(null, columnValue("id", lessThanOrEqualTo(1)))) {
            assertThat(mapper.apply(cur).collect(toList()), containsInAnyOrder(PERSON_0, PERSON_1));
        }

        try (Cursor<T> cur = view.query(null, columnValue("name", nullValue()))) {
            assertThat(mapper.apply(cur).collect(toList()), containsInAnyOrder(PERSON_0));
        }

        try (Cursor<T> cur = view.query(null, columnValue("name", notNullValue()))) {
            assertThat(mapper.apply(cur).collect(toList()), containsInAnyOrder(PERSON_1, PERSON_2));
        }

        try (Cursor<T> cur = view.query(null, columnValue("id", in(1, 2)))) {
            assertThat(mapper.apply(cur).collect(toList()), containsInAnyOrder(PERSON_1, PERSON_2));
        }

        try (Cursor<T> cur = view.query(null, columnValue("id", notIn(1, 2)))) {
            assertThat(mapper.apply(cur).collect(toList()), containsInAnyOrder(PERSON_0));
        }

        try (Cursor<T> cur = view.query(null, columnValue("hash", in(PERSON_1.hash, PERSON_2.hash)))) {
            assertThat(mapper.apply(cur).collect(toList()), containsInAnyOrder(PERSON_1, PERSON_2));
        }

        try (Cursor<T> cur = view.query(null, columnValue("hash", notIn(PERSON_1.hash, PERSON_2.hash)))) {
            assertThat(mapper.apply(cur).collect(toList()), containsInAnyOrder(PERSON_0));
        }
    }

    private static Stream<Arguments> ignites() {
        return Stream.of(
                Arguments.of(CLIENT),
                Arguments.of(CLUSTER.aliveNode())
        );
    }

    @ParameterizedTest(autoCloseArguments = false)
    @MethodSource("ignites")
    public void testRecordBinaryView(Ignite ignite) {
        RecordView<Tuple> view = ignite.tables().table(DEFAULT_TABLE_NAME).recordView();

        checkQuery(
                view,
                (cur) -> StreamSupport.stream(spliteratorUnknownSize(cur, Spliterator.ORDERED), false)
                        .map((t) -> new PersonEx(t.intValue("id"), t.stringValue("name"), t.doubleValue("salary"), t.value("hash")))
        );
    }

    private static Stream<Arguments> testRecordPojoView() {
        return Stream.of(
                // TODO https://issues.apache.org/jira/browse/IGNITE-20977
                //Arguments.of(CLUSTER.aliveNode()),
                Arguments.of(CLIENT)
        );
    }

    @ParameterizedTest(autoCloseArguments = false)
    @MethodSource
    public void testRecordPojoView(Ignite ignite) {
        RecordView<PersonEx> view = ignite.tables().table(DEFAULT_TABLE_NAME).recordView(PersonEx.class);

        checkQuery(view, (cur) -> StreamSupport.stream(spliteratorUnknownSize(cur, Spliterator.ORDERED), false));
    }

    @Test
    public void testOptions() {
        RecordView<PersonEx> view = CLIENT.tables().table(DEFAULT_TABLE_NAME).recordView(PersonEx.class);

        AsyncCursor<PersonEx> ars = await(view.queryAsync(null, null, builder().pageSize(2).build()));

        assertNotNull(ars);
        assertEquals(2, ars.currentPageSize());
        await(ars.closeAsync());
    }

    static class PersonEx {
        int id;

        String name;

        double salary;

        byte[] hash;

        PersonEx() {
            // No-op.
        }

        PersonEx(int id, String name, double salary, byte[] hash) {
            this.id = id;
            this.name = name;
            this.salary = salary;
            this.hash = hash;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PersonEx product = (PersonEx) o;
            return id == product.id && Double.compare(salary, product.salary) == 0 && Objects.equals(name, product.name)
                    && Arrays.equals(hash, product.hash);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, name, salary, Arrays.hashCode(hash));
        }

        @Override
        public String toString() {
            return S.toString(PersonEx.class, this);
        }
    }
}
