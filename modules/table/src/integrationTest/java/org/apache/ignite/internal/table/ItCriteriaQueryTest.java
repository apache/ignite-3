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

import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrows;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.internal.testframework.matchers.TupleMatcher.tupleValue;
import static org.apache.ignite.table.criteria.Criteria.columnValue;
import static org.apache.ignite.table.criteria.Criteria.equalTo;
import static org.apache.ignite.table.criteria.Criteria.not;
import static org.apache.ignite.table.criteria.CriteriaQueryOptions.builder;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.google.common.collect.Lists;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.AsyncCursor;
import org.apache.ignite.lang.Cursor;
import org.apache.ignite.lang.ErrorGroups.Criteria;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.criteria.CriteriaException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for the criteria query API.
 */
public class ItCriteriaQueryTest extends ClusterPerClassIntegrationTest {
    private static IgniteClient CLIENT;

    @Override
    protected int initialNodes() {
        return 1;
    }

    @BeforeAll
    protected void beforeAll() {
        CLIENT = IgniteClient.builder()
                .addresses("127.0.0.1:" + CLUSTER.aliveNode().clientAddress().port()).build();

        createTable(DEFAULT_TABLE_NAME, 1, 8);

        for (int i = 0; i < 3; i++) {
            insertPeople(DEFAULT_TABLE_NAME, new Person(i, "name" + i, 10.0d * i));
        }
    }

    @AfterAll
    void stopClient() throws Exception {
        IgniteUtils.closeAll(CLIENT);
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

        try (Cursor<Tuple> cur = view.queryCriteria(null, null)) {
            assertThat(Lists.newArrayList(cur), containsInAnyOrder(
                    allOf(tupleValue("id", is(0)), tupleValue("name", is("name0")), tupleValue("salary", is(0.0d))),
                    allOf(tupleValue("id", is(1)), tupleValue("name", is("name1")), tupleValue("salary", is(10.0d))),
                    allOf(tupleValue("id", is(2)), tupleValue("name", is("name2")), tupleValue("salary", is(20.0d)))
            ));
        }

        try (Cursor<Tuple> cur = view.queryCriteria(null, columnValue("id", equalTo(2)))) {
            assertThat(Lists.newArrayList(cur), containsInAnyOrder(
                    allOf(tupleValue("id", is(2)), tupleValue("name", is("name2")), tupleValue("salary", is(20.0d)))
            ));
        }

        try (Cursor<Tuple> cur = view.queryCriteria(null, not(columnValue("id", equalTo(2))))) {
            assertThat(Lists.newArrayList(cur), containsInAnyOrder(
                    allOf(tupleValue("id", is(0)), tupleValue("name", is("name0")), tupleValue("salary", is(0.0d))),
                    allOf(tupleValue("id", is(1)), tupleValue("name", is("name1")), tupleValue("salary", is(10.0d)))
            ));
        }
    }

    @ParameterizedTest(autoCloseArguments = false)
    @MethodSource("ignites")
    public void testRecordPojoView(Ignite ignite) {
        RecordView<Person> view = ignite.tables().table(DEFAULT_TABLE_NAME).recordView(Person.class);

        try (Cursor<Person> cur = view.queryCriteria(null, null)) {
            assertThat(Lists.newArrayList(cur), containsInAnyOrder(
                    new Person(0, "name0", 0.0d),
                    new Person(1, "name1", 10.0d),
                    new Person(2, "name2", 20.0d)
            ));
        }

        try (Cursor<Person> cur = view.queryCriteria(null, columnValue("id", equalTo(2)))) {
            assertThat(Lists.newArrayList(cur), containsInAnyOrder(
                    new Person(2, "name2", 20.0d)
            ));
        }

        try (Cursor<Person> cur = view.queryCriteria(null, not(columnValue("id", equalTo(2))))) {
            assertThat(Lists.newArrayList(cur), containsInAnyOrder(
                    new Person(0, "name0", 0.0d),
                    new Person(1, "name1", 10.0d)
            ));
        }
    }

    @ParameterizedTest(autoCloseArguments = false)
    @MethodSource("ignites")
    public void testKeyValueBinaryView(Ignite ignite) {
        KeyValueView<Tuple, Tuple> view = ignite.tables().table(DEFAULT_TABLE_NAME).keyValueView();

        try (Cursor<Entry<Tuple, Tuple>> cur = view.queryCriteria(null, null)) {
            assertThat(toMap(cur), allOf(
                    aMapWithSize(3),
                    hasEntry(
                            tupleValue("id", is(0)),
                            allOf(tupleValue("id", is(0)), tupleValue("name", is("name0")), tupleValue("salary", is(0.0d)))
                    ),
                    hasEntry(
                            tupleValue("id", is(1)),
                            allOf(tupleValue("id", is(1)), tupleValue("name", is("name1")), tupleValue("salary", is(10.0d)))
                    ),
                    hasEntry(
                            tupleValue("id", is(2)),
                            allOf(tupleValue("id", is(2)), tupleValue("name", is("name2")), tupleValue("salary", is(20.0d)))
                    )
            ));
        }

        try (Cursor<Entry<Tuple, Tuple>> cur = view.queryCriteria(null, columnValue("id", equalTo(2)))) {
            assertThat(toMap(cur), allOf(
                    aMapWithSize(1),
                    hasEntry(
                            tupleValue("id", is(2)),
                            allOf(tupleValue("id", is(2)), tupleValue("name", is("name2")), tupleValue("salary", is(20.0d)))
                    )
            ));
        }

        try (Cursor<Entry<Tuple, Tuple>> cur = view.queryCriteria(null, not(columnValue("id", equalTo(2))))) {
            assertThat(toMap(cur), allOf(
                    aMapWithSize(2),
                    hasEntry(
                            tupleValue("id", is(0)),
                            allOf(tupleValue("id", is(0)), tupleValue("name", is("name0")), tupleValue("salary", is(0.0d)))
                    ),
                    hasEntry(
                            tupleValue("id", is(1)),
                            allOf(tupleValue("id", is(1)), tupleValue("name", is("name1")), tupleValue("salary", is(10.0d)))
                    )
            ));
        }
    }

    @ParameterizedTest(autoCloseArguments = false)
    @MethodSource("ignites")
    public void testKeyValueView(Ignite ignite) {
        KeyValueView<PersonKey, Person> view = ignite.tables().table(DEFAULT_TABLE_NAME).keyValueView(PersonKey.class, Person.class);

        try (Cursor<Entry<PersonKey, Person>> cur = view.queryCriteria(null, null)) {
            assertThat(toMap(cur), allOf(
                    aMapWithSize(3),
                    hasEntry(new PersonKey(0), new Person(0, "name0", 0.0d)),
                    hasEntry(new PersonKey(1), new Person(1, "name1", 10.0d)),
                    hasEntry(new PersonKey(2), new Person(2, "name2", 20.0d))
            ));
        }

        try (Cursor<Entry<PersonKey, Person>> cur = view.queryCriteria(null, columnValue("id", equalTo(2)))) {
            assertThat(toMap(cur), allOf(
                    aMapWithSize(1),
                    hasEntry(new PersonKey(2), new Person(2, "name2", 20.0d))
            ));
        }

        try (Cursor<Entry<PersonKey, Person>> cur = view.queryCriteria(null, not(columnValue("id", equalTo(2))))) {
            assertThat(toMap(cur), allOf(
                    aMapWithSize(2),
                    hasEntry(new PersonKey(0), new Person(0, "name0", 0.0d)),
                    hasEntry(new PersonKey(1), new Person(1, "name1", 10.0d))
            ));
        }
    }

    @Test
    public void testOptions() {
        RecordView<Person> view = CLIENT.tables().table(DEFAULT_TABLE_NAME).recordView(Person.class);

        AsyncCursor<Person> ars = await(view.queryCriteriaAsync(null, null, builder().pageSize(2).build()));

        assertNotNull(ars);
        assertEquals(2, ars.currentPageSize());
        await(ars.closeAsync());
    }

    @Test
    void testNoMorePages() {
        RecordView<Person> view = CLIENT.tables().table(DEFAULT_TABLE_NAME).recordView(Person.class);

        AsyncCursor<Person> ars = await(view.queryCriteriaAsync(null, null, builder().pageSize(3).build()));

        assertNotNull(ars);
        assertThrows(CriteriaException.class, () -> await(ars.fetchNextPage()), Criteria.CURSOR_NO_MORE_PAGES_ERR,
                "There are no more pages");
    }

    @Test
    void testFetchCursorIsClosed() {
        RecordView<Person> view = CLIENT.tables().table(DEFAULT_TABLE_NAME).recordView(Person.class);

        AsyncCursor<Person> ars = await(view.queryCriteriaAsync(null, null, builder().pageSize(2).build()));

        assertNotNull(ars);
        await(ars.closeAsync());
        assertThrows(CriteriaException.class, () -> await(ars.fetchNextPage()), Criteria.CURSOR_CLOSED_ERR, "Cursor is closed");
    }

    @Test
    void testInvalidColumnName() {
        RecordView<Person> view = CLIENT.tables().table(DEFAULT_TABLE_NAME).recordView(Person.class);

        assertThrows(IllegalArgumentException.class, () -> await(view.queryCriteriaAsync(null, columnValue("id1", equalTo(2)))),
                "Unexpected column name: ID1");
    }

    private static <K, V> Map<K, V> toMap(Cursor<Entry<K, V>> cursor) {
        return Lists.newArrayList(cursor).stream().collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    }

    /**
     * Key class for reading from table.
     */
    public static class PersonKey {
        int id;

        private PersonKey() {
            //No-op.
        }

        PersonKey(int id) {
            this.id = id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PersonKey person = (PersonKey) o;
            return id == person.id;
        }

        @Override
        public int hashCode() {
            return Objects.hash(id);
        }
    }
}
