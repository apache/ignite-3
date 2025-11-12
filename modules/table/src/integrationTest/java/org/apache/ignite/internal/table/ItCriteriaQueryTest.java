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
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCode;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.internal.testframework.matchers.TupleMatcher.tupleValue;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;
import static org.apache.ignite.lang.util.IgniteNameUtils.quoteIfNeeded;
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
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Spliterator;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.lang.AsyncCursor;
import org.apache.ignite.lang.Cursor;
import org.apache.ignite.lang.CursorClosedException;
import org.apache.ignite.lang.ErrorGroups.Common;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.criteria.CriteriaException;
import org.apache.ignite.table.criteria.CriteriaQuerySource;
import org.apache.ignite.table.mapper.Mapper;
import org.apache.ignite.tx.Transaction;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
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
    /** Table name. */
    private static final String TABLE_NAME = "tbl";

    /** Table with quoted name. */
    private static final String QUOTED_TABLE_NAME = quoteIfNeeded("TaBleName");

    private static final String COLUMN_NAME = "colUmn";

    private static final String QUOTED_COLUMN_NAME = quoteIfNeeded(COLUMN_NAME);

    private static IgniteClient CLIENT;

    /** {@inheritDoc} */
    @Override
    protected int initialNodes() {
        return 1;
    }

    /** {@inheritDoc} */
    @BeforeAll
    void beforeAll() {
        CLIENT = IgniteClient.builder()
                .addresses("127.0.0.1:" + unwrapIgniteImpl(CLUSTER.aliveNode()).clientAddress().port()).build();

        sql(format("CREATE TABLE {} (id INT PRIMARY KEY, name VARCHAR, salary DOUBLE, hash VARBINARY)", TABLE_NAME));

        insertData(
                TABLE_NAME,
                List.of("ID", "name", "salary", "hash"),
                new Object[]{0, null, 0.0d, "hash0".getBytes()},
                new Object[]{1, "name1", 10.0d, "hash1".getBytes()},
                new Object[]{2, "name2", 20.0d, "hash2".getBytes()}
        );

        sql(format("CREATE TABLE {} (id INT PRIMARY KEY, {} VARCHAR)", QUOTED_TABLE_NAME, QUOTED_COLUMN_NAME));

        insertData(
                QUOTED_TABLE_NAME,
                List.of("id", QUOTED_COLUMN_NAME),
                new Object[]{0, "name0"},
                new Object[]{1, "name1"},
                new Object[]{2, "name2"}
        );
    }

    @AfterAll
    void stopClient() throws Exception {
        closeAll(CLIENT);
    }

    private static Stream<Arguments> testRecordViewQuery() {
        Table table = CLUSTER.aliveNode().tables().table(TABLE_NAME);
        Table clientTable = CLIENT.tables().table(TABLE_NAME);

        Function<TestObject, Tuple> objMapper = (obj) -> Tuple.create().set("id", obj.id).set("name", obj.name)
                .set("salary", obj.salary).set("hash", obj.hash);

        return Stream.of(
                Arguments.of(table.recordView(), identity()),
                Arguments.of(table.recordView(TestObject.class), objMapper),
                Arguments.of(clientTable.recordView(), identity()),
                Arguments.of(clientTable.recordView(TestObject.class), objMapper)
        );
    }

    @ParameterizedTest
    @MethodSource
    public <T> void testRecordViewQuery(CriteriaQuerySource<T> view, Function<T, Tuple> mapper) {
        Matcher<Tuple> person0 = allOf(tupleValue("id", is(0)), tupleValue("name", Matchers.nullValue()), tupleValue("salary", is(0.0d)),
                tupleValue("hash", is("hash0".getBytes())));
        Matcher<Tuple> person1 = allOf(tupleValue("id", is(1)), tupleValue("name", is("name1")), tupleValue("salary", is(10.0d)),
                tupleValue("hash", is("hash1".getBytes())));
        Matcher<Tuple> person2 = allOf(tupleValue("id", is(2)), tupleValue("name", is("name2")), tupleValue("salary", is(20.0d)),
                tupleValue("hash", is("hash2".getBytes())));

        try (Cursor<T> cur = view.query(null, null)) {
            assertThat(mapToTupleList(cur, mapper), containsInAnyOrder(person0, person1, person2));
        }

        try (Cursor<T> cur = view.query(null, columnValue("id", equalTo(2)))) {
            assertThat(mapToTupleList(cur, mapper), containsInAnyOrder(person2));
        }

        try (Cursor<T> cur = view.query(null, columnValue("hash", equalTo("hash2".getBytes())))) {
            assertThat(mapToTupleList(cur, mapper), containsInAnyOrder(person2));
        }

        try (Cursor<T> cur = view.query(null, columnValue("id", notEqualTo(2)))) {
            assertThat(mapToTupleList(cur, mapper), containsInAnyOrder(person0, person1));
        }

        try (Cursor<T> cur = view.query(null, columnValue("hash", notEqualTo("hash2".getBytes())))) {
            assertThat(mapToTupleList(cur, mapper), containsInAnyOrder(person0, person1));
        }

        try (Cursor<T> cur = view.query(null, columnValue("id", greaterThan(1)))) {
            assertThat(mapToTupleList(cur, mapper), containsInAnyOrder(person2));
        }

        try (Cursor<T> cur = view.query(null, columnValue("id", greaterThanOrEqualTo(1)))) {
            assertThat(mapToTupleList(cur, mapper), containsInAnyOrder(person1, person2));
        }

        try (Cursor<T> cur = view.query(null, columnValue("id", lessThan(1)))) {
            assertThat(mapToTupleList(cur, mapper), containsInAnyOrder(person0));
        }

        try (Cursor<T> cur = view.query(null, columnValue("id", lessThanOrEqualTo(1)))) {
            assertThat(mapToTupleList(cur, mapper), containsInAnyOrder(person0, person1));
        }

        try (Cursor<T> cur = view.query(null, columnValue("name", nullValue()))) {
            assertThat(mapToTupleList(cur, mapper), containsInAnyOrder(person0));
        }

        try (Cursor<T> cur = view.query(null, columnValue("name", notNullValue()))) {
            assertThat(mapToTupleList(cur, mapper), containsInAnyOrder(person1, person2));
        }

        try (Cursor<T> cur = view.query(null, columnValue("id", in(1, 2)))) {
            assertThat(mapToTupleList(cur, mapper), containsInAnyOrder(person1, person2));
        }

        try (Cursor<T> cur = view.query(null, columnValue("id", notIn(1, 2)))) {
            assertThat(mapToTupleList(cur, mapper), containsInAnyOrder(person0));
        }

        try (Cursor<T> cur = view.query(null, columnValue("hash", in("hash1".getBytes(), "hash2".getBytes())))) {
            assertThat(mapToTupleList(cur, mapper), containsInAnyOrder(person1, person2));
        }

        try (Cursor<T> cur = view.query(null, columnValue("hash", in((byte[]) null)))) {
            assertThat(mapToTupleList(cur, mapper), empty());
        }

        try (Cursor<T> cur = view.query(null, columnValue("hash", notIn("hash1".getBytes(), "hash2".getBytes())))) {
            assertThat(mapToTupleList(cur, mapper), containsInAnyOrder(person0));
        }

        try (Cursor<T> cur = view.query(null, columnValue("hash", notIn((byte[]) null)))) {
            assertThat(mapToTupleList(cur, mapper), containsInAnyOrder(person0, person1, person2));
        }
    }

    private static Stream<Arguments> testKeyValueView() {
        Table table = CLUSTER.aliveNode().tables().table(TABLE_NAME);
        Table clientTable = CLIENT.tables().table(TABLE_NAME);

        Function<Entry<TestObjectKey, TestObject>, Entry<Tuple, Tuple>> kvMapper = (entry) -> {
            TestObjectKey key = entry.getKey();
            TestObject val = entry.getValue();

            return new IgniteBiTuple<>(Tuple.create().set("id", key.id), Tuple.create().set("name", val.name).set("salary", val.salary)
                    .set("hash", val.hash));
        };

        return Stream.of(
                Arguments.of(table.keyValueView(), identity()),
                Arguments.of(table.keyValueView(TestObjectKey.class, TestObject.class), kvMapper),
                Arguments.of(clientTable.keyValueView(), identity()),
                Arguments.of(clientTable.keyValueView(TestObjectKey.class, TestObject.class), kvMapper)
        );
    }

    @ParameterizedTest
    @MethodSource
    public <T> void testKeyValueView(CriteriaQuerySource<T> view, Function<T, Entry<Tuple, Tuple>> mapper) {
        Matcher<Tuple> personKey0 = tupleValue("id", is(0));
        Matcher<Tuple> person0 = allOf(tupleValue("name", Matchers.nullValue()), tupleValue("salary", is(0.0d)),
                tupleValue("hash", is("hash0".getBytes())));

        Matcher<Tuple> personKey1 = tupleValue("id", is(1));
        Matcher<Tuple> person1 = allOf(tupleValue("name", is("name1")), tupleValue("salary", is(10.0d)),
                tupleValue("hash", is("hash1".getBytes())));

        Matcher<Tuple> personKey2 = tupleValue("id", is(2));
        Matcher<Tuple> person2 = allOf(tupleValue("name", is("name2")), tupleValue("salary", is(20.0d)),
                tupleValue("hash", is("hash2".getBytes())));

        try (Cursor<T> cur = view.query(null, null)) {
            assertThat(mapToTupleMap(cur, mapper), allOf(
                    aMapWithSize(3),
                    hasEntry(personKey0, person0),
                    hasEntry(personKey1, person1),
                    hasEntry(personKey2, person2)
            ));
        }

        try (Cursor<T> cur = view.query(null, columnValue("id", equalTo(2)))) {
            assertThat(mapToTupleMap(cur, mapper), allOf(
                    aMapWithSize(1),
                    hasEntry(personKey2, person2)
            ));
        }

        try (Cursor<T> cur = view.query(null, columnValue("hash", equalTo("hash2".getBytes())))) {
            assertThat(mapToTupleMap(cur, mapper), allOf(
                    aMapWithSize(1),
                    hasEntry(personKey2, person2)
            ));
        }

        try (Cursor<T> cur = view.query(null, columnValue("id", notEqualTo(2)))) {
            assertThat(mapToTupleMap(cur, mapper), allOf(
                    aMapWithSize(2),
                    hasEntry(personKey0, person0),
                    hasEntry(personKey1, person1)
            ));
        }

        try (Cursor<T> cur = view.query(null, columnValue("hash", notEqualTo("hash2".getBytes())))) {
            assertThat(mapToTupleMap(cur, mapper), allOf(
                    aMapWithSize(2),
                    hasEntry(personKey0, person0),
                    hasEntry(personKey1, person1)
            ));
        }

        try (Cursor<T> cur = view.query(null, columnValue("id", greaterThan(1)))) {
            assertThat(mapToTupleMap(cur, mapper), allOf(
                    aMapWithSize(1),
                    hasEntry(personKey2, person2)
            ));
        }

        try (Cursor<T> cur = view.query(null, columnValue("id", greaterThanOrEqualTo(1)))) {
            assertThat(mapToTupleMap(cur, mapper), allOf(
                    aMapWithSize(2),
                    hasEntry(personKey1, person1),
                    hasEntry(personKey2, person2)
            ));
        }

        try (Cursor<T> cur = view.query(null, columnValue("id", lessThan(1)))) {
            assertThat(mapToTupleMap(cur, mapper), allOf(
                    aMapWithSize(1),
                    hasEntry(personKey0, person0)
            ));
        }

        try (Cursor<T> cur = view.query(null, columnValue("id", lessThanOrEqualTo(1)))) {
            assertThat(mapToTupleMap(cur, mapper), allOf(
                    aMapWithSize(2),
                    hasEntry(personKey0, person0),
                    hasEntry(personKey1, person1)
            ));
        }

        try (Cursor<T> cur = view.query(null, columnValue("name", nullValue()))) {
            assertThat(mapToTupleMap(cur, mapper), allOf(
                    aMapWithSize(1),
                    hasEntry(personKey0, person0)
            ));
        }

        try (Cursor<T> cur = view.query(null, columnValue("name", notNullValue()))) {
            assertThat(mapToTupleMap(cur, mapper), allOf(
                    aMapWithSize(2),
                    hasEntry(personKey1, person1),
                    hasEntry(personKey2, person2)
            ));
        }

        try (Cursor<T> cur = view.query(null, columnValue("id", in(1, 2)))) {
            assertThat(mapToTupleMap(cur, mapper), allOf(
                    aMapWithSize(2),
                    hasEntry(personKey1, person1),
                    hasEntry(personKey2, person2)
            ));
        }

        try (Cursor<T> cur = view.query(null, columnValue("id", notIn(1, 2)))) {
            assertThat(mapToTupleMap(cur, mapper), allOf(
                    aMapWithSize(1),
                    hasEntry(personKey0, person0)
            ));
        }

        try (Cursor<T> cur = view.query(null, columnValue("hash", in("hash1".getBytes(), "hash2".getBytes())))) {
            assertThat(mapToTupleMap(cur, mapper), allOf(
                    aMapWithSize(2),
                    hasEntry(personKey1, person1),
                    hasEntry(personKey2, person2)
            ));
        }

        try (Cursor<T> cur = view.query(null, columnValue("hash", in((byte[]) null)))) {
            assertThat(mapToTupleMap(cur, mapper), anEmptyMap());
        }

        try (Cursor<T> cur = view.query(null, columnValue("hash", notIn("hash1".getBytes(), "hash2".getBytes())))) {
            assertThat(mapToTupleMap(cur, mapper), allOf(
                    aMapWithSize(1),
                    hasEntry(personKey0, person0)
            ));
        }

        try (Cursor<T> cur = view.query(null, columnValue("hash", notIn((byte[]) null)))) {
            assertThat(mapToTupleMap(cur, mapper), allOf(
                    aMapWithSize(3),
                    hasEntry(personKey0, person0),
                    hasEntry(personKey1, person1),
                    hasEntry(personKey2, person2)
            ));
        }
    }

    private static Stream<Arguments> clientViews() {
        Table clientTable = CLIENT.tables().table(TABLE_NAME);

        return Stream.of(
                Arguments.of(clientTable.recordView()),
                Arguments.of(clientTable.recordView(TestObject.class)),
                Arguments.of(clientTable.keyValueView()),
                Arguments.of(clientTable.keyValueView(TestObjectKey.class, TestObject.class))
        );
    }

    @ParameterizedTest
    @MethodSource("clientViews")
    public <T> void testPageOption(CriteriaQuerySource<T> view) {
        AsyncCursor<T> ars = await(view.queryAsync(null, null, null, builder().pageSize(2).build()));

        assertNotNull(ars);
        assertEquals(2, ars.currentPageSize());

        AsyncCursor<T> ars1 = await(ars.fetchNextPage());

        assertNotNull(ars1);
        assertEquals(1, ars1.currentPageSize());

        assertEquals(ars, ars1);

        await(ars.closeAsync());
    }

    private static Stream<Arguments> allViews() {
        Table table = CLUSTER.aliveNode().tables().table(TABLE_NAME);
        Table clientTable = CLIENT.tables().table(TABLE_NAME);

        return Stream.of(
                Arguments.of(table.keyValueView()),
                Arguments.of(table.keyValueView(TestObjectKey.class, TestObject.class)),
                Arguments.of(clientTable.keyValueView()),
                Arguments.of(clientTable.keyValueView(TestObjectKey.class, TestObject.class))
        );
    }

    @ParameterizedTest
    @MethodSource("allViews")
    void testFetchCursorIsClosed(CriteriaQuerySource<TestObject> view) {
        AsyncCursor<TestObject> ars1 = await(view.queryAsync(null, null, null, builder().pageSize(2).build()));

        assertNotNull(ars1);
        await(ars1.closeAsync());
        assertThrowsWithCode(CursorClosedException.class, Common.CURSOR_ALREADY_CLOSED_ERR,
                () -> await(ars1.fetchNextPage()), "Cursor is closed");

        AsyncCursor<TestObject> ars2 = await(view.queryAsync(null, null, null, builder().pageSize(3).build()));

        assertNotNull(ars2);
        assertThrowsWithCode(CursorClosedException.class, Common.CURSOR_ALREADY_CLOSED_ERR,
                () -> await(ars2.fetchNextPage()), "Cursor is closed");
    }

    @ParameterizedTest
    @MethodSource("allViews")
    <T> void testInvalidColumnName(CriteriaQuerySource<T> view) {
        assertThrowsWithCode(CriteriaException.class, INTERNAL_ERR,
                () -> await(view.queryAsync(null, columnValue("id1", equalTo(2)))), "Unexpected column name: ID1");
    }

    private static Stream<Arguments> testRecordViewWithQuotes() {
        Table table = CLUSTER.aliveNode().tables().table(QUOTED_TABLE_NAME);
        Table clientTable = CLIENT.tables().table(QUOTED_TABLE_NAME);

        Mapper<QuotedObject> recMapper = Mapper.builder(QuotedObject.class)
                .map(COLUMN_NAME, QUOTED_COLUMN_NAME)
                .automap()
                .build();

        Function<QuotedObject, Tuple> objMapper = (obj) -> Tuple.create(Map.of("id", obj.id, QUOTED_COLUMN_NAME, obj.colUmn));

        return Stream.of(
                Arguments.of(table.recordView(), identity()),
                Arguments.of(table.recordView(recMapper), objMapper),
                Arguments.of(clientTable.recordView(), identity()),
                Arguments.of(clientTable.recordView(recMapper), objMapper)
        );
    }

    @ParameterizedTest
    @MethodSource
    public <T> void testRecordViewWithQuotes(CriteriaQuerySource<T> view, Function<T, Tuple> mapper) {
        try (Cursor<T> cur = view.query(null, columnValue(QUOTED_COLUMN_NAME, equalTo("name1")))) {
            assertThat(mapToTupleList(cur, mapper), containsInAnyOrder(
                    allOf(tupleValue("id", is(1)), tupleValue(QUOTED_COLUMN_NAME, is("name1")))
            ));
        }
    }

    private static Stream<Arguments> testKeyViewWithQuotes() {
        Table table = CLUSTER.aliveNode().tables().table(QUOTED_TABLE_NAME);
        Table clientTable = CLIENT.tables().table(QUOTED_TABLE_NAME);

        Mapper<QuotedObjectKey> keyMapper = Mapper.of(QuotedObjectKey.class);
        Mapper<QuotedObject> valMapper = Mapper.builder(QuotedObject.class)
                .map(COLUMN_NAME, QUOTED_COLUMN_NAME)
                .build();

        Function<Entry<QuotedObjectKey, QuotedObject>, Entry<Tuple, Tuple>> kvMapper = (entry) ->
                new IgniteBiTuple<>(Tuple.create(Map.of("id", entry.getKey().id)), Tuple.create(Map.of(QUOTED_COLUMN_NAME,
                        entry.getValue().colUmn)));

        return Stream.of(
                Arguments.of(table.keyValueView(), identity()),
                Arguments.of(table.keyValueView(keyMapper, valMapper), kvMapper),
                Arguments.of(clientTable.keyValueView(), identity()),
                Arguments.of(clientTable.keyValueView(keyMapper, valMapper), kvMapper)
        );
    }

    @ParameterizedTest
    @MethodSource
    public <T> void testKeyViewWithQuotes(CriteriaQuerySource<T> view, Function<T, Entry<Tuple, Tuple>> mapper) {
        try (Cursor<T> cur = view.query(null, columnValue(QUOTED_COLUMN_NAME, equalTo("name1")))) {
            assertThat(mapToTupleMap(cur, mapper), allOf(
                    aMapWithSize(1),
                    hasEntry(tupleValue("id", is(1)), tupleValue(QUOTED_COLUMN_NAME, is("name1")))
            ));
        }
    }

    @Test
    public void testRecordViewAllColumnTypes() {
        String tableName = "all_column_types";

        sql(format("CREATE TABLE {} (str VARCHAR PRIMARY KEY, byteCol TINYINT, shortCol SMALLINT, intCol INT, longCol BIGINT, "
                        + "floatCol REAL, doubleCol DOUBLE, decimalCol DECIMAL, boolCol BOOLEAN, bytesCol VARBINARY, "
                        + "uuidCol UUID, dateCol DATE, timeCol TIME, datetimeCol TIMESTAMP, instantCol TIMESTAMP WITH LOCAL TIME ZONE)",
                tableName));

        UUID uuid = UUID.randomUUID();
        LocalDate localDate = LocalDate.of(2024, 1, 1);
        LocalTime localTime = LocalTime.of(12, 15, 10);
        LocalDateTime localDateTime = LocalDateTime.of(2024, 1, 1, 12, 0);
        byte[] bytes = {1, 2, 3};

        insertData(
                tableName,
                List.of("str", "byteCol", "shortCol", "intCol", "longCol", "floatCol", "doubleCol", "decimalCol",
                        "boolCol", "bytesCol", "uuidCol", "dateCol", "timeCol", "datetimeCol", "instantCol"),
                new Object[]{"test", (byte) 1, (short) 2, 3, 4L, 5.0f, 6.0d, new BigDecimal("7.0"), true,
                        bytes, uuid, localDate,
                        localTime, localDateTime,
                        Instant.parse("2024-01-01T12:00:00Z")}
        );

        Table table = CLIENT.tables().table(tableName);

        try (Cursor<TestAllColumnTypes> cur = table.recordView(TestAllColumnTypes.class).query(null, null)) {
            List<TestAllColumnTypes> results = StreamSupport.stream(spliteratorUnknownSize(cur, Spliterator.ORDERED), false)
                    .collect(toList());

            assertThat(results, Matchers.hasSize(1));

            TestAllColumnTypes row = results.get(0);

            assertEquals("test", row.str);
            assertEquals(Byte.valueOf((byte) 1), row.byteCol);
            assertEquals(Short.valueOf((short) 2), row.shortCol);
            assertEquals(Integer.valueOf(3), row.intCol);
            assertEquals(Long.valueOf(4L), row.longCol);
            assertEquals(Float.valueOf(5.0f), row.floatCol);
            assertEquals(Double.valueOf(6.0d), row.doubleCol);
            assertEquals(new BigDecimal("7.0"), row.decimalCol);
            assertEquals(Boolean.TRUE, row.boolCol);
            assertArrayEquals(bytes, row.bytesCol);
            assertEquals(uuid, row.uuidCol);
            assertEquals(localDate, row.dateCol);
            assertEquals(localTime, row.timeCol);
            assertEquals(localDateTime, row.datetimeCol);
            assertEquals(Instant.parse("2024-01-01T12:00:00Z"), row.instantCol);
        }
    }

    private static Stream<Arguments> testSessionClosing() {
        Table table = CLUSTER.aliveNode().tables().table(TABLE_NAME);

        Transaction tx = CLUSTER.aliveNode().transactions().begin();
        tx.rollback();

        return Stream.of(
                Arguments.of(table.recordView(), tx),
                Arguments.of(table.recordView(TestObject.class), tx),
                Arguments.of(table.keyValueView(), tx),
                Arguments.of(table.keyValueView(TestObjectKey.class, TestObject.class), tx)
        );
    }

    private static <T> List<Tuple> mapToTupleList(Cursor<T> cur, Function<T, Tuple> mapper) {
        return StreamSupport.stream(spliteratorUnknownSize(cur, Spliterator.ORDERED), false)
                .map(mapper)
                .collect(toList());
    }

    private static <T, K, V> Map<K, V> mapToTupleMap(Cursor<T> cur, Function<T, Entry<K, V>> mapper) {
        return StreamSupport.stream(spliteratorUnknownSize(cur, Spliterator.ORDERED), false)
                .map(mapper)
                .collect(toMap(Entry::getKey, Entry::getValue));
    }

    static class QuotedObjectKey {
        int id;
    }

    static class QuotedObject {
        int id;
        String colUmn;
    }

    static class TestObjectKey {
        int id;
    }

    static class TestObject {
        int id;

        String name;

        double salary;

        byte[] hash;
    }

    static class TestAllColumnTypes {
        String str;
        Byte byteCol;
        Short shortCol;
        Integer intCol;
        Long longCol;
        Float floatCol;
        Double doubleCol;
        BigDecimal decimalCol;
        Boolean boolCol;
        byte[] bytesCol;
        UUID uuidCol;
        LocalDate dateCol;
        LocalTime timeCol;
        LocalDateTime datetimeCol;
        Instant instantCol;
    }
}
