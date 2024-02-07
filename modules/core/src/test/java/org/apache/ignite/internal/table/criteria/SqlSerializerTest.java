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

package org.apache.ignite.internal.table.criteria;

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.util.ArrayUtils.OBJECT_EMPTY_ARRAY;
import static org.apache.ignite.lang.util.IgniteNameUtils.quote;
import static org.apache.ignite.table.criteria.Criteria.and;
import static org.apache.ignite.table.criteria.Criteria.columnValue;
import static org.apache.ignite.table.criteria.Criteria.equalTo;
import static org.apache.ignite.table.criteria.Criteria.greaterThan;
import static org.apache.ignite.table.criteria.Criteria.greaterThanOrEqualTo;
import static org.apache.ignite.table.criteria.Criteria.in;
import static org.apache.ignite.table.criteria.Criteria.lessThan;
import static org.apache.ignite.table.criteria.Criteria.lessThanOrEqualTo;
import static org.apache.ignite.table.criteria.Criteria.not;
import static org.apache.ignite.table.criteria.Criteria.notEqualTo;
import static org.apache.ignite.table.criteria.Criteria.notIn;
import static org.apache.ignite.table.criteria.Criteria.notNullValue;
import static org.apache.ignite.table.criteria.Criteria.nullValue;
import static org.apache.ignite.table.criteria.Criteria.or;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.of;

import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.apache.ignite.table.criteria.Criteria;
import org.apache.ignite.table.criteria.Expression;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * SQL generation test.
 */
class SqlSerializerTest {
    static Stream<Arguments> testCondition() {
        byte[] arr1 = "arr1".getBytes();
        byte[] arr2 = "arr2".getBytes();

        return Stream.of(
                of(columnValue("a", equalTo("a1")), "A = ?", new Object[]{"a1"}),
                of(columnValue("a", equalTo(arr1)), "A = ?", new Object[]{arr1}),
                of(columnValue("a", equalTo((Comparable<Object>) null)), "A IS NULL", OBJECT_EMPTY_ARRAY),
                of(columnValue("a", equalTo((byte[]) null)), "A IS NULL", OBJECT_EMPTY_ARRAY),
                of(columnValue("a", notEqualTo("a1")), "A <> ?", new Object[]{"a1"}),
                of(columnValue("a", notEqualTo(arr1)), "A <> ?", new Object[]{arr1}),
                of(columnValue("a", notEqualTo((Comparable<Object>) null)), "A IS NOT NULL", OBJECT_EMPTY_ARRAY),
                of(columnValue("a", notEqualTo((byte[]) null)), "A IS NOT NULL", OBJECT_EMPTY_ARRAY),
                of(columnValue("a", greaterThan("a1")), "A > ?", new Object[]{"a1"}),
                of(columnValue("a", greaterThanOrEqualTo("a1")), "A >= ?", new Object[]{"a1"}),
                of(columnValue("a", lessThan("a1")), "A < ?", new Object[]{"a1"}),
                of(columnValue("a", lessThanOrEqualTo("a1")), "A <= ?", new Object[]{"a1"}),
                of(columnValue("a", nullValue()), "A IS NULL", OBJECT_EMPTY_ARRAY),
                of(columnValue("a", notNullValue()), "A IS NOT NULL", OBJECT_EMPTY_ARRAY),
                of(columnValue("a", in("a1", "a2")), "A IN (?, ?)", new Object[]{"a1", "a2"}),
                of(columnValue("a", in("a1")), "A = ?", new Object[]{"a1"}),
                of(columnValue("a", in(arr1, arr2)), "A IN (?, ?)", new Object[]{arr1, arr2}),
                of(columnValue("a", in(arr1)), "A = ?", new Object[]{arr1}),
                of(columnValue("a", in((Comparable<Object>) null)), "A IS NULL", OBJECT_EMPTY_ARRAY),
                of(columnValue("a", in((byte[]) null)), "A IS NULL", OBJECT_EMPTY_ARRAY),
                of(columnValue("a", notIn("a1", "a2")), "A NOT IN (?, ?)", new Object[]{"a1", "a2"}),
                of(columnValue("a", notIn("a1")), "A <> ?", new Object[]{"a1"}),
                of(columnValue("a", notIn("a1")), "A <> ?", new Object[]{"a1"}),
                of(columnValue("a", notIn((Comparable<Object>) null)), "A IS NOT NULL", OBJECT_EMPTY_ARRAY),
                of(columnValue("a", notIn((byte[]) null)), "A IS NOT NULL", OBJECT_EMPTY_ARRAY)
        );
    }

    @ParameterizedTest
    @MethodSource
    void testCondition(Criteria criteria, String wherePart, Object[] arguments) {
        SqlSerializer ser = new SqlSerializer.Builder()
                .tableName("test")
                .columns(Set.of("A"))
                .where(criteria)
                .build();

        assertThat(ser.toString(), endsWith(wherePart));
        assertArrayEquals(arguments, ser.getArguments());
    }

    @Test
    void testInvalidCondition() {
        assertThrows(IllegalArgumentException.class, () -> in(new Comparable[0]), "values must not be empty or null");
        assertThrows(IllegalArgumentException.class, () -> in(new byte[0][0]), "values must not be empty or null");
        assertThrows(IllegalArgumentException.class, () -> notIn(new Comparable[0]), "values must not be empty or null");
        assertThrows(IllegalArgumentException.class, () -> notIn(new byte[0][0]), "values must not be empty or null");
    }

    static Stream<Arguments> testExpression() {
        return Stream.of(
                of(and(columnValue("a", nullValue())), "A IS NULL", OBJECT_EMPTY_ARRAY),
                of(
                        and(columnValue("a", nullValue()), columnValue("b", notIn("b1", "b2", "b3"))),
                        "(A IS NULL) AND (B NOT IN (?, ?, ?))",
                        new Object[] {"b1", "b2", "b3"}
                ),
                of(
                        and(columnValue("a", nullValue()), columnValue("b", notIn("b1", "b2", "b3")), columnValue("c", equalTo("c1"))),
                        "(A IS NULL) AND (B NOT IN (?, ?, ?)) AND (C = ?)",
                        new Object[] {"b1", "b2", "b3", "c1"}
                ),
                of(or(columnValue("a", nullValue())), "A IS NULL", OBJECT_EMPTY_ARRAY),
                of(
                        or(columnValue("a", nullValue()), columnValue("b", notIn("b1", "b2", "b3"))),
                        "(A IS NULL) OR (B NOT IN (?, ?, ?))",
                        new Object[] {"b1", "b2", "b3"}
                ),
                of(
                        or(columnValue("a", nullValue()), columnValue("b", notIn("b1", "b2", "b3")), columnValue("c", equalTo("c1"))),
                        "(A IS NULL) OR (B NOT IN (?, ?, ?)) OR (C = ?)",
                        new Object[] {"b1", "b2", "b3", "c1"}
                ),
                of(
                        not(and(columnValue("a", equalTo("a1")), columnValue("b", equalTo("b1")))),
                        "NOT ((A = ?) AND (B = ?))",
                        new Object[] {"a1", "b1"}
                )
        );
    }

    @ParameterizedTest
    @MethodSource
    void testExpression(Criteria criteria, String wherePart, Object[] arguments) {
        SqlSerializer ser = new SqlSerializer.Builder()
                .tableName("test")
                .columns(Set.of("A", "B", "C"))
                .where(criteria)
                .build();

        assertThat(ser.toString(), endsWith(wherePart));
        assertArrayEquals(arguments, ser.getArguments());
    }

    static Stream<Arguments> testInvalidExpression() {
        return Stream.<Consumer<Expression[]>>of(
                Criteria::and,
                Criteria::or
        ).map(Arguments::of);
    }

    @ParameterizedTest
    @MethodSource
    void testInvalidExpression(Consumer<Expression[]> consumer) {
        assertThrows(IllegalArgumentException.class, () -> consumer.accept(null), "expressions must not be empty or null");
        assertThrows(IllegalArgumentException.class, () -> consumer.accept(new Expression[0]), "expressions must not be empty or null");
        assertThrows(IllegalArgumentException.class, () -> consumer.accept(new Expression[]{columnValue("a", equalTo("a1")), null}),
                "expressions must not be empty or null");
    }

    @Test
    void testColumnNameValidation() {
        IllegalArgumentException iae = assertThrows(
                IllegalArgumentException.class,
                () -> new SqlSerializer.Builder()
                        .tableName("test")
                        .where(columnValue("a", equalTo("a")))
                        .build()
        );

        assertThat(iae.getMessage(), containsString("The columns of the table must be specified to validate input"));

        iae = assertThrows(
                IllegalArgumentException.class,
                () -> new SqlSerializer.Builder()
                        .tableName("test")
                        .columns(Set.of("B"))
                        .where(columnValue("a", equalTo("a")))
                        .build()
        );

        assertThat(iae.getMessage(), containsString("Unexpected column name: A"));
    }

    @Test
    void testQuote() {
        SqlSerializer ser = new SqlSerializer.Builder()
                .tableName("Test")
                .columns(Set.of("Aa"))
                .where(columnValue(quote("Aa"), equalTo(1)))
                .build();

        assertThat(ser.toString(), endsWith(format("FROM {} WHERE {} = ?", quote("Test"), quote("Aa"))));
        assertArrayEquals(new Object[]{1}, ser.getArguments());
    }

    @Test
    void testIndexName() {
        SqlSerializer ser = new SqlSerializer.Builder()
                .tableName("test")
                .indexName("idx_a")
                .columns(Set.of("a"))
                .where(columnValue(quote("a"), equalTo(1)))
                .build();

        assertThat(ser.toString(), startsWith("SELECT /*+ FORCE_INDEX(\"IDX_A\") */ * FROM"));
        assertArrayEquals(new Object[]{1}, ser.getArguments());

        ser = new SqlSerializer.Builder()
                .tableName("test")
                .indexName("PUBLIC.idx_a")
                .columns(Set.of("a"))
                .where(columnValue(quote("a"), equalTo(1)))
                .build();

        assertThat(ser.toString(), startsWith("SELECT /*+ FORCE_INDEX(\"PUBLIC.IDX_A\") */ * FROM"));
        assertArrayEquals(new Object[]{1}, ser.getArguments());

        IllegalArgumentException iae = assertThrows(
                IllegalArgumentException.class,
                () -> new SqlSerializer.Builder()
                        .tableName("test")
                        .indexName("'idx_a'")
                        .columns(Set.of("a"))
                        .where(columnValue(quote("a"), equalTo(1)))
                        .build()
        );

        assertThat(iae.getMessage(), containsString("Index name must be alphanumeric with underscore and start with letter. Was: 'idx_a'"));

        iae = assertThrows(
                IllegalArgumentException.class,
                () -> new SqlSerializer.Builder()
                        .tableName("test")
                        .indexName("1idx_a")
                        .columns(Set.of("a"))
                        .where(columnValue(quote("a"), equalTo(1)))
                        .build()
        );

        assertThat(iae.getMessage(), containsString("Index name must be alphanumeric with underscore and start with letter. Was: 1idx_a"));
    }
}
