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

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.internal.testframework.matchers.TupleMatcher.tupleValue;
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
import static org.apache.ignite.table.criteria.CriteriaQueryOptions.builder;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.google.common.collect.Lists;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.AsyncCursor;
import org.apache.ignite.lang.Cursor;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.Nullable;
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
                "CREATE TABLE {} (id INT PRIMARY KEY, name VARCHAR, price DOUBLE, hash VARBINARY)",
                DEFAULT_TABLE_NAME
        ));

        insertData(DEFAULT_TABLE_NAME, List.of("ID", "name", "price", "hash"), new Object[][]{
                {0, "name0", 0.0d, null},
                {1, "name1", 10.0d, "name1".getBytes()},
                {2, "name2", 20.0d, "name2".getBytes()}
        });
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

        IgniteTestUtils.assertThrows(
                IgniteException.class,
                () -> view.query(null, columnValue("id", equalTo("2"))),
                "Dynamic parameter requires adding explicit type cast"
        );

        try (Cursor<Tuple> cur = view.query(null, null)) {
            assertThat(Lists.newArrayList(cur), containsInAnyOrder(
                    allOf(tupleValue("id", is(0)), tupleValue("name", is("name0")), tupleValue("price", is(0.0d))),
                    allOf(tupleValue("id", is(1)), tupleValue("name", is("name1")), tupleValue("price", is(10.0d))),
                    allOf(tupleValue("id", is(2)), tupleValue("name", is("name2")), tupleValue("price", is(20.0d)))
            ));
        }

        try (Cursor<Tuple> cur = view.query(null, columnValue("id", equalTo(2)))) {
            assertThat(Lists.newArrayList(cur), containsInAnyOrder(
                    allOf(tupleValue("id", is(2)), tupleValue("name", is("name2")), tupleValue("price", is(20.0d)))
            ));
        }

        try (Cursor<Tuple> cur = view.query(null, columnValue("hash", equalTo("name2".getBytes())))) {
            assertThat(Lists.newArrayList(cur), containsInAnyOrder(
                    allOf(tupleValue("id", is(2)), tupleValue("name", is("name2")), tupleValue("price", is(20.0d)))
            ));
        }

        try (Cursor<Tuple> cur = view.query(null, columnValue("id", notEqualTo(2)))) {
            assertThat(Lists.newArrayList(cur), containsInAnyOrder(
                    allOf(tupleValue("id", is(0)), tupleValue("name", is("name0")), tupleValue("price", is(0.0d))),
                    allOf(tupleValue("id", is(1)), tupleValue("name", is("name1")), tupleValue("price", is(10.0d)))
            ));
        }

        try (Cursor<Tuple> cur = view.query(null, columnValue("hash", notEqualTo("name2".getBytes())))) {
            assertThat(Lists.newArrayList(cur), containsInAnyOrder(
                    allOf(tupleValue("id", is(1)), tupleValue("name", is("name1")), tupleValue("price", is(10.0d)))
            ));
        }

        try (Cursor<Tuple> cur = view.query(null, columnValue("id", greaterThan(1)))) {
            assertThat(Lists.newArrayList(cur), containsInAnyOrder(
                    allOf(tupleValue("id", is(2)), tupleValue("name", is("name2")), tupleValue("price", is(20.0d)))
            ));
        }

        try (Cursor<Tuple> cur = view.query(null, columnValue("id", greaterThanOrEqualTo(1)))) {
            assertThat(Lists.newArrayList(cur), containsInAnyOrder(
                    allOf(tupleValue("id", is(1)), tupleValue("name", is("name1")), tupleValue("price", is(10.0d))),
                    allOf(tupleValue("id", is(2)), tupleValue("name", is("name2")), tupleValue("price", is(20.0d)))
            ));
        }

        try (Cursor<Tuple> cur = view.query(null, columnValue("id", lessThan(1)))) {
            assertThat(Lists.newArrayList(cur), containsInAnyOrder(
                    allOf(tupleValue("id", is(0)), tupleValue("name", is("name0")), tupleValue("price", is(0.0d)))
            ));
        }

        try (Cursor<Tuple> cur = view.query(null, columnValue("id", lessThanOrEqualTo(1)))) {
            assertThat(Lists.newArrayList(cur), containsInAnyOrder(
                    allOf(tupleValue("id", is(0)), tupleValue("name", is("name0")), tupleValue("price", is(0.0d))),
                    allOf(tupleValue("id", is(1)), tupleValue("name", is("name1")), tupleValue("price", is(10.0d)))
            ));
        }

        try (Cursor<Tuple> cur = view.query(null, columnValue("hash", nullValue()))) {
            assertThat(Lists.newArrayList(cur), containsInAnyOrder(
                    allOf(tupleValue("id", is(0)), tupleValue("name", is("name0")), tupleValue("price", is(0.0d)))
            ));
        }

        try (Cursor<Tuple> cur = view.query(null, columnValue("hash", notNullValue()))) {
            assertThat(Lists.newArrayList(cur), containsInAnyOrder(
                    allOf(tupleValue("id", is(1)), tupleValue("name", is("name1")), tupleValue("price", is(10.0d))),
                    allOf(tupleValue("id", is(2)), tupleValue("name", is("name2")), tupleValue("price", is(20.0d)))
            ));
        }

        try (Cursor<Tuple> cur = view.query(null, columnValue("id", in(1, 2)))) {
            assertThat(Lists.newArrayList(cur), containsInAnyOrder(
                    allOf(tupleValue("id", is(1)), tupleValue("name", is("name1")), tupleValue("price", is(10.0d))),
                    allOf(tupleValue("id", is(2)), tupleValue("name", is("name2")), tupleValue("price", is(20.0d)))
            ));
        }

        try (Cursor<Tuple> cur = view.query(null, columnValue("id", notIn(1, 2)))) {
            assertThat(Lists.newArrayList(cur), containsInAnyOrder(
                    allOf(tupleValue("id", is(0)), tupleValue("name", is("name0")), tupleValue("price", is(0.0d)))
            ));
        }
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
        RecordView<Product> view = ignite.tables().table(DEFAULT_TABLE_NAME).recordView(Product.class);

        IgniteTestUtils.assertThrows(
                IgniteException.class,
                () -> view.query(null, columnValue("id", equalTo("2"))),
                "Dynamic parameter requires adding explicit type cast"
        );

        try (Cursor<Product> cur = view.query(null, null)) {
            assertThat(Lists.newArrayList(cur), containsInAnyOrder(
                    new Product(0, "name0", 0.0d, null),
                    new Product(1, "name1", 10.0d, "name1".getBytes()),
                    new Product(2, "name2", 20.0d, "name2".getBytes())
            ));
        }

        try (Cursor<Product> cur = view.query(null, columnValue("id", equalTo(2)))) {
            assertThat(Lists.newArrayList(cur), containsInAnyOrder(
                    new Product(2, "name2", 20.0d, "name2".getBytes())
            ));
        }

        try (Cursor<Product> cur = view.query(null, columnValue("hash", equalTo("name2".getBytes())))) {
            assertThat(Lists.newArrayList(cur), containsInAnyOrder(
                    new Product(2, "name2", 20.0d, "name2".getBytes())
            ));
        }

        try (Cursor<Product> cur = view.query(null, columnValue("id", notEqualTo(2)))) {
            assertThat(Lists.newArrayList(cur), containsInAnyOrder(
                    new Product(0, "name0", 0.0d, null),
                    new Product(1, "name1", 10.0d, "name1".getBytes())
            ));
        }

        try (Cursor<Product> cur = view.query(null, columnValue("hash", notEqualTo("name2".getBytes())))) {
            assertThat(Lists.newArrayList(cur), containsInAnyOrder(
                    new Product(1, "name1", 10.0d, "name1".getBytes())
            ));
        }

        try (Cursor<Product> cur = view.query(null, columnValue("id", greaterThan(1)))) {
            assertThat(Lists.newArrayList(cur), containsInAnyOrder(
                    new Product(2, "name2", 20.0d, "name2".getBytes())
            ));
        }

        try (Cursor<Product> cur = view.query(null, columnValue("id", greaterThanOrEqualTo(1)))) {
            assertThat(Lists.newArrayList(cur), containsInAnyOrder(
                    new Product(1, "name1", 10.0d, "name1".getBytes()),
                    new Product(2, "name2", 20.0d, "name2".getBytes())
            ));
        }

        try (Cursor<Product> cur = view.query(null, columnValue("id", lessThan(1)))) {
            assertThat(Lists.newArrayList(cur), containsInAnyOrder(
                    new Product(0, "name0", 0.0d, null)
            ));
        }

        try (Cursor<Product> cur = view.query(null, columnValue("id", lessThanOrEqualTo(1)))) {
            assertThat(Lists.newArrayList(cur), containsInAnyOrder(
                    new Product(0, "name0", 0.0d, null),
                    new Product(1, "name1", 10.0d, "name1".getBytes())
            ));
        }

        try (Cursor<Product> cur = view.query(null, columnValue("hash", nullValue()))) {
            assertThat(Lists.newArrayList(cur), containsInAnyOrder(
                    new Product(0, "name0", 0.0d, null)
            ));
        }

        try (Cursor<Product> cur = view.query(null, columnValue("hash", notNullValue()))) {
            assertThat(Lists.newArrayList(cur), containsInAnyOrder(
                    new Product(1, "name1", 10.0d, "name1".getBytes()),
                    new Product(2, "name2", 20.0d, "name2".getBytes())
            ));
        }

        try (Cursor<Product> cur = view.query(null, columnValue("id", in(1, 2)))) {
            assertThat(Lists.newArrayList(cur), containsInAnyOrder(
                    new Product(1, "name1", 10.0d, "name1".getBytes()),
                    new Product(2, "name2", 20.0d, "name2".getBytes())
            ));
        }

        try (Cursor<Product> cur = view.query(null, columnValue("id", notIn(1, 2)))) {
            assertThat(Lists.newArrayList(cur), containsInAnyOrder(
                    new Product(0, "name0", 0.0d, null)
            ));
        }

        try (Cursor<Product> cur = view.query(null, columnValue("id", equalTo(2)))) {
            assertThat(Lists.newArrayList(cur), containsInAnyOrder(
                    new Product(2, "name2", 20.0d, "name2".getBytes())
            ));
        }

        try (Cursor<Product> cur = view.query(null, not(columnValue("id", equalTo(2))))) {
            assertThat(Lists.newArrayList(cur), containsInAnyOrder(
                    new Product(0, "name0", 0.0d, null),
                    new Product(1, "name1", 10.0d, "name1".getBytes())
            ));
        }
    }

    @Test
    public void testOptions() {
        RecordView<Product> view = CLIENT.tables().table(DEFAULT_TABLE_NAME).recordView(Product.class);

        AsyncCursor<Product> ars = await(view.queryAsync(null, null, builder().pageSize(2).build()));

        assertNotNull(ars);
        assertEquals(2, ars.currentPageSize());
        await(ars.closeAsync());
    }

    private static class Product {
        int id;

        String name;

        double price;

        byte @Nullable [] hash;

        public Product() {
            //No-op.
        }

        public Product(int id, String name, double price, byte @Nullable [] hash) {
            this.id = id;
            this.name = name;
            this.price = price;
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
            Product product = (Product) o;
            return id == product.id && Double.compare(price, product.price) == 0 && Objects.equals(name, product.name) &&
                    Arrays.equals(hash, product.hash);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, name, price, Arrays.hashCode(hash));
        }
    }
}
