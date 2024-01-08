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

import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.internal.testframework.matchers.TupleMatcher.tupleValue;
import static org.apache.ignite.table.criteria.CriteriaQueryOptions.builder;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.google.common.collect.Lists;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.AsyncCursor;
import org.apache.ignite.lang.Cursor;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;
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

        createTable(DEFAULT_TABLE_NAME, 1, 8);

        for (int i = 0; i < 3; i++) {
            insertPeople(DEFAULT_TABLE_NAME, new Person(i, "name" + i, 10.0d * i));
        }
    }

    @AfterAll
    void stopClient() throws Exception {
        IgniteUtils.closeAll(CLIENT);
    }

    private static Stream<Arguments> testRecordBinaryView() {
        return Stream.of(
                Arguments.of(CLIENT),
                Arguments.of(CLUSTER.aliveNode())
        );
    }

    @ParameterizedTest(autoCloseArguments = false)
    @MethodSource
    public void testRecordBinaryView(Ignite ignite) {
        RecordView<Tuple> view = ignite.tables().table(DEFAULT_TABLE_NAME).recordView();

        try (Cursor<Tuple> cur = view.query(null, null)) {
            assertThat(Lists.newArrayList(cur), containsInAnyOrder(
                    allOf(tupleValue("id", is(0)), tupleValue("name", is("name0")), tupleValue("salary", is(0.0d))),
                    allOf(tupleValue("id", is(1)), tupleValue("name", is("name1")), tupleValue("salary", is(10.0d))),
                    allOf(tupleValue("id", is(2)), tupleValue("name", is("name2")), tupleValue("salary", is(20.0d)))
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
        RecordView<Person> view = ignite.tables().table(DEFAULT_TABLE_NAME).recordView(Person.class);

        try (Cursor<Person> cur = view.query(null, null)) {
            assertThat(Lists.newArrayList(cur), containsInAnyOrder(
                    new Person(0, "name0", 0.0d),
                    new Person(1, "name1", 10.0d),
                    new Person(2, "name2", 20.0d)
            ));
        }
    }

    @Test
    public void testOptions() {
        RecordView<Person> view = CLIENT.tables().table(DEFAULT_TABLE_NAME).recordView(Person.class);

        AsyncCursor<Person> ars = await(view.queryAsync(null, null, builder().pageSize(2).build()));

        assertNotNull(ars);
        assertEquals(2, ars.currentPageSize());
        await(ars.closeAsync());
    }
}
