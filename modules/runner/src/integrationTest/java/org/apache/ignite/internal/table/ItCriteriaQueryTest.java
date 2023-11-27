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

import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.table.criteria.CriteriaBuilder.columnName;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.InitParameters;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.TestIgnitionManager;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.sql.Session;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.criteria.Criteria;
import org.apache.ignite.table.mapper.Mapper;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests for the criteria query table API.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class ItCriteriaQueryTest extends BaseIgniteAbstractTest {
    private static final String TABLE_NAME = "SOME_TABLE";

    private static final String COLUMN_KEY = "key";

    private static final int BASE_PORT = 3344;

    private static final String NODE_BOOTSTRAP_CFG = "{\n"
            + "  \"network\": {\n"
            + "    \"port\":{},\n"
            + "    \"nodeFinder\":{\n"
            + "      \"netClusterNodes\": [ {} ]\n"
            + "    }\n"
            + "  }\n"
            + "}";

    private static Ignite NODE;

    @WorkDirectory
    private static Path WORK_DIR;

    private Table table;

    @BeforeAll
    static void startNode(TestInfo testInfo) {
        String connectNodeAddr = "\"localhost:" + BASE_PORT + '\"';

        String nodeName = testNodeName(testInfo, 0);

        String config = IgniteStringFormatter.format(NODE_BOOTSTRAP_CFG, BASE_PORT, connectNodeAddr);

        CompletableFuture<Ignite> future = TestIgnitionManager.start(nodeName, config, WORK_DIR.resolve(nodeName));

        String metaStorageNodeName = testNodeName(testInfo, 0);

        InitParameters initParameters = InitParameters.builder()
                .destinationNodeName(metaStorageNodeName)
                .metaStorageNodeNames(List.of(metaStorageNodeName))
                .clusterName("cluster")
                .build();

        TestIgnitionManager.init(initParameters);

        assertThat(future, willCompleteSuccessfully());

        NODE = future.join();
    }

    @AfterAll
    static void stopNode(TestInfo testInfo) throws Exception {
        NODE = null;

        IgniteUtils.closeAll(() -> IgnitionManager.stop(testNodeName(testInfo, 0)));
    }

    @BeforeEach
    void createTable() {
        table = startTable(node(), TABLE_NAME);
        populateData(table.keyValueView());
    }

    @AfterEach
    void dropTable() {
        stopTable(node(), TABLE_NAME);

        table = null;
    }

    @Test
    public void testBasicQueryCriteriaRecordBinaryView() {
        try (var cursor = table.recordView().queryCriteria(null, null)) {
            assertThat(cursor.getAll(), hasSize(15));
        }

        try (var cursor = table.recordView().queryCriteria(null, columnName(COLUMN_KEY).equal(2))) {
            assertThat(cursor.getAll(), hasItem(tupleValue(COLUMN_KEY, equalTo(2))));
        }

        try (var cursor = table.recordView().queryCriteria(null, Criteria.not(Criteria.equal(COLUMN_KEY, 2)))) {
            assertThat(cursor.getAll(), not(hasItem(tupleValue(COLUMN_KEY, equalTo(2)))));
        }
    }

    @Disabled("https://issues.apache.org/jira/browse/IGNITE-18695")
    @Test
    public void testBasicQueryCriteriaRecordPojoView() {
        RecordView<TestPojo> view = table.recordView(TestPojo.class);

        try (var cursor = view.queryCriteria(null, null)) {
            assertThat(cursor.getAll(), hasSize(3));
        }

        try (var cursor = view.queryCriteria(null, columnName(COLUMN_KEY).equal(2))) {
            assertThat(cursor.getAll(), hasItem(hasProperty(COLUMN_KEY, equalTo(2))));
        }

        try (var cursor = view.queryCriteria(null, Criteria.not(Criteria.equal(COLUMN_KEY, 2)))) {
            assertThat(cursor.getAll(), not(hasItem(hasProperty(COLUMN_KEY, equalTo(2)))));
        }
    }

    @Disabled("https://issues.apache.org/jira/browse/IGNITE-16116")
    @Test
    public void testBasicQueryCriteriaKvBinaryView() {
        try (var cursor = table.keyValueView().queryCriteria(null, null)) {
            assertThat(cursor.getAll(), hasSize(15));
        }

        try (var cursor = table.keyValueView().queryCriteria(null, columnName("key").equal(0L))) {
            assertThat(cursor.getAll(), hasItem(hasProperty("key", equalTo(0L))));
        }

        try (var cursor = table.keyValueView().queryCriteria(null, Criteria.not(Criteria.equal("key", 0L)))) {
            assertThat(cursor.getAll(), not(hasItem(hasProperty("key", equalTo(0L)))));
        }
    }

    @Disabled("https://issues.apache.org/jira/browse/IGNITE-16116")
    @Test
    public void testBasicQueryCriteriaKvPojoView() {
        var view = table.keyValueView(Mapper.of(Integer.class), Mapper.of(TestPojo.class));

        try (var cursor = view.queryCriteria(null, null)) {
            assertThat(cursor.getAll(), hasSize(3));
        }

        try (var cursor = view.queryCriteria(null, columnName(COLUMN_KEY).equal(2))) {
            assertThat(cursor.getAll(), hasItem(hasKey(2)));
        }

        try (var cursor = view.queryCriteria(null, Criteria.not(Criteria.equal(COLUMN_KEY, 2)))) {
            assertThat(cursor.getAll(), not(hasItem(hasKey(2))));
        }
    }

    private static IgniteImpl node() {
        return (IgniteImpl) NODE;
    }

    private static Table startTable(Ignite node, String tableName) {
        try (Session session = node.sql().createSession()) {
            session.execute(
                    null,
                    String.format("create table \"%s\" (key int primary key, valInt int, valStr varchar default 'default')", tableName)
            );
        }

        Table table = node.tables().table(tableName);

        assertNotNull(table);

        return table;
    }

    private static void stopTable(Ignite node, String tableName) {
        try (Session session = node.sql().createSession()) {
            session.execute(null, "drop table " + tableName);
        }
    }

    private static void populateData(KeyValueView<Tuple, Tuple> keyValueView) {
        for (int val = 0; val < 15; val++) {
            Tuple tableKey = Tuple.create().set("key", val % 100);

            Tuple value = Tuple.create().set("valInt", val).set("valStr", "some string row" + val);

            keyValueView.put(null, tableKey, value);
        }
    }

    /**
     * Creates a matcher for matching tuple value.
     *
     * @param valueMatcher Matcher for matching tuple value.
     * @return Matcher for matching tuple value.
     */
    private static <T> Matcher<Tuple> tupleValue(String columnName, Matcher<T> valueMatcher) {
        return new FeatureMatcher<>(valueMatcher, "A tuple with value", "value") {
            @Override
            protected @Nullable T featureValueOf(Tuple actual) {
                return actual.value(columnName);
            }
        };
    }

    /**
     * Test class.
     */
    private static class TestPojo {
        public TestPojo() {
            //No-op.
        }

        public TestPojo(int key) {
            this.key = key;
        }

        int key;

        int valInt;
        
        String valStr;

        public int getKey() {
            return key;
        }

        public int getValInt() {
            return valInt;
        }

        public String getValStr() {
            return valStr;
        }
    }
}
