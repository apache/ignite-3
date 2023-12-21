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
import static org.apache.ignite.internal.testframework.matchers.TupleMatcher.tupleValue;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasProperty;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.google.common.collect.Lists;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.InitParameters;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.TestIgnitionManager;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.Cursor;
import org.apache.ignite.sql.Session;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests for the criteria query table API.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
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

    @WorkDirectory
    private static Path WORK_DIR;

    private Ignite node;

    @BeforeAll
    void beforeAll(TestInfo testInfo) {
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

        node = future.join();

        startTable(node, TABLE_NAME);
        populateData(node, TABLE_NAME);
    }

    @AfterAll
    void afterAll(TestInfo testInfo) throws Exception {
        stopTable(node, TABLE_NAME);
        IgniteUtils.closeAll(() -> IgnitionManager.stop(testNodeName(testInfo, 0)));
    }

    @Test
    public void testBasicQueryCriteriaRecordBinaryView() {
        RecordView<Tuple> view = node.tables().table(TABLE_NAME).recordView();

        try (Cursor<Tuple> cur = view.queryCriteria(null, null)) {
            assertThat(Lists.newArrayList(cur), containsInAnyOrder(
                    tupleValue(COLUMN_KEY, is(0)),
                    tupleValue(COLUMN_KEY, is(1)),
                    tupleValue(COLUMN_KEY, is(2))
            ));
        }
    }

    @Disabled("https://issues.apache.org/jira/browse/IGNITE-20977")
    @Test
    public void testBasicQueryCriteriaRecordPojoView() {
        RecordView<TestPojo> view = node.tables().table(TABLE_NAME).recordView(TestPojo.class);

        try (Cursor<TestPojo> cur = view.queryCriteria(null, null)) {
            assertThat(Lists.newArrayList(cur), containsInAnyOrder(
                    hasProperty(COLUMN_KEY, is(0)),
                    hasProperty(COLUMN_KEY, is(1)),
                    hasProperty(COLUMN_KEY, is(2))
            ));
        }
    }

    private static void startTable(Ignite node, String tableName) {
        try (Session session = node.sql().createSession()) {
            session.execute(
                    null,
                    String.format("create table \"%s\" (key int primary key, valInt int, valStr varchar default 'default')", tableName)
            );
        }

        assertNotNull(node.tables().table(tableName));
    }

    private static void stopTable(Ignite node, String tableName) {
        try (Session session = node.sql().createSession()) {
            session.execute(null, "drop table " + tableName);
        }
    }

    private static void populateData(Ignite ignite, String tableName) {
        RecordView<Tuple> table = ignite.tables().table(tableName).recordView();

        for (int val = 0; val < 3; val++) {
            table.insert(null, Tuple.create(Map.of(COLUMN_KEY, val % 100, "valInt", val, "valStr", "some string row" + val)));
        }
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
