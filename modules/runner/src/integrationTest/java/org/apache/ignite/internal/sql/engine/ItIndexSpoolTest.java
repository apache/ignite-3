/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.sql.engine;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.params.ParameterizedTest.ARGUMENTS_PLACEHOLDER;

import java.util.List;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.table.Table;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Index spool test.
 */
@Disabled("https://issues.apache.org/jira/browse/IGNITE-17612")
public class ItIndexSpoolTest extends AbstractBasicIntegrationTest {
    private static final IgniteLogger LOG = Loggers.forClass(AbstractBasicIntegrationTest.class);

    /**
     * After each.
     */
    @AfterEach
    protected void cleanUp() {
        if (LOG.isInfoEnabled()) {
            LOG.info("Start cleanUp()");
        }

        //TODO: https://issues.apache.org/jira/browse/IGNITE-17562
        // Remove this, indices must be dropped together with the table.
        CLUSTER_NODES.get(0).tables().tables().stream()
                .map(Table::name)
                .forEach(name -> sql("DROP INDEX IF EXISTS " + name + "_JID_IDX"));

        CLUSTER_NODES.get(0).tables().tables().stream()
                .map(Table::name)
                .forEach(CLUSTER_NODES.get(0).tables()::dropTable);

        if (LOG.isInfoEnabled()) {
            LOG.info("End cleanUp()");
        }
    }

    /**
     * Test.
     */
    @ParameterizedTest(name = "tableSize=" + ARGUMENTS_PLACEHOLDER)
    @ValueSource(ints = {1, 10, 512, 513, 2000})
    public void test(int rows) {
        prepareDataSet(rows);

        var res = sql("SELECT /*+ DISABLE_RULE('NestedLoopJoinConverter', 'MergeJoinConverter') */"
                        + "T0.val, T1.val FROM TEST0 as T0 "
                        + "JOIN TEST1 as T1 on T0.jid = T1.jid "
        );

        assertThat(res.size(), is(rows));

        res.forEach(r -> assertThat(r.get(0), is(r.get(1))));
    }

    private void prepareDataSet(int rowsCount) {
        Object[][] dataRows = new Object[rowsCount][];

        for (int i = 0; i < rowsCount; i++) {
            dataRows[i] = new Object[]{i, i + 1, "val_" + i};
        }

        for (String name : List.of("TEST0", "TEST1")) {
            sql("CREATE TABLE " + name + "(id INT PRIMARY KEY, jid INT, val VARCHAR) WITH replicas=2,partitions=10");

            // TODO: https://issues.apache.org/jira/browse/IGNITE-17304 uncomment this
            // sql("CREATE INDEX " + name + "_jid_idx ON " + name + "(jid)");

            insertData("PUBLIC." + name, new String[]{"ID", "JID", "VAL"}, dataRows);
        }
    }
}
