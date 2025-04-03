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

package org.apache.ignite.internal.sql.engine;

import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.sql.engine.util.Commons.IN_BUFFER_SIZE;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.List;
import java.util.stream.Stream;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Index spool test.
 */
@Disabled("https://issues.apache.org/jira/browse/IGNITE-21286")
public class ItIndexSpoolTest extends BaseSqlIntegrationTest {
    /**
     * After each.
     */
    @AfterEach
    protected void cleanUp() {
        dropAllTables();
        dropAllZonesExceptDefaultOne();
    }

    private static Stream<Arguments> rowsWithPartitionsArgs() {
        return Stream.of(
                Arguments.of(1, 1),
                Arguments.of(10, 1),
                Arguments.of(IN_BUFFER_SIZE, 1),
                Arguments.of(IN_BUFFER_SIZE + 1, 1),
                Arguments.of(2000, 1),
                Arguments.of(IN_BUFFER_SIZE, 2),
                Arguments.of(IN_BUFFER_SIZE + 1, 2));
    }

    /**
     * Test.
     */
    @ParameterizedTest(name = "tableSize={0}, partitions={1}")
    @MethodSource("rowsWithPartitionsArgs")
    public void test(int rows, int partitions) throws InterruptedException {
        prepareDataSet(rows, partitions);

        var res = sql("SELECT /*+ DISABLE_RULE('NestedLoopJoinConverter', 'MergeJoinConverter', 'HashJoinConverter') */"
                        + "T0.val, T1.val FROM TEST0 as T0 "
                        + "JOIN TEST1 as T1 on T0.jid = T1.jid "
        );

        assertThat(res.size(), is(rows));

        res.forEach(r -> assertThat(r.get(0), is(r.get(1))));
    }

    private void prepareDataSet(int rowsCount, int parts) {
        Object[][] dataRows = new Object[rowsCount][];

        for (int i = 0; i < rowsCount; i++) {
            dataRows[i] = new Object[]{i, i + 1, "val_" + i};
        }

        for (String name : List.of("TEST0", "TEST1")) {
            sql(String.format(
                    "CREATE ZONE %s (replicas 2, partitions %d) storage profiles ['%s']",
                    "ZONE_" + name,
                    parts,
                    DEFAULT_STORAGE_PROFILE
            ));
            sql(String.format("CREATE TABLE %s(id INT PRIMARY KEY, jid INT, val VARCHAR) ZONE %s",
                    name, "ZONE_" + name));

            sql("CREATE INDEX " + name + "_jid_idx ON " + name + "(jid)");

            insertData(name, List.of("ID", "JID", "VAL"), dataRows);
        }
    }
}
