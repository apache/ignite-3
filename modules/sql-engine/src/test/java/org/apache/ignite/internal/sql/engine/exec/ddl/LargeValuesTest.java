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

package org.apache.ignite.internal.sql.engine.exec.ddl;

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.InternalSqlRow;
import org.apache.ignite.internal.sql.engine.exec.ScannableTable;
import org.apache.ignite.internal.sql.engine.framework.DataProvider;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders.AssignmentsProvider;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders.DefaultAssignmentsProvider;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders.DefaultDataProvider;
import org.apache.ignite.internal.sql.engine.framework.TestCluster;
import org.apache.ignite.internal.sql.engine.framework.TestNode;
import org.apache.ignite.internal.sql.engine.util.CursorUtils;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.util.AsyncCursor.BatchedResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;

public class LargeValuesTest extends BaseIgniteAbstractTest {

    private TestCluster testCluster;

    private TestNode gatewayNode;

    @BeforeEach
    public void setup() {
        testCluster = TestBuilders.cluster()
                .nodes("N1")
                .defaultDataProvider(new DefaultDataProvider() {
                    @Override
                    public ScannableTable get(String tableName) {
                        return TestBuilders.tableScan(DataProvider.fromCollection(List.of()));
                    }
                })
                .defaultAssignmentsProvider(new DefaultAssignmentsProvider() {
                    @Override
                    public AssignmentsProvider get(String tableName) {
                        return new AssignmentsProvider() {
                            @Override
                            public List<List<String>> get(int partitionsCount, boolean includeBackups) {
                                return IntStream.range(0, partitionsCount)
                                        .mapToObj(i -> List.of("N1"))
                                        .collect(Collectors.toList());
                            }
                        };
                    }
                })
                .build();

        testCluster.start();

        gatewayNode = testCluster.node("N1");
    }

    @AfterEach
    public void afterEach() throws Exception {
        if (testCluster != null) {
            testCluster.stop();
        }
    }

    @ParameterizedTest
    public void test(int size) throws Exception {
        {
            String stmt = format("CREATE TABLE t (id INT PRIMARY KEY, val VARBINARY({}))", size);
            AsyncSqlCursor<InternalSqlRow> cursor = gatewayNode.executeQuery(stmt);

            BatchedResult<InternalSqlRow> rs = cursor.requestNextAsync(1).join();
            assertNotNull(rs);
        }

        {
            AsyncSqlCursor<InternalSqlRow> cursor = gatewayNode.executeQuery("SELECT * FROM t");
            List<InternalSqlRow> rows = CursorUtils.getAllFromCursor(cursor);
            assertTrue(rows.isEmpty());
        }
    }


}
