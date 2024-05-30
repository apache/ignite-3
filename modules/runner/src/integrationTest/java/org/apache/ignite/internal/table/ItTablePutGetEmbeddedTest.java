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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.stream.Stream;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.IgniteTables;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests to check that rows can be inserted and retrieved through embedded client.
 */
public class ItTablePutGetEmbeddedTest extends ClusterPerClassIntegrationTest {
    @AfterEach
    void dropTables() {
        for (Table table : tables().tables()) {
            sql("DROP TABLE " + table.name());
        }
    }

    @ParameterizedTest
    @MethodSource("tableDefinitions")
    @SuppressWarnings("DataFlowIssue")
    void putGetInTableWithDifferentOrdersOfColumnsInKeyAndColocationKey(String tableDefinitionDdl) {
        sql(tableDefinitionDdl);

        {
            KeyValueView<Tuple, Tuple> kvView = tables().table("test").keyValueView();

            for (int i = 0; i < 10; i++) {
                Tuple key = Tuple.create()
                        .set("c1", i)
                        .set("c3", i * 100);

                Tuple val = Tuple.create()
                        .set("c2", i * 10);

                kvView.put(null, key, val);
            }

            for (int i = 0; i < 10; i++) {
                Tuple key = Tuple.create()
                        .set("c1", i)
                        .set("c3", i * 100);

                assertEquals(i * 10, kvView.get(null, key).intValue("c2"));
                assertEquals(i * 10, kvView.get(null, key).intValue(0));
            }
        }

        {
            RecordView<Tuple> recordView = tables().table("test").recordView();

            for (int i = 100; i < 110; i++) {
                Tuple record = Tuple.create()
                        .set("c1", i)
                        .set("c2", i * 10)
                        .set("c3", i * 100);

                recordView.insert(null, record);
            }

            for (int i = 100; i < 110; i++) {
                Tuple key = Tuple.create()
                        .set("c1", i)
                        .set("c3", i * 100);

                assertEquals(i * 10, recordView.get(null, key).intValue("c2"));
                assertEquals(i * 10, recordView.get(null, key).intValue(1));
            }
        }
    }

    private static Stream<Arguments> tableDefinitions() {
        return Stream.of(
                "CREATE TABLE test (c1 INT, c2 INT, c3 INT, PRIMARY KEY (c1, c3)) COLOCATE BY (c1)",
                "CREATE TABLE test (c1 INT, c2 INT, c3 INT, PRIMARY KEY (c1, c3)) COLOCATE BY (c3)",
                "CREATE TABLE test (c1 INT, c2 INT, c3 INT, PRIMARY KEY (c1, c3)) COLOCATE BY (c1, c3)",
                "CREATE TABLE test (c1 INT, c2 INT, c3 INT, PRIMARY KEY (c1, c3)) COLOCATE BY (c3, c1)",
                "CREATE TABLE test (c1 INT, c2 INT, c3 INT, PRIMARY KEY (c3, c1)) COLOCATE BY (c1)",
                "CREATE TABLE test (c1 INT, c2 INT, c3 INT, PRIMARY KEY (c3, c1)) COLOCATE BY (c3)",
                "CREATE TABLE test (c1 INT, c2 INT, c3 INT, PRIMARY KEY (c3, c1)) COLOCATE BY (c1, c3)",
                "CREATE TABLE test (c1 INT, c2 INT, c3 INT, PRIMARY KEY (c3, c1)) COLOCATE BY (c3, c1)"
        ).map(Arguments::of);
    }

    IgniteTables tables() {
        return CLUSTER.aliveNode().tables();
    }
}
