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

import static java.util.stream.Collectors.joining;
import static org.apache.ignite.internal.sql.engine.util.QueryChecker.containsIndexScan;

import java.util.List;
import java.util.stream.Stream;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Integration test of index building.
 */
public class ItBuildIndexTest extends ClusterPerClassIntegrationTest {
    private static final String ZONE_NAME = "zone_table";

    private static final String TABLE_NAME = "test_table";

    private static final String INDEX_NAME = "test_index";

    @AfterEach
    void tearDown() {
        sql("DROP TABLE IF EXISTS " + TABLE_NAME);
    }

    @ParameterizedTest
    @MethodSource("replicas")
    void testBuildIndexOnStableTopology(int replicas) throws Exception {
        sql(IgniteStringFormatter.format("CREATE ZONE IF NOT EXISTS {} WITH REPLICAS={}, PARTITIONS={}",
                ZONE_NAME, replicas, 2
        ));

        sql(IgniteStringFormatter.format(
                "CREATE TABLE {} (i0 INTEGER PRIMARY KEY, i1 INTEGER) WITH PRIMARY_ZONE='{}'",
                TABLE_NAME, ZONE_NAME.toUpperCase()
        ));

        sql(IgniteStringFormatter.format(
                "INSERT INTO {} VALUES {}",
                TABLE_NAME, toValuesString(List.of(1, 1), List.of(2, 2), List.of(3, 3), List.of(4, 4), List.of(5, 5))
        ));

        sql(IgniteStringFormatter.format("CREATE INDEX {} ON {} (i1)", INDEX_NAME, TABLE_NAME));

        // TODO: IGNITE-19150 We are waiting for schema synchronization to avoid races to create and destroy indexes
        waitForIndexBuild(TABLE_NAME, INDEX_NAME);

        assertQuery(IgniteStringFormatter.format("SELECT * FROM {} WHERE i1 > 0", TABLE_NAME))
                .matches(containsIndexScan("PUBLIC", TABLE_NAME.toUpperCase(), INDEX_NAME.toUpperCase()))
                .returns(1, 1)
                .returns(2, 2)
                .returns(3, 3)
                .returns(4, 4)
                .returns(5, 5)
                .check();
    }

    private static int[] replicas() {
        return new int[]{1, 2, 3};
    }

    private static String toValuesString(List<Object>... values) {
        return Stream.of(values)
                .peek(Assertions::assertNotNull)
                .map(objects -> objects.stream().map(Object::toString).collect(joining(", ", "(", ")")))
                .collect(joining(", "));
    }
}
