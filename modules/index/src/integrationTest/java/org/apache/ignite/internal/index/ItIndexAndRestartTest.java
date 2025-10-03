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

package org.apache.ignite.internal.index;

import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import java.time.Duration;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.junit.jupiter.api.Test;

class ItIndexAndRestartTest extends BaseSqlIntegrationTest {
    private static final String ZONE_NAME = "ZONE_TABLE";

    private static final String TABLE_NAME = "TEST_TABLE";

    /** Must be at least 2 to have a node without the partition; 3 is best as it allows to keep CMG/MG majority despite node restarts. */
    private static final int NODE_COUNT = 3;

    @Override
    protected int initialNodes() {
        return NODE_COUNT;
    }

    @Override
    protected int[] cmgMetastoreNodes() {
        return IntStream.range(0, NODE_COUNT).toArray();
    }

    @Test
    void indexOnTableWithOnePartitionCanBeCreatedAfterRestart() {
        sql(format("CREATE ZONE IF NOT EXISTS {} (REPLICAS {}, PARTITIONS {}) STORAGE PROFILES ['{}']",
                ZONE_NAME, 1, 1, DEFAULT_STORAGE_PROFILE
        ));

        sql(format(
                "CREATE TABLE {} (i0 INTEGER PRIMARY KEY, i1 INTEGER) ZONE {}",
                TABLE_NAME, ZONE_NAME
        ));

        for (int i = 0; i < NODE_COUNT; i++) {
            CLUSTER.restartNode(i);
        }

        assertTimeoutPreemptively(
                Duration.ofSeconds(10),
                () -> sql(format("CREATE INDEX idx ON {}(i1)", TABLE_NAME)),
                "Did not create index in time; probably something is broken, check the test logs"
        );

        for (Ignite node : CLUSTER.nodes()) {
            assertTimeoutPreemptively(
                    Duration.ofSeconds(10),
                    () -> node.tables().tables(),
                    "Could not obtain tables in time for " + node.name() + "; probably something is broken, check the node logs"
            );
        }
    }
}
