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

package org.apache.ignite.internal.catalog;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import java.util.concurrent.ForkJoinPool;
import java.util.stream.IntStream;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.util.IgniteUtils;
import org.junit.jupiter.api.Test;

class ItConcurrentDdlsTest extends ClusterPerTestIntegrationTest {
    private static final String ZONE_NAME = "TEST_ZONE";

    @Override
    protected int initialNodes() {
        return 1;
    }

    /**
     * Makes sure we can request DDLs with high concurrency and not fall into 'no more retry attempts' trap.
     */
    @Test
    void createTablesConcurrently() {
        // Just 1 partition to make the test lighter and faster (number of partitions is not the focus of this test).
        createZoneWith1Partition();

        ForkJoinPool pool = new ForkJoinPool(10);

        assertDoesNotThrow(() -> pool.submit(this::createTablesInParallel).get());

        IgniteUtils.shutdownAndAwaitTermination(pool, 10, SECONDS);
    }

    private void createZoneWith1Partition() {
        node(0).sql().executeScript(
                "CREATE ZONE " + ZONE_NAME + " (partitions 1, replicas 1) storage profiles ['"
                        + CatalogService.DEFAULT_STORAGE_PROFILE + "']"
        );
    }

    private void createTablesInParallel() {
        IntStream.range(0, 30).parallel().forEach(n -> {
            String tableName = "TEST" + n;

            node(0).sql().executeScript(
                    "CREATE TABLE " + tableName + " (id INT PRIMARY KEY, val VARCHAR) ZONE " + ZONE_NAME
            );
        });
    }
}
