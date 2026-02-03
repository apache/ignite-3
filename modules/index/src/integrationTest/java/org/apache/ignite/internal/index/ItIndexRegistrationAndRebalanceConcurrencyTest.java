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
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import java.time.Duration;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.junit.jupiter.api.Test;

class ItIndexRegistrationAndRebalanceConcurrencyTest extends ClusterPerTestIntegrationTest {
    private static final String TEST_ZONE_NAME = "test_zone";
    private static final String TEST_TABLE_NAME = "test_table";

    /**
     * This makes sure that partitions coming in/going out during index registration do not break anything.
     */
    @Test
    void indexRegistrationAndRebalanceConcurrency() {
        Ignite ignite = node(0);

        ignite.sql().executeScript(
                "CREATE ZONE " + TEST_ZONE_NAME + " WITH replicas=3, storage_profiles='" + DEFAULT_STORAGE_PROFILE + "';"
                        + "CREATE TABLE " + TEST_TABLE_NAME + " (key INT PRIMARY KEY, val VARCHAR(20)) ZONE test_zone"
        );

        waitForStoragesToBeCreated(ignite);

        initiateRebalanceOnTestZone(ignite);

        assertTimeoutPreemptively(
                Duration.ofSeconds(30),
                () -> ignite.sql().executeScript("CREATE INDEX test_index on " + TEST_TABLE_NAME + " (val)")
        );
    }

    private static void waitForStoragesToBeCreated(Ignite ignite) {
        ignite.sql().execute("SELECT * FROM " + TEST_TABLE_NAME).close();
    }

    private static void initiateRebalanceOnTestZone(Ignite ignite) {
        ignite.sql().executeScript("ALTER ZONE " + TEST_ZONE_NAME + " SET (replicas 2)");
    }
}
