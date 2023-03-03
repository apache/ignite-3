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

package org.apache.ignite.internal;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.sql.engine.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.testframework.WithSystemProperty;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * There are tests which have predefined and unchanged metadata.
 * The test check that the behavior of the cluster whit property IGNITE_GET_METADATA_LOCALLY_ONLY is correct.
 */
@WithSystemProperty(key = "IGNITE_GET_METADATA_LOCALLY_ONLY", value = "true")
public class ItNoSyncMetadataTest extends ClusterPerClassIntegrationTest {

    /**
     * Creates a table and waits when the metadata synchronizes among nodes of the cluster.
     * The method invokes before the test starts.
     */
    @BeforeAll
    void startNodes() {
        sql("CREATE TABLE t1 (id INT PRIMARY KEY, c1 INT NOT NULL, c2 INT, c3 INT)");

        try {
            Assertions.assertTrue(IgniteTestUtils.waitForCondition(
                    () -> CLUSTER_NODES.stream().filter(ignite -> ignite.tables().tables().size() != 1).collect(
                            Collectors.toList()).isEmpty(), 10_000));
        } catch (InterruptedException e) {
            Assertions.fail("Waiting of table creation on all node was interrupted.");
        }

        log.info("All metadata have been changed and synchronized over the whole cluster.");
    }

    /**
     * Inserts data to the preconfigured table and checks it from each node.
     */
    @Test
    public void test() {
        insertData("T1", List.of("ID", "C1", "C2", "C3"),
                new Object[]{0, 1, 1, 1},
                new Object[]{1, 2, null, 2},
                new Object[]{2, 2, 2, 2},
                new Object[]{3, 3, 3, null},
                new Object[]{4, 3, 3, 3},
                new Object[]{5, 4, 4, 4}
        );

        for (Ignite ignite : CLUSTER_NODES) {
            checkData(ignite.tables().table("T1"), new String[]{"ID", "C1", "C2", "C3"},
                    new Object[]{0, 1, 1, 1},
                    new Object[]{1, 2, null, 2},
                    new Object[]{2, 2, 2, 2},
                    new Object[]{3, 3, 3, null},
                    new Object[]{4, 3, 3, 3},
                    new Object[]{5, 4, 4, 4});
        }
    }
}
