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

package org.apache.ignite.internal.partition.replicator;

import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import java.time.Duration;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.lang.IgniteSystemProperties;
import org.apache.ignite.internal.testframework.WithSystemProperty;
import org.apache.ignite.internal.testframework.failure.FailureManagerExtension;
import org.apache.ignite.internal.testframework.failure.MuteFailureManagerLogging;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@WithSystemProperty(key = IgniteSystemProperties.COLOCATION_FEATURE_FLAG, value = "true")
// TODO: https://issues.apache.org/jira/browse/IGNITE-24943 - remove the mute.
@ExtendWith(FailureManagerExtension.class)
@MuteFailureManagerLogging
class ItBigZoneOperationTest extends ClusterPerTestIntegrationTest {
    @Override
    protected int initialNodes() {
        return 1;
    }

    @Test
    void zoneWithManyPartitionsDoesNotCauseHangs() {
        Ignite node = cluster.node(0);

        assertTimeoutPreemptively(Duration.ofSeconds(20), () -> {
            node.sql().executeScript(
                    "CREATE ZONE TEST_ZONE WITH STORAGE_PROFILES='default', PARTITIONS=300;"
                            + "CREATE TABLE TEST_TABLE(id INT PRIMARY KEY, val VARCHAR(255)) ZONE TEST_ZONE"
            );

            node.sql().execute(null, "SELECT * FROM TEST_TABLE").close();
        });
    }
}
