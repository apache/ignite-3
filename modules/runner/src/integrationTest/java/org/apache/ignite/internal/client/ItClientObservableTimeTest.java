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

package org.apache.ignite.internal.client;

import org.apache.ignite.InitParametersBuilder;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.testframework.TestIgnitionManager;
import org.apache.ignite.internal.wrapper.Wrappers;
import org.apache.ignite.sql.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** Set of tests to make sure thin client uses suitable {@link HybridTimestampTracker} for sql. */
@SuppressWarnings("ConcatenationWithEmptyString")
public class ItClientObservableTimeTest extends ClusterPerClassIntegrationTest {
    @Override
    protected int initialNodes() {
        return 1;
    }

    @AfterEach
    void cleanUp() {
        dropAllTables();
    }

    @Override
    protected void configureInitParameters(InitParametersBuilder builder) {
        builder.clusterConfiguration(TestIgnitionManager.PRODUCTION_CLUSTER_CONFIG_STRING);
    }

    @Test
    void observableTimeAdjustmentWorksForQueryStartedFromThinClient() {
        try (
                IgniteClient client1 = newClient();
                IgniteClient client2 = newClient();
                ResultSet<?> ignored = client1.sql().execute(null, "CREATE TABLE my_table (id INT PRIMARY KEY, val INT)")
        ) {
            // Should not throw
            try (ResultSet<?> rs = client2.sql().execute(null, "SELECT * FROM my_table")) {
                while (rs.hasNext()) {
                    rs.next();
                }
            }
        }
    }

    @Test
    void startOfRoTxInScriptAccountForPreviouslyExecutedStatements() {
        try (IgniteClient client = newClient()) {
            // Should not throw
            client.sql().executeScript(""
                    + "CREATE TABLE my_table (id INT PRIMARY KEY, val INT);"
                    + "START TRANSACTION READ ONLY;"
                    + "SELECT COUNT(val) FROM my_table;"
                    + "COMMIT;"
            );
        }
    }

    private static IgniteClient newClient() {
        return IgniteClient.builder()
                .addresses("localhost:" + Wrappers.unwrap(CLUSTER.aliveNode(), IgniteImpl.class).clientAddress().port())
                .build();
    }
}
