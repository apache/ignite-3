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

package org.apache.ignite.internal.app;

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.junit.jupiter.api.BeforeAll;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.junitpioneer.jupiter.cartesian.CartesianTest.Enum;

/**
 * Tests for making sure that API references obtained in the embedded mode function as they should with respect to in-process restart.
 *
 * <ul>
 *     <li>References obtained after the restart work correctly</li>
 *     <li>References obtained before the restart continue to be functional (transparency)</li>
 * </ul>
 */
class ItInProcessRestartApiReferencesTest extends ClusterPerClassIntegrationTest {
    private static IgniteServerImpl server;

    private static References beforeRestart;

    private static References afterRestart;

    @Override
    protected int initialNodes() {
        return 1;
    }

    @BeforeAll
    void init() throws Exception {
        server = (IgniteServerImpl) CLUSTER.server(0);

        server.api().sql().executeScript("CREATE TABLE test (id INT PRIMARY KEY, val VARCHAR)");

        beforeRestart = new References(server);

        assertThat(server.restartAsync(), willCompleteSuccessfully());

        afterRestart = new References(server);
    }

    @CartesianTest
    void syncOperationsWorkAfterRestart(@Enum SyncApiOperation operation, @Enum ReferenceEpoch epoch) {
        assertDoesNotThrow(() -> operation.execute(epoch.references()));
    }

    @CartesianTest
    void asyncOperationsWorkAfterRestart(@Enum AsyncApiOperation operation, @Enum ReferenceEpoch epoch) {
        assertThat(operation.execute(epoch.references()), willCompleteSuccessfully());
    }

    private enum ReferenceEpoch {
        BEFORE_RESTART, AFTER_RESTART;

        private References references() {
            return this == BEFORE_RESTART ? beforeRestart : afterRestart;
        }
    }
}
