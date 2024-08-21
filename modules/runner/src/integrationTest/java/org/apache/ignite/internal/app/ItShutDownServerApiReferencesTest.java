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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.lang.IgniteException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Makes sure that references to API objects obtained in embedded mode stop functioning after the node gets shut down.
 */
class ItShutDownServerApiReferencesTest extends ClusterPerClassIntegrationTest {
    private static IgniteServerImpl server;

    private static References beforeShutdown;

    @Override
    protected int initialNodes() {
        return 1;
    }

    @BeforeAll
    void init() throws Exception {
        server = (IgniteServerImpl) CLUSTER.server(0);

        server.api().sql().executeScript("CREATE TABLE test (id INT PRIMARY KEY, val VARCHAR)");

        beforeShutdown = new References(server);

        assertThat(server.shutdownAsync(), willCompleteSuccessfully());
    }

    @ParameterizedTest
    @EnumSource(SyncApiOperation.class)
    void syncOperationsThrowAfterShutdown(SyncApiOperation operation) {
        IgniteException ex = assertThrows(IgniteException.class, () -> operation.execute(beforeShutdown));
        assertThat(ex.getMessage(), is("The node is already shut down."));
    }

    @ParameterizedTest
    @EnumSource(AsyncApiOperation.class)
    void asyncOperationsWorkAfterRestart(AsyncApiOperation operation) {
        assertThat(operation.execute(beforeShutdown), willThrow(IgniteException.class, 10, SECONDS, "The node is already shut down."));
    }
}
