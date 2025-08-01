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

package org.apache.ignite.internal.cli.commands.recovery.cluster;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.app.IgniteServerImpl;
import org.apache.ignite.internal.cli.CliIntegrationTest;
import org.jetbrains.annotations.Nullable;

abstract class ItSystemDisasterRecoveryCliTest extends CliIntegrationTest {
    static void waitTillNodeRestartsInternally(int nodeIndex) throws InterruptedException {
        // restartFuture() becomes non-null when restart is initiated.

        assertTrue(
                waitForCondition(() -> restartFuture(nodeIndex) != null, SECONDS.toMillis(20)),
                "Node did not attempt to be restarted in time"
        );
        assertThat(restartFuture(nodeIndex), willCompleteSuccessfully());
    }

    @Nullable
    private static CompletableFuture<Void> restartFuture(int nodeIndex) {
        return ((IgniteServerImpl) CLUSTER.server(nodeIndex)).restartFuture();
    }
}
