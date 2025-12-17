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

package org.apache.ignite.internal.network;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.apache.ignite.internal.util.ExceptionUtils.unwrapRootCause;
import static org.apache.ignite.lang.ErrorGroups.Network.ADDRESS_UNRESOLVED_ERR;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.testframework.log4j2.LogInspector;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * Tests that node finder failure triggers failure manager.
 */
class ItStaticNodeFinderTest extends ClusterPerClassIntegrationTest {
    @Override
    protected int initialNodes() {
        return 1;
    }

    @Override
    protected String getNodeBootstrapConfigTemplate() {
        return "ignite {\n"
                + "  network: {\n"
                + "    nodeFinder.netClusterNodes: [ \"bad.host:1234\" ]\n"
                + "  },\n"
                + "}";
    }

    @Override
    protected boolean needInitializeCluster() {
        return false;
    }

    @Test
    void testNodeShutdownOnNodeFinderFailure(TestInfo testInfo) {
        LogInspector logInspector = new LogInspector(FailureManager.class.getName());

        logInspector.addHandler(
                evt -> {
                    log.debug(evt.getMessage().getFormattedMessage());

                    return false;
                }, () -> {});

        logInspector.start();

        try {
            Throwable throwable = assertThrowsWithCause(
                    () -> CLUSTER.startAndInit(testInfo, initialNodes(), cmgMetastoreNodes(), this::configureInitParameters),
                    IgniteInternalException.class);

            IgniteInternalException actual = (IgniteInternalException) unwrapRootCause(throwable);
            assertEquals(ADDRESS_UNRESOLVED_ERR, actual.code());
            assertEquals("No network address found", actual.getMessage());
        } finally {
            logInspector.stop();
        }
    }
}
