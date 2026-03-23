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

package org.apache.ignite.internal.configuration;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import org.apache.ignite.IgniteServer;
import org.apache.ignite.InitParameters;
import org.apache.ignite.configuration.validation.ConfigurationValidationException;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.TestIgnitionManager;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.tostring.IgniteToStringBuilder;
import org.apache.ignite.internal.tostring.SensitiveDataLoggingPolicy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Integration test for checking Cluster configuration.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class ItClusterConfigurationTest extends BaseIgniteAbstractTest {
    private IgniteServer node;

    @BeforeEach
    @AfterEach
    public void resetLoggingPolicy() {
        IgniteToStringBuilder.setSensitiveDataPolicy(SensitiveDataLoggingPolicy.HASH);
    }

    @AfterEach
    void tearDown() {
        if (node != null) {
            node.shutdown();
        }
    }

    @Test
    void testValidationFailsWithDuplicates(TestInfo testInfo, @WorkDirectory Path workDir) {
        node = TestIgnitionManager.start(testNodeName(testInfo, 0), null, workDir);

        var parameters = InitParameters.builder()
                .metaStorageNodes(node)
                .clusterName("cluster")
                .clusterConfiguration("ignite { system { idleSafeTimeSyncIntervalMillis: 50, idleSafeTimeSyncIntervalMillis: 100 }}")
                .build();

        assertThrowsWithCause(
                () -> TestIgnitionManager.init(node, parameters),
                ConfigurationValidationException.class,
                "Validation did not pass for keys: [ignite.system.idleSafeTimeSyncIntervalMillis, Duplicated key]");
    }

    @Test
    void testSensitiveDataLogging(TestInfo testInfo, @WorkDirectory Path workDir) throws InterruptedException {
        node = TestIgnitionManager.start(testNodeName(testInfo, 0), null, workDir);

        var parameters = InitParameters.builder()
                .metaStorageNodes(node)
                .clusterName("cluster")
                .clusterConfiguration("ignite { system { properties { sensitiveDataLogging: plain } } }")
                .build();

        TestIgnitionManager.init(node, parameters);

        assertTrue(waitForCondition(() -> IgniteToStringBuilder.getSensitiveDataLogging() == SensitiveDataLoggingPolicy.PLAIN, 10_000));
    }
}
