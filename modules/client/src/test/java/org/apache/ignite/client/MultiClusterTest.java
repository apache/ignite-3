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

package org.apache.ignite.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.UUID;
import org.apache.ignite.client.IgniteClient.Builder;
import org.apache.ignite.client.fakes.FakeIgnite;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.util.IgniteUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests client behavior with multiple clusters
 */
public class MultiClusterTest {
    private static final UUID clusterId1 = UUID.randomUUID();

    private static final UUID clusterId2 = UUID.randomUUID();

    private TestServer server1;

    private TestServer server2;

    @BeforeEach
    void setUp() {
        server1 = new TestServer(10900, 10, 0, new FakeIgnite(), null, "s1", clusterId1);
        server2 = new TestServer(10900, 10, 0, new FakeIgnite(), null, "s2", clusterId2);
    }

    @AfterEach
    void tearDown() throws Exception {
        IgniteUtils.closeAll(server1, server2);
    }

    @Test
    public void testClientDropsConnectionOnClusterIdMismatch()
            throws Exception {
        TestLoggerFactory loggerFactory = new TestLoggerFactory("client");

        Builder builder = IgniteClient.builder()
                .addresses("127.0.0.1:" + server1.port(), "127.0.0.1:" + server2.port())
                .loggerFactory(loggerFactory);

        try (var client = builder.build()) {
            assertTrue(IgniteTestUtils.waitForCondition(() -> getFailedConnectionEntry(loggerFactory) != null, 3000));

            assertEquals(1, client.connections().size());

            String err = getFailedConnectionEntry(loggerFactory);
        }
    }

    private static @Nullable String getFailedConnectionEntry(TestLoggerFactory loggerFactory) {
        return loggerFactory.logger.entries().stream()
                .filter(x -> x.contains("Failed to establish connection to"))
                .findFirst()
                .orElse(null);
    }
}
