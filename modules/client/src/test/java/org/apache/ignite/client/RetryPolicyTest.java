/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.function.Function;
import org.apache.ignite.client.fakes.FakeIgnite;
import org.apache.ignite.internal.util.IgniteUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Tests thin client retry behavior.
 */
public class RetryPolicyTest {
    private static final int ITER = 100;

    /** Test server. */
    private TestServer server;

    @AfterEach
    void tearDown() throws Exception {
        IgniteUtils.closeAll(server);
    }

    @Test
    public void testNoRetryPolicySecondRequestFails() throws Exception {
        initServer(reqId -> reqId % 3 == 0);

        try (var client = getClient(null)) {
            assertEquals("t", client.tables().tables().get(0).name());
            assertThrows(IgniteClientException.class, () -> client.tables().tables().get(0).name());
        }
    }

    @Test
    public void testRetryPolicyCompletesOperationWithoutException() throws Exception {
        initServer(reqId -> reqId % 3 == 0);

        try (var client = getClient(new RetryLimitPolicy().retryLimit(20))) {
             for (int i = 0; i < ITER; i++) {
                assertEquals("t", client.tables().tables().get(0).name());
             }
        }
    }

    private IgniteClient getClient(RetryPolicy retryPolicy) {
        return IgniteClient.builder()
                .addresses("127.0.0.1:" + server.port())
                .retryPolicy(retryPolicy)
                .reconnectThrottlingPeriod(0)
                .build();
    }

    private void initServer(Function<Integer, Boolean> integerBooleanFunction) {
        FakeIgnite ign = new FakeIgnite();
        ign.tables().createTable("t", c -> c.changeName("t"));

        server = new TestServer(10900, 10, 0, ign, integerBooleanFunction);
    }
}
