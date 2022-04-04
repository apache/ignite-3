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

import org.apache.ignite.client.fakes.FakeIgnite;
import org.apache.ignite.internal.util.IgniteUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Tests thin client retry behavior.
 */
public class RetryPolicyTest {
    /** Test server. */
    TestServer server;

    /** Test server 2. */
    TestServer server2;

    @AfterEach
    void tearDown() throws Exception {
        IgniteUtils.closeAll(server, server2);
    }

    @Test
    public void testNoRetryPolicySecondRequestFails() {
        FakeIgnite ign = new FakeIgnite();
        ign.tables().createTable("t", c -> c.changeName("t"));

        server = new TestServer(10900, 10, 0, ign, true);

        var client = IgniteClient.builder()
                .addresses("127.0.0.1:10900..10910")
                .build();

        assertEquals("t", client.tables().tables().get(0).name());
        assertThrows(IgniteClientException.class, () -> client.tables().tables().get(0).name());
    }
}
