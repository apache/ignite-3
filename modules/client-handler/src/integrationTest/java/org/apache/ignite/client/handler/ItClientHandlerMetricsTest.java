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

package org.apache.ignite.client.handler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.net.SocketException;
import java.nio.file.Path;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Client handler metrics tests.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class ItClientHandlerMetricsTest {
    private TestServer testServer;

    @WorkDirectory
    private Path workDir;

    @AfterEach
    void tearDown() throws Exception {
        if (testServer != null) {
            testServer.tearDown();
        }
    }

    @Test
    void sessionsRejectedTls(TestInfo testInfo) throws Exception {
        testServer = new TestServer(
                TestSslConfig.builder()
                        .keyStorePath(ItClientHandlerTestUtils.generateKeystore(workDir))
                        .keyStorePassword("changeit")
                        .build()
        );

        var serverModule = testServer.start(testInfo);

        assertThrows(SocketException.class, () -> ItClientHandlerTestUtils.connectAndHandshake(serverModule));

        assertEquals(1, testServer.metrics().sessionsRejectedTls().value());
        assertEquals(0, testServer.metrics().sessionsRejected().value());
        assertEquals(0, testServer.metrics().sessionsAccepted().value());
    }

    @Test
    void sessionsRejected(TestInfo testInfo) throws Exception {
        testServer = new TestServer(null);
        var serverModule = testServer.start(testInfo);

        // Bad MAGIC.
        assertThrows(SocketException.class, () -> ItClientHandlerTestUtils.connectAndHandshake(serverModule, true, false));
        assertEquals(1, testServer.metrics().sessionsRejected().value());

        // Bad version.
        ItClientHandlerTestUtils.connectAndHandshake(serverModule, false, true);
        assertEquals(2, testServer.metrics().sessionsRejected().value());

        assertEquals(0, testServer.metrics().sessionsRejectedTls().value());
        assertEquals(0, testServer.metrics().sessionsAccepted().value());
        assertEquals(0, testServer.metrics().sessionsActive().value());
    }

    @Test
    void sessionsAccepted(TestInfo testInfo) throws Exception {
        testServer = new TestServer(null);
        var serverModule = testServer.start(testInfo);

        ItClientHandlerTestUtils.connectAndHandshake(serverModule);
        assertEquals(1, testServer.metrics().sessionsAccepted().value());
    }
}
