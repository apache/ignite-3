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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.msgpack.core.MessagePack;

/** SSL client integration test. */
public class ItSslClientHandlerTest {

    /** Magic bytes. */
    private static final byte[] MAGIC = {0x49, 0x47, 0x4E, 0x49};

    ClientHandlerModule serverModule;

    TestServer testServer;

    int serverPort;

    String password;

    String keyStorePkcs12Path;

    @BeforeEach
    void setUp() {
        password = "changeit";
        keyStorePkcs12Path = ItSslClientHandlerTest.class.getClassLoader().getResource("ssl/keystore.pfx").getPath();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (testServer != null) {
            testServer.tearDown();
        }
    }

    @Test
    @DisplayName("When ssl not configured (by default) the client can connect")
    void sslNotConfigured(TestInfo testInfo) throws IOException {
        // Given server started
        testServer = new TestServer();
        serverModule = testServer.start(testInfo);

        // Then
        performAndCheckMagic();
    }

    @Test
    @DisplayName("When ssl configured on the server but not configured on the client then exception is thrown")
    void sslConfiguredOnlyForServer(TestInfo testInfo) {
        // Given server started
        testServer = new TestServer(
                TestSslConfig.builder()
                        .keyStorePath(keyStorePkcs12Path)
                        .keyStorePassword(password)
                        .build()
        );
        serverModule = testServer.start(testInfo);

        // Then
        assertThrows(SocketException.class, this::performAndCheckMagic);
    }

    private void performAndCheckMagic() throws IOException {
        serverPort = serverModule.localAddress().getPort();

        try (var sock = new Socket("127.0.0.1", serverPort)) {
            OutputStream out = sock.getOutputStream();

            // Magic: IGNI
            out.write(MAGIC);

            // Handshake.
            var packer = MessagePack.newDefaultBufferPacker();
            packer.packInt(0);
            packer.packInt(0);
            packer.packInt(0);
            packer.packInt(7); // Size.

            packer.packInt(3); // Major.
            packer.packInt(0); // Minor.
            packer.packInt(0); // Patch.

            packer.packInt(2); // Client type: general purpose.

            packer.packBinaryHeader(0); // Features.
            packer.packMapHeader(0); // Extensions.

            out.write(packer.toByteArray());
            out.flush();

            // Read response.
            var unpacker = MessagePack.newDefaultUnpacker(sock.getInputStream());
            var magic = unpacker.readPayload(4);

            assertArrayEquals(MAGIC, magic);
        }
    }
}
