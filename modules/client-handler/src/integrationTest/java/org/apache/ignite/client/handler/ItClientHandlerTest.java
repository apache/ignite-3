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

package org.apache.ignite.client.handler;

import static org.apache.ignite.configuration.annotation.ConfigurationType.LOCAL;
import static org.apache.ignite.lang.ErrorGroups.Client.PROTOCOL_COMPATIBILITY_ERR;
import static org.apache.ignite.lang.ErrorGroups.Common.UNKNOWN_ERR;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.List;
import java.util.Map;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.configuration.schemas.clientconnector.ClientConnectorConfiguration;
import org.apache.ignite.configuration.schemas.network.NetworkConfiguration;
import org.apache.ignite.internal.configuration.ConfigurationManager;
import org.apache.ignite.internal.configuration.storage.TestConfigurationStorage;
import org.apache.ignite.internal.sql.engine.QueryProcessor;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NettyBootstrapFactory;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.table.manager.IgniteTables;
import org.apache.ignite.tx.IgniteTransactions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.mockito.Mockito;
import org.msgpack.core.MessagePack;

/**
 * Client connector integration tests with real sockets.
 */
public class ItClientHandlerTest {
    /** Magic bytes. */
    private static final byte[] MAGIC = new byte[]{0x49, 0x47, 0x4E, 0x49};

    private ClientHandlerModule serverModule;

    private ConfigurationManager configurationManager;

    private NettyBootstrapFactory bootstrapFactory;

    private int serverPort;

    @BeforeEach
    public void setUp(TestInfo testInfo) {
        serverModule = startServer(testInfo);
        serverPort = serverModule.localAddress().getPort();
    }

    @AfterEach
    public void tearDown() throws Exception {
        serverModule.stop();
        configurationManager.stop();
        bootstrapFactory.stop();
    }

    @Test
    void testHandshakeInvalidMagicHeaderDropsConnection() throws Exception {
        try (var sock = new Socket("127.0.0.1", serverPort)) {
            OutputStream out = sock.getOutputStream();
            out.write(new byte[]{63, 64, 65, 66, 67});
            out.flush();

            assertThrows(IOException.class, () -> writeAndFlushLoop(sock));
        }
    }

    @Test
    void testHandshakeValidReturnsSuccess() throws Exception {
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
            final var magic = unpacker.readPayload(4);
            unpacker.skipValue(3); // LE int zeros.
            final var len = unpacker.unpackInt();
            final var major = unpacker.unpackInt();
            final var minor = unpacker.unpackInt();
            final var patch = unpacker.unpackInt();
            final var success = unpacker.tryUnpackNil();
            assertTrue(success);

            final var idleTimeout = unpacker.unpackLong();
            final var nodeId = unpacker.unpackString();
            final var nodeName = unpacker.unpackString();

            var featuresLen = unpacker.unpackBinaryHeader();
            unpacker.skipValue(featuresLen);

            var extensionsLen = unpacker.unpackMapHeader();
            unpacker.skipValue(extensionsLen);

            assertArrayEquals(MAGIC, magic);
            assertEquals(26, len);
            assertEquals(3, major);
            assertEquals(0, minor);
            assertEquals(0, patch);
            assertEquals(0, idleTimeout);
            assertEquals("id", nodeId);
            assertEquals("consistent-id", nodeName);
        }
    }

    @Test
    void testHandshakeInvalidVersionReturnsError() throws Exception {
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

            packer.packInt(2); // Major.
            packer.packInt(8); // Minor.
            packer.packInt(0); // Patch.

            packer.packInt(2); // Client type: general purpose.

            packer.packBinaryHeader(0); // Features.
            packer.packMapHeader(0); // Extensions.

            out.write(packer.toByteArray());
            out.flush();

            // Read response.
            var unpacker = MessagePack.newDefaultUnpacker(sock.getInputStream());
            var magic = unpacker.readPayload(4);
            unpacker.readPayload(4); // Length.
            final var major = unpacker.unpackInt();
            final var minor = unpacker.unpackInt();
            final var patch = unpacker.unpackInt();

            unpacker.skipValue(); // traceId
            final var code = unpacker.tryUnpackNil() ? UNKNOWN_ERR : unpacker.unpackInt();
            final var errClassName = unpacker.unpackString();
            final var errMsg = unpacker.tryUnpackNil() ? null : unpacker.unpackString();
            final var errStackTrace = unpacker.tryUnpackNil() ? null : unpacker.unpackString();

            assertArrayEquals(MAGIC, magic);
            assertEquals(3, major);
            assertEquals(0, minor);
            assertEquals(0, patch);
            assertEquals(PROTOCOL_COMPATIBILITY_ERR, code);

            assertThat(errMsg, containsString("Unsupported version: 2.8.0"));
            assertEquals("org.apache.ignite.lang.IgniteException", errClassName);
            assertNull(errStackTrace);
        }
    }

    private ClientHandlerModule startServer(TestInfo testInfo) {
        configurationManager = new ConfigurationManager(
                List.of(ClientConnectorConfiguration.KEY, NetworkConfiguration.KEY),
                Map.of(),
                new TestConfigurationStorage(LOCAL),
                List.of(),
                List.of()
        );

        configurationManager.start();

        var registry = configurationManager.configurationRegistry();

        registry.getConfiguration(ClientConnectorConfiguration.KEY).change(
                local -> local.changePort(10800).changePortRange(10)
        ).join();

        bootstrapFactory = new NettyBootstrapFactory(registry.getConfiguration(NetworkConfiguration.KEY), testInfo.getDisplayName());

        bootstrapFactory.start();

        ClusterService clusterService = mock(ClusterService.class, RETURNS_DEEP_STUBS);
        Mockito.when(clusterService.topologyService().localMember().id()).thenReturn("id");
        Mockito.when(clusterService.topologyService().localMember().name()).thenReturn("consistent-id");

        var module = new ClientHandlerModule(mock(QueryProcessor.class), mock(IgniteTables.class), mock(IgniteTransactions.class), registry,
                mock(IgniteCompute.class), clusterService, bootstrapFactory, mock(IgniteSql.class));

        module.start();

        return module;
    }

    private void writeAndFlushLoop(Socket socket) throws Exception {
        var stop = System.currentTimeMillis() + 5000;
        var out = socket.getOutputStream();

        while (System.currentTimeMillis() < stop) {
            out.write(1);
            out.flush();
        }
    }
}
