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

import static org.apache.ignite.client.handler.ItClientHandlerTestUtils.MAGIC;
import static org.apache.ignite.configuration.annotation.ConfigurationType.DISTRIBUTED;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.lang.ErrorGroups.Authentication.INVALID_CREDENTIALS_ERR;
import static org.apache.ignite.lang.ErrorGroups.Authentication.UNSUPPORTED_AUTHENTICATION_TYPE_ERR;
import static org.apache.ignite.lang.ErrorGroups.Client.PROTOCOL_COMPATIBILITY_ERR;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.BitSet;
import org.apache.ignite.client.handler.configuration.ClientConnectorConfiguration;
import org.apache.ignite.internal.configuration.ClusterConfiguration;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.network.configuration.NetworkConfiguration;
import org.apache.ignite.internal.security.authentication.basic.BasicAuthenticationProviderChange;
import org.apache.ignite.internal.security.configuration.SecurityConfiguration;
import org.apache.ignite.internal.security.configuration.SecurityExtensionConfiguration;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.msgpack.core.MessagePack;

/**
 * Client connector integration tests with real sockets.
 */
@ExtendWith(ConfigurationExtension.class)
public class ItClientHandlerTest extends BaseIgniteAbstractTest {
    private ClientHandlerModule serverModule;

    private TestServer testServer;

    private int serverPort;

    @InjectConfiguration(rootName = "ignite", type = DISTRIBUTED)
    private ClusterConfiguration clusterConfiguration;

    private SecurityConfiguration securityConfiguration;

    @InjectConfiguration
    private ClientConnectorConfiguration clientConnectorConfiguration;

    @InjectConfiguration
    private NetworkConfiguration networkConfiguration;

    @BeforeEach
    public void setUp(TestInfo testInfo) {
        securityConfiguration = ((SecurityExtensionConfiguration) clusterConfiguration).security();

        testServer = new TestServer(null, securityConfiguration, clientConnectorConfiguration, networkConfiguration);
        serverModule = testServer.start(testInfo);
        serverPort = serverModule.localAddress().getPort();
    }

    @AfterEach
    public void tearDown() {
        assertThat(serverModule.stopAsync(new ComponentContext()), willCompleteSuccessfully());
        testServer.tearDown();
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
            packer.packInt(0); // Extensions.

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
            unpacker.skipValue(); // Node id.
            final var nodeName = unpacker.unpackString();
            unpacker.skipValue(2); // Cluster ids.
            unpacker.skipValue(); // Cluster name.
            unpacker.skipValue(); // Observable timestamp.

            unpacker.skipValue(); // Major.
            unpacker.skipValue(); // Minor.
            unpacker.skipValue(); // Maintenance.
            unpacker.skipValue(); // Patch.
            unpacker.skipValue(); // Pre release.

            var featuresLen = unpacker.unpackBinaryHeader();
            unpacker.skipValue(featuresLen);

            var extensionsLen = unpacker.unpackInt();
            unpacker.skipValue(extensionsLen);

            assertArrayEquals(MAGIC, magic);
            assertEquals(100, len);
            assertEquals(3, major);
            assertEquals(0, minor);
            assertEquals(0, patch);
            assertEquals(5000, idleTimeout);
            assertEquals("consistent-id", nodeName);
        }
    }

    @Test
    void testHandshakeWithUnsupportedAuthenticationType() throws Exception {
        setupAuthentication("admin", "password");

        try (var sock = new Socket("127.0.0.1", serverPort)) {
            OutputStream out = sock.getOutputStream();

            // Magic: IGNI
            out.write(MAGIC);

            // Handshake.
            var packer = MessagePack.newDefaultBufferPacker();
            packer.packInt(0);
            packer.packInt(0);
            packer.packInt(0);
            packer.packInt(66); // Size.

            packer.packInt(3); // Major.
            packer.packInt(0); // Minor.
            packer.packInt(0); // Patch.

            packer.packInt(2); // Client type: general purpose.

            packer.packBinaryHeader(0); // Features.
            packer.packInt(3); // Extensions.
            packer.packString("authn-type");
            packer.packString("ldap");
            packer.packString("authn-identity");
            packer.packString("admin");
            packer.packString("authn-secret");
            packer.packString("password");

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
            final var code = unpacker.tryUnpackNil() ? INTERNAL_ERR : unpacker.unpackInt();
            final var errClassName = unpacker.unpackString();
            final var errMsg = unpacker.tryUnpackNil() ? null : unpacker.unpackString();
            final var errStackTrace = unpacker.tryUnpackNil() ? null : unpacker.unpackString();

            assertArrayEquals(MAGIC, magic);
            assertEquals(3, major);
            assertEquals(0, minor);
            assertEquals(0, patch);
            assertEquals(UNSUPPORTED_AUTHENTICATION_TYPE_ERR, code);

            assertThat(errMsg, containsString("Unsupported authentication type: ldap"));
            assertEquals(
                    "org.apache.ignite.security.exception.UnsupportedAuthenticationTypeException",
                    errClassName
            );

            assertEquals(
                    "To see the full stack trace, set clientConnector.sendServerExceptionStackTraceToClient:true on the server",
                    errStackTrace);
        }
    }

    @Test
    void testHandshakeWithAuthenticationValidCredentials() throws Exception {
        setupAuthentication("admin", "password");

        try (var sock = new Socket("127.0.0.1", serverPort)) {
            OutputStream out = sock.getOutputStream();

            // Magic: IGNI
            out.write(MAGIC);

            // Handshake.
            var packer = MessagePack.newDefaultBufferPacker();
            packer.packInt(0);
            packer.packInt(0);
            packer.packInt(0);
            packer.packInt(67); // Size.

            packer.packInt(3); // Major.
            packer.packInt(0); // Minor.
            packer.packInt(0); // Patch.

            packer.packInt(2); // Client type: general purpose.

            packer.packBinaryHeader(0); // Features.
            packer.packInt(3); // Extensions.
            packer.packString("authn-type");
            packer.packString("basic");
            packer.packString("authn-identity");
            packer.packString("admin");
            packer.packString("authn-secret");
            packer.packString("password");

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

            var idleTimeout = unpacker.unpackLong();
            var nodeIdHeader = unpacker.unpackExtensionTypeHeader();
            var nodeId = unpacker.readPayload(nodeIdHeader.getLength());
            var nodeName = unpacker.unpackString();

            unpacker.skipValue(2); // Cluster ids.
            var clusterName = unpacker.unpackString();

            assertArrayEquals(MAGIC, magic);
            assertEquals(3, major);
            assertEquals(0, minor);
            assertEquals(0, patch);
            assertEquals(5000, idleTimeout);
            assertEquals(16, nodeId.length);
            assertEquals("consistent-id", nodeName);
            assertEquals("Test Server", clusterName);
        }
    }

    @Test
    void testHandshakeWithAuthenticationInvalidCredentials() throws Exception {
        setupAuthentication("admin", "password");

        try (var sock = new Socket("127.0.0.1", serverPort)) {
            OutputStream out = sock.getOutputStream();

            // Magic: IGNI
            out.write(MAGIC);

            // Handshake.
            var packer = MessagePack.newDefaultBufferPacker();
            packer.packInt(0);
            packer.packInt(0);
            packer.packInt(0);
            packer.packInt(75); // Size.

            packer.packInt(3); // Major.
            packer.packInt(0); // Minor.
            packer.packInt(0); // Patch.

            packer.packInt(2); // Client type: general purpose.

            packer.packBinaryHeader(0); // Features.
            packer.packInt(3); // Extensions.
            packer.packString("authn-type");
            packer.packString("basic");
            packer.packString("authn-identity");
            packer.packString("admin");
            packer.packString("authn-secret");
            packer.packString("invalid-password");

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
            final var code = unpacker.tryUnpackNil() ? INTERNAL_ERR : unpacker.unpackInt();
            final var errClassName = unpacker.unpackString();
            final var errMsg = unpacker.tryUnpackNil() ? null : unpacker.unpackString();
            final var errStackTrace = unpacker.tryUnpackNil() ? null : unpacker.unpackString();

            assertArrayEquals(MAGIC, magic);
            assertEquals(3, major);
            assertEquals(0, minor);
            assertEquals(0, patch);
            assertEquals(INVALID_CREDENTIALS_ERR, code);

            assertThat(errMsg, containsString("Authentication failed"));
            assertEquals("org.apache.ignite.security.exception.InvalidCredentialsException", errClassName);
            assertEquals(
                    "To see the full stack trace, set clientConnector.sendServerExceptionStackTraceToClient:true on the server",
                    errStackTrace);
        }
    }

    @Test
    void testHandshakeWithAuthenticationEmptyCredentials() throws Exception {
        setupAuthentication("admin", "password");

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
            packer.packInt(0); // Extensions.

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
            final var code = unpacker.tryUnpackNil() ? INTERNAL_ERR : unpacker.unpackInt();
            final var errClassName = unpacker.unpackString();
            final var errMsg = unpacker.tryUnpackNil() ? null : unpacker.unpackString();
            final var errStackTrace = unpacker.tryUnpackNil() ? null : unpacker.unpackString();

            assertArrayEquals(MAGIC, magic);
            assertEquals(3, major);
            assertEquals(0, minor);
            assertEquals(0, patch);
            assertEquals(INVALID_CREDENTIALS_ERR, code);

            assertThat(errMsg, containsString("Authentication failed"));
            assertEquals("org.apache.ignite.security.exception.InvalidCredentialsException", errClassName);

            assertEquals(
                    "To see the full stack trace, set clientConnector.sendServerExceptionStackTraceToClient:true on the server",
                    errStackTrace);
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
            packer.packInt(0); // Extensions.

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
            final var code = unpacker.tryUnpackNil() ? INTERNAL_ERR : unpacker.unpackInt();
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

            assertEquals(
                    "To see the full stack trace, set clientConnector.sendServerExceptionStackTraceToClient:true on the server",
                    errStackTrace);
        }
    }

    @Test
    public void testServerReturnsAllItsFeatures() throws IOException {
        try (var sock = new Socket("127.0.0.1", serverPort)) {
            OutputStream out = sock.getOutputStream();

            // Magic: IGNI
            out.write(MAGIC);

            // Handshake.
            var packer = MessagePack.newDefaultBufferPacker();
            packer.packInt(0);
            packer.packInt(0);
            packer.packInt(0);
            packer.packInt(9); // Size.

            packer.packInt(3); // Major.
            packer.packInt(0); // Minor.
            packer.packInt(0); // Patch.

            packer.packInt(2); // Client type: general purpose.

            BitSet clientFeatures = new BitSet();
            // Supported features
            clientFeatures.set(1);
            clientFeatures.set(2);
            clientFeatures.set(6);
            clientFeatures.set(7);
            clientFeatures.set(8);
            // Unsupported feature
            clientFeatures.set(4);

            packer.packBinaryHeader(clientFeatures.toByteArray().length); // Features.);
            packer.writePayload(clientFeatures.toByteArray());

            packer.packInt(0); // Extensions.

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
            unpacker.skipValue(); // Node id.
            final var nodeName = unpacker.unpackString();
            unpacker.skipValue(2); // Cluster ids.
            unpacker.skipValue(); // Cluster name.
            unpacker.skipValue(); // Observable timestamp.

            unpacker.skipValue(); // Major.
            unpacker.skipValue(); // Minor.
            unpacker.skipValue(); // Maintenance.
            unpacker.skipValue(); // Patch.
            unpacker.skipValue(); // Pre release.

            // Features
            var featuresLen = unpacker.unpackBinaryHeader();
            assertTrue(featuresLen > 0);
            byte[] featuresBits = unpacker.readPayload(featuresLen);
            BitSet supportedFeatures = BitSet.valueOf(featuresBits);

            // Server features
            BitSet expected = new BitSet();
            expected.set(1);
            expected.set(2);
            expected.set(3);
            expected.set(5);
            expected.set(6);
            expected.set(7);
            expected.set(8);
            expected.set(9);
            expected.set(10);
            expected.set(11);
            expected.set(12);
            expected.set(13);
            expected.set(14);
            expected.set(15);
            expected.set(16);
            expected.set(17);
            
            assertEquals(expected, supportedFeatures);

            var extensionsLen = unpacker.unpackInt();
            unpacker.skipValue(extensionsLen);

            assertArrayEquals(MAGIC, magic);
            assertEquals(extensionsLen + featuresLen + 97 /* rest of the fields */, len);
            assertEquals(3, major);
            assertEquals(0, minor);
            assertEquals(0, patch);
            assertEquals(5000, idleTimeout);
            assertEquals("consistent-id", nodeName);
        }
    }

    private static void writeAndFlushLoop(Socket socket) throws Exception {
        var stop = System.currentTimeMillis() + 5000;
        var out = socket.getOutputStream();

        while (System.currentTimeMillis() < stop) {
            out.write(1);
            out.flush();
        }
    }

    private void setupAuthentication(String username, String password) {
        securityConfiguration.change(change -> {
            change.changeEnabled(true);
            change.changeAuthentication().changeProviders().create("basic", authenticationProviderChange -> {
                authenticationProviderChange.convert(BasicAuthenticationProviderChange.class)
                        .changeUsers(users -> users.create(username, user -> user.changePassword(password)));
            });
        }).join();
    }
}
