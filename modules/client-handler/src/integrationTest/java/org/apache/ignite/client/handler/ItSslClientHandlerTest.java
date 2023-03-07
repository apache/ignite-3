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

import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.netty.handler.ssl.util.SelfSignedCertificate;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.SocketException;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.lang.IgniteException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

/** SSL client integration test. */
@ExtendWith(WorkDirectoryExtension.class)
public class ItSslClientHandlerTest {
    private ClientHandlerModule serverModule;

    private TestServer testServer;

    private String keyStorePkcs12Path;

    @WorkDirectory
    private Path workDir;

    @BeforeEach
    void setUp() throws Exception {
        keyStorePkcs12Path = workDir.resolve("keystore.p12").toAbsolutePath().toString();
        generateKeystore(new SelfSignedCertificate("localhost"));
    }

    private void generateKeystore(SelfSignedCertificate cert)
            throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException {
        KeyStore ks = KeyStore.getInstance("PKCS12");
        ks.load(null, null);
        ks.setKeyEntry("key", cert.key(), "changeit".toCharArray(), new Certificate[]{cert.cert()});
        try (FileOutputStream fos = new FileOutputStream(keyStorePkcs12Path)) {
            ks.store(fos, "changeit".toCharArray());
        }
    }

    @AfterEach
    void tearDown() throws Exception {
        if (testServer != null) {
            testServer.tearDown();
        }
    }

    @Test
    @DisplayName("When SSL not configured (by default) the client can connect")
    void sslNotConfigured(TestInfo testInfo) throws IOException {
        // Given server started
        testServer = new TestServer();
        serverModule = testServer.start(testInfo);

        // Then
        performAndCheckMagic();
    }

    @Test
    @DisplayName("When SSL configured on the server but not configured on the client then exception is thrown")
    void sslConfiguredOnlyForServer(TestInfo testInfo) {
        // Given server started
        testServer = new TestServer(
                TestSslConfig.builder()
                        .keyStorePath(keyStorePkcs12Path)
                        .keyStorePassword("changeit")
                        .build()
        );
        serverModule = testServer.start(testInfo);

        // Then
        assertThrows(SocketException.class, this::performAndCheckMagic);

        // TODO: Separate test class for metrics.
        assertEquals(1, testServer.metrics().sessionsRejectedTls().value());
        assertEquals(0, testServer.metrics().sessionsRejected().value());
        assertEquals(0, testServer.metrics().sessionsAccepted().value());
    }

    @Test
    @DisplayName("When SSL is configured incorrectly then exception is thrown on start")
    @SuppressWarnings("ThrowableNotThrown")
    void sslMisconfigured(TestInfo testInfo) {
        testServer = new TestServer(
                TestSslConfig.builder()
                        .keyStorePath(keyStorePkcs12Path)
                        .keyStorePassword("wrong-password")
                        .build()
        );

        assertThrowsWithCause(() -> testServer.start(testInfo), IgniteException.class, "keystore password was incorrect");
    }

    private void performAndCheckMagic() throws IOException {
        ItClientHandlerTestUtils.connectAndHandshake(serverModule);
    }
}
