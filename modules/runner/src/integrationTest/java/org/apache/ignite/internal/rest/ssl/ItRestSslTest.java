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

package org.apache.ignite.internal.rest.ssl;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.file.Path;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.util.stream.Stream;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

/** Tests for the REST SSL configuration. */
@ExtendWith(WorkDirectoryExtension.class)
public class ItRestSslTest {

    /** HTTP port of the test node. */
    private static final int httpPort = 10300;

    /** HTTPS port of the test node. */
    private static final int httpsPort = 10400;

    /** Trust store path. */
    private static final String trustStorePath = "ssl/truststore.jks";

    /** Trust store password. */
    private static final String trustStorePassword = "changeIt";

    /** Path to the working directory. */
    @WorkDirectory
    private static Path workDir;

    /** HTTP client that is expected to be defined in subclasses. */
    private static HttpClient client;

    /** SSL HTTP client that is expected to be defined in subclasses. */
    private static HttpClient sslClient;

    private static RestNode httpNode;

    private static RestNode httpsNode;

    private static RestNode dualProtocolNode;

    @BeforeAll
    static void beforeAll(TestInfo testInfo) throws Exception {

        client = HttpClient.newBuilder()
                .build();

        sslClient = HttpClient.newBuilder()
                .sslContext(sslContext())
                .build();

        httpNode = new RestNode(workDir, testNodeName(testInfo, 3344), 3344, 10300, 10400, false, false);
        httpsNode = new RestNode(workDir, testNodeName(testInfo, 3345), 3345, 10301, 10401, true, false);
        dualProtocolNode = new RestNode(workDir, testNodeName(testInfo, 3346), 3346, 10302, 10402, true, true);
        Stream.of(httpNode, httpsNode, dualProtocolNode)
                .forEach(RestNode::start);
    }

    @Test
    void httpsProtocol() throws IOException, InterruptedException {
        // When GET /management/v1/configuration/node
        URI uri = URI.create(httpsNode.httpsAddress() + "/management/v1/configuration/node");
        HttpRequest request = HttpRequest.newBuilder(uri).build();

        // Then response code is 200
        HttpResponse<String> response = sslClient.send(request, BodyHandlers.ofString());
        assertEquals(200, response.statusCode());
    }

    @Test
    void httpProtocol(TestInfo testInfo) throws IOException, InterruptedException {
        // When GET /management/v1/configuration/node
        URI uri = URI.create(httpNode.httpAddress() + "/management/v1/configuration/node");
        HttpRequest request = HttpRequest.newBuilder(uri).build();

        // Then response code is 200
        HttpResponse<String> response = client.send(request, BodyHandlers.ofString());
        assertEquals(200, response.statusCode());
    }

    @Test
    void dualProtocol() throws IOException, InterruptedException {
        // When GET /management/v1/configuration/node
        URI httpUri = URI.create(dualProtocolNode.httpAddress() + "/management/v1/configuration/node");
        HttpRequest httpRequest = HttpRequest.newBuilder(httpUri).build();

        URI httpsUri = URI.create(dualProtocolNode.httpsAddress() + "/management/v1/configuration/node");
        HttpRequest httpsRequest = HttpRequest.newBuilder(httpsUri).build();

        // Then HTTP response code is 200
        HttpResponse<String> httpResponse = client.send(httpRequest, BodyHandlers.ofString());
        assertEquals(200, httpResponse.statusCode());

        // And HTTPS response code is 200
        httpResponse = sslClient.send(httpsRequest, BodyHandlers.ofString());
        assertEquals(200, httpResponse.statusCode());
    }

    @Test
    void httpsProtocolNotSslClient() {
        // When GET /management/v1/configuration/node
        URI uri = URI.create(httpsNode.httpsAddress() + "/management/v1/configuration/node");
        HttpRequest request = HttpRequest.newBuilder(uri).build();

        // Then IOException
        assertThrows(IOException.class, () -> client.send(request, BodyHandlers.ofString()));

    }

    @Test
    void httpProtocolNotSslClient() throws IOException, InterruptedException {
        // When GET /management/v1/configuration/node
        URI uri = URI.create(httpsNode.httpAddress() + "/management/v1/configuration/node");
        HttpRequest request = HttpRequest.newBuilder(uri).build();

        // Expect IOException
        assertThrows(IOException.class, () -> client.send(request, BodyHandlers.ofString()));
    }

    @AfterAll
    static void afterAll() {
        Stream.of(httpNode, httpsNode, dualProtocolNode)
                .forEach(RestNode::stop);
    }

    private static SSLContext sslContext()
            throws CertificateException, KeyStoreException, IOException, NoSuchAlgorithmException, KeyManagementException {
        String path = ItRestSslTest.class.getClassLoader().getResource(trustStorePath).getPath();
        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        KeyStore keyStore = KeyStore.getInstance(new File(path), trustStorePassword.toCharArray());
        trustManagerFactory.init(keyStore);
        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(null, trustManagerFactory.getTrustManagers(), new SecureRandom());
        return sslContext;
    }
}
