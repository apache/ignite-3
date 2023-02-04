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

package org.apache.ignite.internal.rest;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
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
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

/** Tests for the REST SSL configuration. */
@ExtendWith(WorkDirectoryExtension.class)
public class ItRestSslTest {

    /** Network port of the test node. */
    private static final int nodePort = 3344;

    /** HTTP host and port url part. */
    private static final String HTTP_HOST_PORT = "localhost:10300";

    /** HTTP address. */
    private static final String HTTP_ADDRESS = "http://" + HTTP_HOST_PORT;

    /** HTTPS address. */
    private static final String HTTPS_ADDRESS = "https://" + HTTP_HOST_PORT;

    /** Key store path. */
    private static final String keyStorePath = "ssl/keystore.p12";

    /** Key store password. */
    private static final String keyStorePassword = "changeIt";

    /** Trust store path. */
    private static final String trustStorePath = "ssl/truststore.jks";

    /** Trust store password. */
    private static final String trustStorePassword = "changeIt";

    /** Name of the test node. */
    private static String nodeName;

    /** Path to the working directory. */
    @WorkDirectory
    private static Path workDir;

    /** HTTP client that is expected to be defined in subclasses. */
    private static HttpClient client;

    /** SSL HTTP client that is expected to be defined in subclasses. */
    private static HttpClient sslСlient;

    @BeforeAll
    static void beforeAll(TestInfo testInfo)
            throws CertificateException, KeyStoreException, IOException, NoSuchAlgorithmException, KeyManagementException {
        nodeName = testNodeName(testInfo, nodePort);
        String keyStoreAbsolutPath = ItRestSslTest.class.getClassLoader().getResource(keyStorePath).getPath();
        String nodeCfg = "{\n"
                + "  network: {\n"
                + "    port: " + nodePort + ",\n"
                + "    nodeFinder: {\n"
                + "      netClusterNodes: [ \"localhost:3344\", \"localhost:3345\", \"localhost:3346\" ]\n"
                + "    }\n"
                + "  },\n"
                + "  rest: {\n"
                + "    ssl: {\n"
                + "      enabled: true,\n"
                + "        keyStore: {\n"
                + "          type: \"PKCS12\",\n"
                + "          path: " + keyStoreAbsolutPath + ",\n"
                + "          password: " + keyStorePassword + "\n"
                + "      }\n"
                + "    }\n"
                + "  }"
                + "}";

        IgnitionManager.start(nodeName, nodeCfg, workDir.resolve(nodeName));

        client = HttpClient.newBuilder()
                .build();

        sslСlient = HttpClient.newBuilder()
                .sslContext(sslContext())
                .build();
    }

    @Test
    @DisplayName("Connect to node with enabled SSL via SSL client")
    void connectToSslNodeViaSslClient() throws IOException, InterruptedException {
        // When GET /management/v1/configuration/node
        URI uri = URI.create(HTTPS_ADDRESS + "/management/v1/configuration/node");
        HttpRequest request = HttpRequest.newBuilder(uri).build();

        // When GET /management/v1/configuration/node
        HttpResponse<String> response = sslСlient.send(request, BodyHandlers.ofString());

        // Expect node configuration can be parsed to hocon format
        Config config = ConfigFactory.parseString(response.body());
        // And has rest.port config value
        assertThat(config.getInt("rest.port"), is(equalTo(10300)));
    }

    @Test
    @DisplayName("Get an error when trying to connect to node with enabled SSL via not SSL client")
    void connectToSslNodeViaNotSslClient() {
        // When GET /management/v1/configuration/node
        URI uri = URI.create(HTTPS_ADDRESS + "/management/v1/configuration/node");
        HttpRequest request = HttpRequest.newBuilder(uri).build();

        // Expect IOException
        assertThrows(IOException.class, () -> client.send(request, BodyHandlers.ofString()));
    }

    @Test
    @DisplayName("Get an error when trying to connect to node with enabled SSL using HTTP protocol")
    void connectToHttp() {
        // When GET /management/v1/configuration/node
        URI uri = URI.create(HTTP_ADDRESS + "/management/v1/configuration/node");
        HttpRequest request = HttpRequest.newBuilder(uri).build();

        // Expect IOException
        assertThrows(IOException.class, () -> client.send(request, BodyHandlers.ofString()));
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

    @AfterAll
    static void afterAll() {
        IgnitionManager.stop(nodeName);
    }
}
