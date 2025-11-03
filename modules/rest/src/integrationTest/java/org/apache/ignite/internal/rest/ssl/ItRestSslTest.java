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

import static org.apache.ignite.internal.testframework.matchers.HttpResponseMatcher.hasStatusCode;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.file.Path;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.stream.Stream;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManagerFactory;
import org.apache.ignite.internal.Cluster;
import org.apache.ignite.internal.ClusterConfiguration;
import org.apache.ignite.internal.rest.RestNode;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

/** Tests for the REST SSL configuration. */
@ExtendWith(WorkDirectoryExtension.class)
public class ItRestSslTest extends BaseIgniteAbstractTest {

    /** Trust store path. */
    private static final String trustStorePath = "ssl/truststore.jks";

    /** Trust store password. */
    private static final String trustStorePassword = "changeit";

    /** Key store path. */
    private static final String keyStorePath = "ssl/keystore.p12";

    /** Key store password. */
    private static final String keyStorePassword = "changeit";

    /** Path to the working directory. */
    @WorkDirectory
    private static Path workDir;

    /** HTTP client. */
    private static HttpClient client;

    /** SSL HTTP client. */
    private static HttpClient sslClient;

    /** SSL HTTP client with client auth. */
    private static HttpClient sslClientWithClientAuth;

    /** SSL HTTP client with custom cipher. */
    private static HttpClient sslClientWithCustomCipher;

    private static RestNode httpNode;

    private static RestNode httpsNode;

    private static RestNode dualProtocolNode;

    private static RestNode httpsWithClientAuthNode;

    private static RestNode httpsWithCustomCipherNode;

    private static Cluster cluster;

    @BeforeAll
    static void beforeAll(TestInfo testInfo) throws Exception {

        client = HttpClient.newBuilder()
                .build();

        sslClient = HttpClient.newBuilder()
                .sslContext(sslContext())
                .build();

        sslClientWithClientAuth = HttpClient.newBuilder()
                .sslContext(sslContextWithClientAuth())
                .build();

        SSLContext sslContext = sslContext();
        SSLParameters sslParameters = sslContext.getDefaultSSLParameters();
        sslParameters.setCipherSuites(new String[]{"TLS_DHE_RSA_WITH_AES_128_CBC_SHA"});
        sslClientWithCustomCipher = HttpClient.newBuilder()
                .sslContext(sslContext)
                .sslParameters(sslParameters)
                .build();

        cluster = new Cluster(ClusterConfiguration.builder(testInfo, workDir).build());

        httpNode = RestNode.builder()
                .cluster(cluster)
                .index(0)
                .sslEnabled(false)
                .dualProtocol(false)
                .build();

        httpsNode = RestNode.builder()
                .cluster(cluster)
                .index(1)
                .sslEnabled(true)
                .dualProtocol(false)
                .build();

        dualProtocolNode = RestNode.builder()
                .cluster(cluster)
                .index(2)
                .sslEnabled(true)
                .dualProtocol(true)
                .build();

        httpsWithClientAuthNode = RestNode.builder()
                .cluster(cluster)
                .index(3)
                .sslEnabled(true)
                .sslClientAuthEnabled(true)
                .dualProtocol(false)
                .build();

        httpsWithCustomCipherNode = RestNode.builder()
                .cluster(cluster)
                .index(4)
                .sslEnabled(true)
                .dualProtocol(false)
                .ciphers("TLS_AES_256_GCM_SHA384")
                .build();

        Stream.of(httpNode, httpsNode, dualProtocolNode, httpsWithClientAuthNode, httpsWithCustomCipherNode)
                .forEach(RestNode::start);
    }

    @AfterAll
    static void shutdownCluster() {
        cluster.shutdown();
    }

    @Test
    void httpsProtocol() throws Exception {
        // When GET /management/v1/configuration/node
        URI uri = URI.create(httpsNode.httpsAddress() + "/management/v1/configuration/node");
        HttpRequest request = HttpRequest.newBuilder(uri).build();

        // Then response code is 200
        assertThat(sslClient.send(request, BodyHandlers.ofString()), hasStatusCode(200));
    }

    @Test
    void httpProtocol(TestInfo testInfo) throws Exception {
        // When GET /management/v1/configuration/node
        URI uri = URI.create(httpNode.httpAddress() + "/management/v1/configuration/node");
        HttpRequest request = HttpRequest.newBuilder(uri).build();

        // Then response code is 200
        assertThat(client.send(request, BodyHandlers.ofString()), hasStatusCode(200));
    }

    @Test
    void dualProtocol() throws Exception {
        // When GET /management/v1/configuration/node
        URI httpUri = URI.create(dualProtocolNode.httpAddress() + "/management/v1/configuration/node");
        HttpRequest httpRequest = HttpRequest.newBuilder(httpUri).build();

        URI httpsUri = URI.create(dualProtocolNode.httpsAddress() + "/management/v1/configuration/node");
        HttpRequest httpsRequest = HttpRequest.newBuilder(httpsUri).build();

        // Then HTTP response code is 200
        assertThat(client.send(httpRequest, BodyHandlers.ofString()), hasStatusCode(200));

        // And HTTPS response code is 200
        assertThat(sslClient.send(httpsRequest, BodyHandlers.ofString()), hasStatusCode(200));
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
    void httpProtocolNotSslClient() {
        // When GET /management/v1/configuration/node
        URI uri = URI.create(httpsNode.httpAddress() + "/management/v1/configuration/node");
        HttpRequest request = HttpRequest.newBuilder(uri).build();

        // Expect IOException
        assertThrows(IOException.class, () -> client.send(request, BodyHandlers.ofString()));
    }

    @Test
    void httpsWithClientAuthProtocol(TestInfo testInfo) throws Exception {
        // When GET /management/v1/configuration/node
        URI uri = URI.create(httpsWithClientAuthNode.httpsAddress() + "/management/v1/configuration/node");
        HttpRequest request = HttpRequest.newBuilder(uri).build();

        // Then response code is 200
        assertThat(sslClientWithClientAuth.send(request, BodyHandlers.ofString()), hasStatusCode(200));
    }

    @Test
    void httpsWithClientAuthProtocolButClientWithoutAuth(TestInfo testInfo) {
        // When GET /management/v1/configuration/node
        URI uri = URI.create(httpsWithClientAuthNode.httpsAddress() + "/management/v1/configuration/node");
        HttpRequest request = HttpRequest.newBuilder(uri).build();

        // Expect IOException for SSL client that does not configure client auth
        assertThrows(IOException.class, () -> sslClient.send(request, BodyHandlers.ofString()));
    }

    @Test
    void httpsWithCustomCipher(TestInfo testInfo) throws Exception {
        // When GET /management/v1/configuration/node
        URI uri = URI.create(httpsWithCustomCipherNode.httpsAddress() + "/management/v1/configuration/node");
        HttpRequest request = HttpRequest.newBuilder(uri).build();

        // Then response code is 200
        assertThat(sslClient.send(request, BodyHandlers.ofString()), hasStatusCode(200));
    }

    @Test
    void httpsWithCustomCipherButClientWithIncompatibleCipher(TestInfo testInfo) {
        // When GET /management/v1/configuration/node
        URI uri = URI.create(httpsWithCustomCipherNode.httpsAddress() + "/management/v1/configuration/node");
        HttpRequest request = HttpRequest.newBuilder(uri).build();

        // Expect IOException for SSL client that configures incompatible cipher
        assertThrows(IOException.class, () -> sslClientWithCustomCipher.send(request, BodyHandlers.ofString()));
    }

    private static SSLContext sslContext() throws CertificateException, KeyStoreException, IOException,
            NoSuchAlgorithmException, KeyManagementException {

        String tsPath = ItRestSslTest.class.getClassLoader().getResource(trustStorePath).getPath();

        KeyStore trustStore = KeyStore.getInstance(new File(tsPath), trustStorePassword.toCharArray());
        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(trustStore);

        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(null, trustManagerFactory.getTrustManagers(), new SecureRandom());

        return sslContext;
    }

    private static SSLContext sslContextWithClientAuth() throws CertificateException, KeyStoreException, IOException,
            NoSuchAlgorithmException, KeyManagementException, UnrecoverableKeyException {

        String tsPath = ItRestSslTest.class.getClassLoader().getResource(trustStorePath).getPath();
        String ksPath = ItRestSslTest.class.getClassLoader().getResource(keyStorePath).getPath();

        KeyStore trustStore = KeyStore.getInstance(new File(tsPath), trustStorePassword.toCharArray());
        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(trustStore);

        KeyStore keyStore = KeyStore.getInstance(new File(ksPath), keyStorePassword.toCharArray());
        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagerFactory.init(keyStore, keyStorePassword.toCharArray());

        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(keyManagerFactory.getKeyManagers(), trustManagerFactory.getTrustManagers(), new SecureRandom());

        return sslContext;
    }
}
