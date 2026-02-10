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

import static org.junit.jupiter.api.Assertions.assertEquals;

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
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import org.apache.ignite.internal.Cluster;
import org.apache.ignite.internal.ClusterConfiguration;
import org.apache.ignite.internal.rest.ssl.ItRestSslTest;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Tests for the REST ports configuration. */
@ExtendWith(WorkDirectoryExtension.class)
public class ItRestPortsTest extends BaseIgniteAbstractTest {

    /** Trust store path. */
    private static final String trustStorePath = "ssl/truststore.jks";

    /** Trust store password. */
    private static final String trustStorePassword = "changeit";

    /** Path to the working directory. */
    @WorkDirectory
    private Path workDir;

    /** SSL HTTP client that is expected to be defined in subclasses. */
    private HttpClient sslClient;

    private Cluster cluster;

    @BeforeEach
    void beforeEach(TestInfo testInfo)
            throws CertificateException, KeyStoreException, IOException, NoSuchAlgorithmException, KeyManagementException {
        cluster = new Cluster(ClusterConfiguration.builder(testInfo, workDir).build());

        sslClient = HttpClient.newBuilder()
                .sslContext(sslContext())
                .build();
    }

    @AfterEach
    void shutdownCluster() {
        cluster.shutdown();
    }

    private static Stream<Arguments> sslConfigurationProperties() {
        return Stream.of(
                Arguments.of(false, false),
                Arguments.of(false, true),
                Arguments.of(true, false),
                Arguments.of(true, true)
        );
    }

    @ParameterizedTest
    @DisplayName("Ports are configured in all configurations")
    @MethodSource("sslConfigurationProperties")
    void portRange(boolean sslEnabled, boolean dualProtocol, TestInfo testInfo) throws Exception {
        List<RestNode> nodes = IntStream.range(0, 3)
                .mapToObj(index -> RestNode.builder()
                        .cluster(cluster)
                        .index(index)
                        .sslEnabled(sslEnabled)
                        .dualProtocol(dualProtocol)
                        .build())
                .collect(Collectors.toList());

        nodes.forEach(RestNode::start);

        // When GET /management/v1/configuration/node
        String httpAddress = sslEnabled ? nodes.get(0).httpsAddress() : nodes.get(0).httpAddress();
        URI uri = URI.create(httpAddress + "/management/v1/configuration/node");
        HttpRequest request = HttpRequest.newBuilder(uri).build();

        // Then response code is 200
        HttpResponse<String> response = sslClient.send(request, BodyHandlers.ofString());
        assertEquals(200, response.statusCode());
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
