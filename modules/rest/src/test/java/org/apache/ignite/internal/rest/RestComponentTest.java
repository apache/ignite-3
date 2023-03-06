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


import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.netty.handler.ssl.util.SelfSignedCertificate;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.util.List;
import org.apache.ignite.internal.configuration.AuthenticationConfiguration;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.rest.authentication.AuthProviderFactory;
import org.apache.ignite.internal.rest.configuration.RestConfiguration;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.lang.ErrorGroups.Common;
import org.apache.ignite.lang.IgniteException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ConfigurationExtension.class)
@ExtendWith(WorkDirectoryExtension.class)
class RestComponentTest {

    private String keyStorePath;

    @InjectConfiguration
    private AuthenticationConfiguration authenticationConfiguration;

    @WorkDirectory
    private Path workDir;

    private static void pingOk(RestComponent component) throws IOException, InterruptedException {
        HttpClient client = HttpClient.newBuilder().build();
        HttpResponse<String> response = client.send(
                HttpRequest.newBuilder()
                        .uri(URI.create("http://" + component.host() + ":" + component.httpPort() + "/ping")).GET()
                        .build(),
                BodyHandlers.ofString()
        );

        assertThat(response.statusCode(), is(200));
        assertThat(response.body(), is("pong"));
    }

    @BeforeEach
    void setUp() {
        keyStorePath = workDir.resolve("keystore.p12").toAbsolutePath().toString();
    }

    @Test
    @DisplayName("REST component stars with default configuration")
    void defaultConfiguration(@InjectConfiguration RestConfiguration restConfiguration) throws Exception {
        // Given
        RestComponent component = new RestComponent(List.of(() -> new AuthProviderFactory(authenticationConfiguration)), restConfiguration);

        // When
        component.start();

        // Then
        pingOk(component);
    }

    @Test
    @DisplayName("REST component does not start with ssl.enabled=true and no keystore")
    void sslConfiguration(@InjectConfiguration("mock.ssl.enabled: true") RestConfiguration restConfiguration) {
        // Given
        RestComponent component = new RestComponent(List.of(() -> new AuthProviderFactory(authenticationConfiguration)), restConfiguration);

        // When
        IgniteException thrown = assertThrows(IgniteException.class, component::start);

        // Then
        assertThat(thrown.code(), is(Common.SSL_CONFIGURATION_ERR));
    }

    @Test
    @DisplayName("REST component does not start with ssl.clientAuth=require and no truststore")
    void clientAuthConfiguration(@InjectConfiguration RestConfiguration restConfiguration) throws Exception {
        // Given correct keystore
        generateKeystore(new SelfSignedCertificate());
        restConfiguration.ssl().enabled().update(true).get();
        restConfiguration.ssl().keyStore().path().update(keyStorePath).get();
        restConfiguration.ssl().keyStore().password().update("changeit").get();
        // And clientAuth=require But no truststore
        restConfiguration.ssl().clientAuth().update("require").get();

        RestComponent component = new RestComponent(List.of(() -> new AuthProviderFactory(authenticationConfiguration)), restConfiguration);

        // When
        IgniteException thrown = assertThrows(IgniteException.class, component::start);

        // Then
        assertThat(thrown.code(), is(Common.SSL_CONFIGURATION_ERR));
    }

    private void generateKeystore(SelfSignedCertificate cert)
            throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException {
        KeyStore ks = KeyStore.getInstance("PKCS12");
        ks.load(null, null);
        ks.setKeyEntry("key", cert.key(), null, new Certificate[]{cert.cert()});
        try (FileOutputStream fos = new FileOutputStream(keyStorePath)) {
            ks.store(fos, "changeit".toCharArray());
        }
    }
}
