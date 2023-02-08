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

package org.apache.ignite.internal.network.ssl;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThrows;

import io.netty.handler.ssl.SslContext;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.network.configuration.SslConfiguration;
import org.apache.ignite.lang.ErrorGroups.Common;
import org.apache.ignite.lang.IgniteException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ConfigurationExtension.class)
class SslContextProviderTest {

    @InjectConfiguration
    SslConfiguration configuration;

    String password;

    String keyStorePkcs12Path;

    String trustStoreJks12Path;

    @BeforeEach
    void setUp() {
        password = "changeit";
        keyStorePkcs12Path = SslContextProviderTest.class.getClassLoader().getResource("ssl/keystore.p12").getPath();
        trustStoreJks12Path = SslContextProviderTest.class.getClassLoader().getResource("ssl/truststore.jks").getPath();
    }

    @Test
    void createsSslContextForClient() throws Exception {
        // Given valid self-signed certificate
        configuration.trustStore().type().update("PKCS12").get();
        configuration.trustStore().path().update(keyStorePkcs12Path).get();
        configuration.trustStore().password().update(password).get();

        // When
        SslContext clientSslContext = SslContextProvider.forClient(configuration.value()).createSslContext();

        // Then
        assertThat(clientSslContext, notNullValue());
    }

    @Test
    void createsSslContextForServer() throws Exception {
        // Given valid self-signed certificate
        configuration.keyStore().password().update(password).get();
        configuration.keyStore().path().update(keyStorePkcs12Path).get();

        // When
        SslContext sslContext = SslContextProvider.forServer(configuration.value()).createSslContext();

        // Then
        assertThat(sslContext, notNullValue());
    }

    @Test
    void createsSslContextForServerJks() throws Exception {
        // Given valid self-signed certificate
        configuration.keyStore().type().update("JKS").get();
        configuration.keyStore().password().update(password).get();
        configuration.keyStore().path().update(trustStoreJks12Path).get();

        // When
        SslContext sslContext = SslContextProvider.forServer(configuration.value()).createSslContext();

        // Then
        assertThat(sslContext, notNullValue());
    }

    @Test
    void throwsIgniteExceptionWhenWrongKeystorePathConfigured() throws Exception {
        // Given wrong path configured for keystore
        configuration.keyStore().path().update("/no/such/file.pfx").get();

        // When
        var thrown = assertThrows(
                IgniteException.class,
                () -> SslContextProvider.forServer(configuration.value()).createSslContext()
        );

        // Then
        assertThat(thrown.groupName(), equalTo(Common.COMMON_ERR_GROUP.name()));
        assertThat(thrown.code(), equalTo(Common.SSL_CONFIGURATION_ERR));
        assertThat(thrown.getMessage(), containsString("File /no/such/file.pfx not found"));
    }

    @Test
    void throwsIgniteExceptionWhenWrongTruststorePathConfigured() throws Exception {
        // Given wrong path configured for truststore
        configuration.trustStore().path().update("/no/such/file.pfx").get();

        // When
        var thrown = assertThrows(
                IgniteException.class,
                () -> SslContextProvider.forClient(configuration.value()).createSslContext()
        );

        // Then
        assertThat(thrown.groupName(), equalTo(Common.COMMON_ERR_GROUP.name()));
        assertThat(thrown.code(), equalTo(Common.SSL_CONFIGURATION_ERR));
        assertThat(thrown.getMessage(), containsString("File /no/such/file.pfx not found"));
    }

    @Test
    void throwsIgniteExceptionWhenWrongKeystorePassword() throws Exception {
        // Given wrong password for keystore
        configuration.keyStore().path().update(keyStorePkcs12Path).get();
        configuration.keyStore().password().update("wrong").get();

        // When
        var thrown = assertThrows(
                IgniteException.class,
                () -> SslContextProvider.forServer(configuration.value()).createSslContext()
        );

        // Then
        assertThat(thrown.groupName(), equalTo(Common.COMMON_ERR_GROUP.name()));
        assertThat(thrown.code(), equalTo(Common.SSL_CONFIGURATION_ERR));
        assertThat(thrown.getMessage(), containsString("keystore password was incorrect"));
    }

    @Test
    void throwsIgniteExceptionWhenWrongTruststorePassword() throws Exception {
        // Given wrong password for truststore
        configuration.trustStore().path().update(trustStoreJks12Path).get();
        configuration.trustStore().password().update("wrong").get();

        // When
        var thrown = assertThrows(
                IgniteException.class,
                () -> SslContextProvider.forClient(configuration.value()).createSslContext()
        );

        // Then
        assertThat(thrown.groupName(), equalTo(Common.COMMON_ERR_GROUP.name()));
        assertThat(thrown.code(), equalTo(Common.SSL_CONFIGURATION_ERR));
        assertThat(thrown.getMessage(), containsString("keystore password was incorrect"));
    }
}
