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

package org.apache.ignite.internal.cli.core;

import static org.apache.ignite.internal.cli.commands.cliconfig.TestConfigManagerHelper.createIntegrationTestsConfig;
import static org.apache.ignite.internal.cli.commands.cliconfig.TestConfigManagerHelper.createJdbcTestsBasicSecretConfig;
import static org.apache.ignite.internal.cli.commands.cliconfig.TestConfigManagerHelper.createJdbcTestsSslBasicSecretConfig;
import static org.apache.ignite.internal.cli.commands.cliconfig.TestConfigManagerHelper.createJdbcTestsSslSecretConfig;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.ignite.internal.cli.commands.cliconfig.TestConfigManagerProvider;
import org.apache.ignite.internal.cli.config.CliConfigKeys;
import org.junit.jupiter.api.Test;

class JdbcUrlFactoryTest {

    private final TestConfigManagerProvider configManagerProvider = new TestConfigManagerProvider();
    private final JdbcUrlFactory factory = new JdbcUrlFactory(configManagerProvider);

    @Test
    void withoutSsl() {
        // Given default config

        // Then JDBC URL is constructed without SSL settings
        String jdbcUrl = factory.constructJdbcUrl("http://localhost:10300", 10800);
        assertEquals("jdbc:ignite:thin://localhost:10800", jdbcUrl);
    }

    @Test
    void withSsl() {
        // Given config with JDBC SSL enabled
        configManagerProvider.setConfigFile(createIntegrationTestsConfig(), createJdbcTestsSslSecretConfig());

        // Then JDBC URL is constructed with SSL settings
        String jdbcUrl = factory.constructJdbcUrl("http://localhost:10300", 10800);
        String expectedJdbcUrl = "jdbc:ignite:thin://localhost:10800"
                + "?sslEnabled=true"
                + "&trustStorePath=ssl/truststore.jks"
                + "&trustStorePassword=changeit"
                + "&keyStorePath=ssl/keystore.p12"
                + "&keyStorePassword=changeit";
        assertEquals(expectedJdbcUrl, jdbcUrl);
    }

    @Test
    void withSslEnabledExplicitly() {
        // Given config with JDBC SSL enabled and ssl-enabled set to true in the config explicitly
        configManagerProvider.setConfigFile(createIntegrationTestsConfig(), createJdbcTestsSslSecretConfig());
        configManagerProvider.configManager.setProperty(CliConfigKeys.JDBC_SSL_ENABLED.value(), "true");

        // Then JDBC URL is constructed with SSL settings
        String jdbcUrl = factory.constructJdbcUrl("http://localhost:10300", 10800);
        String expectedJdbcUrl = "jdbc:ignite:thin://localhost:10800"
                + "?sslEnabled=true"
                + "&trustStorePath=ssl/truststore.jks"
                + "&trustStorePassword=changeit"
                + "&keyStorePath=ssl/keystore.p12"
                + "&keyStorePassword=changeit";
        assertEquals(expectedJdbcUrl, jdbcUrl);
    }

    @Test
    void withBasic() {
        // Given config with basic authentication enabled
        configManagerProvider.setConfigFile(createIntegrationTestsConfig(), createJdbcTestsBasicSecretConfig());

        // Then JDBC URL is constructed with basic authentication settings
        String jdbcUrl = factory.constructJdbcUrl("http://localhost:10300", 10800);
        String expectedJdbcUrl = "jdbc:ignite:thin://localhost:10800"
                + "?username=admin"
                + "&password=password";
        assertEquals(expectedJdbcUrl, jdbcUrl);
    }

    @Test
    void withSslAndBasic() {
        // Given config with JDBC SSL and basic authentication enabled
        configManagerProvider.setConfigFile(createIntegrationTestsConfig(), createJdbcTestsSslBasicSecretConfig());

        // Then JDBC URL is constructed with SSL and basic authentication settings
        String jdbcUrl = factory.constructJdbcUrl("http://localhost:10300", 10800);
        String expectedJdbcUrl = "jdbc:ignite:thin://localhost:10800"
                + "?sslEnabled=true"
                + "&trustStorePath=ssl/truststore.jks"
                + "&trustStorePassword=changeit"
                + "&keyStorePath=ssl/keystore.p12"
                + "&keyStorePassword=changeit"
                + "&username=usr"
                + "&password=pwd";
        assertEquals(expectedJdbcUrl, jdbcUrl);
    }

    @Test
    void withCustomCipher() {
        // Given config with JDBC SSL and basic authentication enabled
        configManagerProvider.setConfigFile(createIntegrationTestsConfig(), createJdbcTestsSslSecretConfig());
        configManagerProvider.configManager.setProperty(CliConfigKeys.JDBC_CIPHERS.value(), "TLS_AES_256_GCM_SHA384");

        // Then JDBC URL is constructed with SSL settings and custom cipher
        String jdbcUrl = factory.constructJdbcUrl("http://localhost:10300", 10800);
        String expectedJdbcUrl = "jdbc:ignite:thin://localhost:10800"
                + "?sslEnabled=true"
                + "&trustStorePath=ssl/truststore.jks"
                + "&trustStorePassword=changeit"
                + "&keyStorePath=ssl/keystore.p12"
                + "&keyStorePassword=changeit"
                + "&ciphers=TLS_AES_256_GCM_SHA384";
        assertEquals(expectedJdbcUrl, jdbcUrl);
    }
}
