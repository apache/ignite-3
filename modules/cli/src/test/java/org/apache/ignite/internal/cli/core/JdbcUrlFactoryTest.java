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

import static org.apache.ignite.internal.cli.commands.cliconfig.TestConfigManagerHelper.createIntegrationTests;
import static org.apache.ignite.internal.cli.commands.cliconfig.TestConfigManagerHelper.createJdbcTestsBasicSecret;
import static org.apache.ignite.internal.cli.commands.cliconfig.TestConfigManagerHelper.createJdbcTestsSslBasicSecret;
import static org.apache.ignite.internal.cli.commands.cliconfig.TestConfigManagerHelper.createJdbcTestsSslSecret;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.ignite.internal.cli.commands.cliconfig.TestConfigManagerProvider;
import org.junit.jupiter.api.Test;

class JdbcUrlFactoryTest {

    private final TestConfigManagerProvider configManagerProvider = new TestConfigManagerProvider();
    private final JdbcUrlFactory factory = new JdbcUrlFactory(configManagerProvider);

    @Test
    void withoutSsl() {
        // Given default config

        // Then JDBC URL is constructed without SSL settings
        String jdbcUrl = factory.constructJdbcUrl("{clientConnector:{port:10800}}", "http://localhost:10300");
        assertEquals("jdbc:ignite:thin://localhost:10800", jdbcUrl);
    }

    @Test
    void withSsl() {
        // Given config with JDBC SSL enabled
        configManagerProvider.setConfigFile(createIntegrationTests(), createJdbcTestsSslSecret());

        // Then JDBC URL is constructed with SSL settings
        String jdbcUrl = factory.constructJdbcUrl("{clientConnector:{port:10800}}", "http://localhost:10300");
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
        // Given config with basic auth enabled
        configManagerProvider.setConfigFile(createIntegrationTests(), createJdbcTestsBasicSecret());

        // Then JDBC URL is constructed with basic auth settings
        String jdbcUrl = factory.constructJdbcUrl("{clientConnector:{port:10800}}", "http://localhost:10300");
        String expectedJdbcUrl = "jdbc:ignite:thin://localhost:10800"
                + "?basicAuthUsername=usr"
                + "&basicAuthPassword=pwd";
        assertEquals(expectedJdbcUrl, jdbcUrl);
    }

    @Test
    void withSslAndBasic() {
        // Given config with JDBC SSL and basic auth enabled
        configManagerProvider.setConfigFile(createIntegrationTests(), createJdbcTestsSslBasicSecret());

        // Then JDBC URL is constructed with SSL and basic auth settings
        String jdbcUrl = factory.constructJdbcUrl("{clientConnector:{port:10800}}", "http://localhost:10300");
        String expectedJdbcUrl = "jdbc:ignite:thin://localhost:10800"
                + "?sslEnabled=true"
                + "&trustStorePath=ssl/truststore.jks"
                + "&trustStorePassword=changeit"
                + "&keyStorePath=ssl/keystore.p12"
                + "&keyStorePassword=changeit"
                + "&basicAuthUsername=usr"
                + "&basicAuthPassword=pwd";
        assertEquals(expectedJdbcUrl, jdbcUrl);
    }
}
