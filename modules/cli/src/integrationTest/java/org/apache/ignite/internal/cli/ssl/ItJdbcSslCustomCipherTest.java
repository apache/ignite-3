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

package org.apache.ignite.internal.cli.ssl;

import static org.apache.ignite.internal.NodeConfig.clientConnectorSslBootstrapConfig;
import static org.junit.jupiter.api.Assertions.assertAll;

import org.apache.ignite.internal.NodeConfig;
import org.apache.ignite.internal.cli.CliIntegrationTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for JDBC SSL. */
public class ItJdbcSslCustomCipherTest extends CliIntegrationTest {
    private static final String CIPHER1 = "TLS_AES_256_GCM_SHA384";
    private static final String CIPHER2 = "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384";

    @Override
    protected String getNodeBootstrapConfigTemplate() {
        return clientConnectorSslBootstrapConfig(CIPHER1);
    }

    @BeforeEach
    public void createTable() {
        createAndPopulateTable();
    }

    @AfterEach
    public void dropTables() {
        dropAllTables();
    }

    @Test
    void jdbcCompatibleCiphers() {
        // Given valid JDBC connection string with SSL configured
        String jdbcUrl = JDBC_URL
                + "?sslEnabled=true"
                + "&trustStorePath=" + NodeConfig.resolvedTruststorePath
                + "&trustStoreType=JKS"
                + "&trustStorePassword=" + NodeConfig.trustStorePassword
                + "&clientAuth=require"
                + "&keyStorePath=" + NodeConfig.resolvedKeystorePath
                + "&keyStoreType=PKCS12"
                + "&keyStorePassword=" + NodeConfig.keyStorePassword
                + "&ciphers=" + CIPHER1;

        // When
        execute("sql", "--jdbc-url", jdbcUrl, "select * from person");

        // Then the query is executed successfully
        assertAll(
                this::assertExitCodeIsZero,
                this::assertOutputIsNotEmpty,
                this::assertErrOutputIsEmpty
        );
    }

    @Test
    void jdbcIncompatibleCiphers() {
        // Given JDBC connection string with SSL configured but incompatible cipher
        String jdbcUrl = JDBC_URL
                + "?sslEnabled=true"
                + "&trustStorePath=" + NodeConfig.resolvedTruststorePath
                + "&trustStoreType=JKS"
                + "&trustStorePassword=" + NodeConfig.trustStorePassword
                + "&clientAuth=require"
                + "&keyStorePath=" + NodeConfig.resolvedKeystorePath
                + "&keyStoreType=PKCS12"
                + "&keyStorePassword=" + NodeConfig.keyStorePassword
                + "&ciphers=" + CIPHER2;

        // When
        execute("sql", "--jdbc-url", jdbcUrl, "select * from person");

        // Then the query is executed successfully
        assertAll(
                () -> assertExitCodeIs(1),
                this::assertOutputIsEmpty,
                () -> assertErrOutputContains("Connection failed"),
                () -> assertErrOutputContains("Handshake error")
        );
    }
}
