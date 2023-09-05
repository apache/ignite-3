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

import static org.junit.jupiter.api.Assertions.assertAll;

import org.apache.ignite.internal.NodeConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Tests for SSL connection with JDBC URL. */
public class ItJdbcSslTest extends CliSslClientConnectorIntegrationTestBase {

    @BeforeEach
    public void createTable() {
        createAndPopulateTable();
    }

    @AfterEach
    public void dropTables() {
        dropAllTables();
    }

    @Test
    @DisplayName("Should connect with JDBC URL with SSL configured")
    void jdbcOkWithSslConfigured() {
        // Given valid JDBC connection string with SSL configured
        String jdbcUrl = JDBC_URL
                + "?sslEnabled=true"
                + "&trustStorePath=" + NodeConfig.resolvedTruststorePath
                + "&trustStoreType=JKS"
                + "&trustStorePassword=" + NodeConfig.trustStorePassword
                + "&clientAuth=require"
                + "&keyStorePath=" + NodeConfig.resolvedKeystorePath
                + "&keyStoreType=PKCS12"
                + "&keyStorePassword=" + NodeConfig.keyStorePassword;

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
    @DisplayName("Should fail to connect with JDBC URL with SSL configured but truststore is not configured")
    void jdbcFailWithSslConfiguredButTruststoreNotConfigured() {
        // Given valid JDBC connection string with SSL configured
        String jdbcUrl = JDBC_URL
                + "?sslEnabled=true"
                + "&clientAuth=require"
                + "&keyStorePath=" + NodeConfig.resolvedKeystorePath
                + "&keyStoreType=PKCS12"
                + "&keyStorePassword=" + NodeConfig.keyStorePassword;

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

    @Test
    @DisplayName("Should fail to connect with JDBC URL with SSL configured but keystore is not configured")
    void jdbcFailWithSslConfiguredButKeystoreNotConfigured() {
        // Given valid JDBC connection string with SSL configured
        String jdbcUrl = JDBC_URL
                + "?sslEnabled=true"
                + "&trustStorePath=" + NodeConfig.resolvedTruststorePath
                + "&trustStoreType=JKS"
                + "&trustStorePassword=" + NodeConfig.trustStorePassword
                + "&clientAuth=require";

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
