package org.apache.ignite.cli.commands.configuration;


import static org.junit.jupiter.api.Assertions.assertAll;

import org.apache.ignite.cli.commands.CliCommandTestIntegrationBase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link ConfigCommand}.
 */
class ItConfigCommandTest extends CliCommandTestIntegrationBase {

    @Test
    @DisplayName("Should read config when valid cluster-url is given")
    void readDefaultConfig() {
        // When read cluster config with valid url
        execute("config", "show", "--cluster-url", NODE_URL);

        // Then
        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                this::assertOutputIsNotEmpty
        );
    }

    @Test
    @DisplayName("Should update config with hocon format when valid cluster-url is given")
    void addConfigKeyValue() {
        // When update default data storage to rocksdb
        execute("config", "update", "--cluster-url", NODE_URL, "{table: {defaultDataStorage: rocksdb}}");

        // Then
        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                this::assertOutputIsNotEmpty
        );

        // When read the updated cluster configuration
        execute("config", "show", "--cluster-url", NODE_URL);

        // Then
        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("\"defaultDataStorage\" : \"rocksdb\"")
        );
    }

    @Test
    @DisplayName("Should update config with key-value format when valid cluster-url is given")
    void updateConfigWithSpecifiedPath() {
        // When update default data storage to rocksdb
        execute("config", "update", "--cluster-url", NODE_URL, "table.defaultDataStorage=rocksdb");

        // Then
        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                this::assertOutputIsNotEmpty
        );

        // When read the updated cluster configuration
        execute("config", "show", "--cluster-url", NODE_URL);

        // Then
        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("\"defaultDataStorage\" : \"rocksdb\"")
        );
    }
}