package org.apache.ignite.cli.commands.topology;

import static org.junit.jupiter.api.Assertions.assertAll;

import org.apache.ignite.cli.commands.CliCommandTestIntegrationBase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link ItTopologyCommand}.
 */
class ItTopologyCommandTest extends CliCommandTestIntegrationBase {

    @Test
    @DisplayName("Should print topology when valid cluster url is provided")
    void printTopology() {
        // When
        execute("topology", "--cluster-url", NODE_URL);

        // Then
        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                this::assertOutputIsNotEmpty
        );
        //todo: check output contains something like:
        //consistent ID, ID, address, status
        //node 1, e2d4988a-b836-4e7e-a888-2639e6f79ef0, 127.0.0.1, RUNNING
        //node 2, 5cb561fc-1963-4f95-98f8-deb407669a86, 127.0.0.2, RECOVERY
    }
}