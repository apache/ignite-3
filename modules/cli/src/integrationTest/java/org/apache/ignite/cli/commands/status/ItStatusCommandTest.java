package org.apache.ignite.cli.commands.status;

import static org.junit.jupiter.api.Assertions.assertAll;

import org.apache.ignite.cli.commands.CliCommandTestIntegrationBase;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link StatusCommand}.
 */
class ItStatusCommandTest extends CliCommandTestIntegrationBase {

    @Test
    @DisplayName("Should print status when valid cluster url is given")
    @Disabled // todo: implement status command
    void printStatus() {
        execute("status", "--cluster-url", NODE_URL);

        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("Cluster status:")
        );
    }
}
