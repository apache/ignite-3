package org.apache.ignite.internal.cli.commands.optimise;

import static org.apache.ignite.internal.cli.commands.Options.Constants.CLUSTER_URL_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.WRITE_INTENSIVE_OPTION;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.ignite.internal.Cluster;
import org.apache.ignite.internal.cli.CliIntegrationTest;
import org.junit.jupiter.api.Test;

public class ItRunOptimiseCommandTest extends CliIntegrationTest {
    @Test
    public void testRunOptimise() {
        execute("optimise", "runOptimise", CLUSTER_URL_OPTION, "http://localhost:" + Cluster.BASE_HTTP_PORT, WRITE_INTENSIVE_OPTION);

        assertErrOutputEmpty();

        assertOutput("Optimisation was started successfully with id ");
    }

    private void assertErrOutputEmpty() {
        assertThat(serr.toString())
                .as("Expected command error output to be empty")
                .isEmpty();
    }

    private void assertOutput(String expectedOutput) {
        assertThat(sout.toString())
                .as("Expected command output to contain: " + expectedOutput + " but was " + sout.toString())
                .contains(expectedOutput);
    }
}
