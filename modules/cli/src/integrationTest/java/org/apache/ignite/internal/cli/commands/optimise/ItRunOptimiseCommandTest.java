package org.apache.ignite.internal.cli.commands.optimise;

import static org.apache.ignite.internal.cli.commands.Options.Constants.CLUSTER_URL_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.WRITE_INTENSIVE_OPTION;

import org.apache.ignite.internal.cli.CliIntegrationTest;
import org.junit.jupiter.api.Test;

public class ItRunOptimiseCommandTest extends CliIntegrationTest {
    @Test
    public void testRunOptimise() {
        execute("optimise", "runOptimise", CLUSTER_URL_OPTION, NODE_URL, WRITE_INTENSIVE_OPTION);

        assertErrOutputIsEmpty();
        assertOutputContains("Optimisation was started successfully with id ");
    }
}
