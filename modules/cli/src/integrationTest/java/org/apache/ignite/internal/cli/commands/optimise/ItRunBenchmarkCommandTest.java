
package org.apache.ignite.internal.cli.commands.optimise;

import static org.apache.ignite.internal.cli.commands.Options.Constants.CLUSTER_URL_OPTION;

import org.apache.ignite.internal.cli.CliIntegrationTest;
import org.junit.jupiter.api.Test;

/** Base test class for Cluster Recovery restart partitions commands. */
public class ItRunBenchmarkCommandTest extends CliIntegrationTest {
    @Test
    public void testRunBenchmark() {
        execute("optimise", "runBenchmark", CLUSTER_URL_OPTION, NODE_URL, "benchmark.sql");

        assertErrOutputIsEmpty();
        assertOutputContains("Benchmark was started successfully with id ");
    }
}
