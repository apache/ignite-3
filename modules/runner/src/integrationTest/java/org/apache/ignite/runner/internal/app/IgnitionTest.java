package org.apache.ignite.runner.internal.app;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.app.Ignite;
import org.apache.ignite.app.IgnitionProcessor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Ignition interface tests.
 */
class IgnitionTest {

    /** Nodes bootstrap configuration. */
    private final String[] nodesBootstrapCfg =
        {
            "{\n" +
                "  \"network\": {\n" +
                "    \"port\":3344,\n" +
                "    \"netClusterNodes\":[ \"localhost:3344\", \"localhost:3345\", \"localhost:3346\" ]\n" +
                "  }\n" +
                "}",

            "{\n" +
                "  \"network\": {\n" +
                "    \"port\":3345,\n" +
                "    \"netClusterNodes\":[ \"localhost:3344\", \"localhost:3345\", \"localhost:3346\" ]\n" +
                "  }\n" +
                "}",

            "{\n" +
                "  \"network\": {\n" +
                "    \"port\":3346,\n" +
                "    \"netClusterNodes\":[ \"localhost:3344\", \"localhost:3345\", \"localhost:3346\" ]\n" +
                "  }\n" +
                "}",
        };

    /**
     * Check that Ignition.start() with bootstrap configuration returns Ignite instance.
     */
    @Test
    void testNodesStartWithBootstrapConfiguration() {
        List<Ignite> startedNodes = new ArrayList<>();

        for (String nodeBootstrapCfg : nodesBootstrapCfg)
            startedNodes.add(IgnitionProcessor.INSTANCE.start(nodeBootstrapCfg));

        Assertions.assertEquals(3, startedNodes.size());

        startedNodes.forEach(Assertions::assertNotNull);
    }

    /**
     * Check that Ignition.start() with bootstrap configuration returns Ignite instance.
     */
    @Test
    void testNodeStartWithoutBootstrapConfiguartion() {
        Ignite ignite = IgnitionProcessor.INSTANCE.start(null);

        Assertions.assertNotNull(ignite);
    }
}
