package org.apache.ignite.internal.cli.call.configuration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import jakarta.inject.Inject;
import org.apache.ignite.internal.cli.CliIntegrationTest;
import org.apache.ignite.internal.cli.call.cluster.status.ClusterRenameCall;
import org.apache.ignite.internal.cli.call.cluster.status.ClusterRenameCallInput;
import org.apache.ignite.internal.cli.call.cluster.status.ClusterStatusCall;
import org.apache.ignite.internal.cli.core.call.DefaultCallOutput;
import org.apache.ignite.internal.cli.core.call.UrlCallInput;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link ClusterRenameCall}.
 */
public class ItRenameCallTest extends CliIntegrationTest {
    private static final UrlCallInput URL_CALL_INPUT = new UrlCallInput(NODE_URL);

    @Inject
    ClusterRenameCall renameCall;

    @Inject
    ClusterStatusCall statusCall;


    @Test
    @DisplayName("Should rename the cluster")
    public void testRename() {
        String name = readClusterName();
        assertEquals("cluster", name);

        var input = ClusterRenameCallInput.builder()
                .clusterUrl(NODE_URL)
                .name("cluster2")
                .build();

        DefaultCallOutput<String> output = renameCall.execute(input);
        assertFalse(output.hasError());
        assertThat(output.body()).contains("Cluster was renamed successfully");

        name = readClusterName();
        assertEquals("cluster2", name);
    }

    private String readClusterName() {
        return statusCall.execute(URL_CALL_INPUT).body().getName();
    }
}
