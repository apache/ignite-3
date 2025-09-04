package org.apache.ignite.internal.deployment;

import static org.apache.ignite.deployment.version.Version.parseVersion;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.Ignite;
import org.apache.ignite.deployment.version.Version;
import org.apache.ignite.internal.Cluster;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.deployunit.DeploymentStatus;
import org.apache.ignite.internal.deployunit.IgniteDeployment;
import org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ItStaticDeploymentTest extends ClusterPerClassIntegrationTest {
    private DeployFiles files;

    @BeforeEach
    public void generateDummy() {
        files = new DeployFiles(WORK_DIR);
    }

    @Override
    protected boolean needInitializeCluster() {
        return false;
    }

    @Test
    public void testStaticDeploy() throws IOException {
        DeployFile smallFile = files.smallFile();
        DeployFile mediumFile = files.mediumFile();

        Path node0WorkDir = CLUSTER.nodeWorkDir(0);
        Path node1WorkDir = CLUSTER.nodeWorkDir(1);
        Path node2WorkDir = CLUSTER.nodeWorkDir(2);

        DeployFiles.staticDeploy("unit1", parseVersion("1.0.0"), smallFile, node0WorkDir);

        DeployFiles.staticDeploy("unit2", parseVersion("1.0.0"), smallFile, node0WorkDir);
        DeployFiles.staticDeploy("unit2", parseVersion("1.0.0"), smallFile, node1WorkDir);

        DeployFiles.staticDeploy("unit3", parseVersion("1.0.0"), smallFile, node0WorkDir);
        DeployFiles.staticDeploy("unit3", parseVersion("1.0.0"), smallFile, node1WorkDir);
        DeployFiles.staticDeploy("unit3", parseVersion("1.0.0"), smallFile, node2WorkDir);

        DeployFiles.staticDeploy("unit1", parseVersion("1.1.0"), smallFile, node0WorkDir);
        DeployFiles.staticDeploy("unit1", parseVersion("1.1.0"), smallFile, node1WorkDir);

        DeployFiles.staticDeploy("unit3", parseVersion("1.0.1"), smallFile, node2WorkDir);

        CLUSTER.startAndInit(3);

        IgniteDeployment node0 = unwrapIgniteImpl(CLUSTER.node(0)).deployment();
        IgniteDeployment node1 = unwrapIgniteImpl(CLUSTER.node(1)).deployment();
        IgniteDeployment node2 = unwrapIgniteImpl(CLUSTER.node(2)).deployment();

        CompletableFuture<DeploymentStatus> unit1 = node0.nodeStatusAsync("unit1", parseVersion("1.0.0"));

        assertThat(unit1, CompletableFutureMatcher.willBe(DeploymentStatus.DEPLOYED));
    }
}
