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

package org.apache.ignite.internal.deployunit;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.deployunit.DeploymentStatus.DEPLOYED;
import static org.apache.ignite.internal.deployunit.DeploymentStatus.OBSOLETE;
import static org.apache.ignite.internal.deployunit.DeploymentStatus.REMOVING;
import static org.apache.ignite.internal.deployunit.DeploymentStatus.UPLOADING;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.doReturn;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.compute.version.Version;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.deployunit.configuration.DeploymentConfiguration;
import org.apache.ignite.internal.deployunit.metastore.DeploymentUnitStore;
import org.apache.ignite.internal.deployunit.metastore.status.UnitClusterStatus;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DeploymentManagerImplTest extends BaseIgniteAbstractTest {
    @Mock
    ClusterService clusterService;

    @Mock
    private DeploymentUnitStore deploymentUnitStore;

    @Mock
    private LogicalTopologyService logicalTopology;

    @Mock
    private Path workDir;

    @Mock
    private DeploymentConfiguration configuration;

    @Mock
    private ClusterManagementGroupManager cmgManage;

    @InjectMocks
    private DeploymentManagerImpl deployment;

    @Test
    public void detectLatestDeployedVersion() {
        List<UnitClusterStatus> unitClusterStatuses = List.of(
                new UnitClusterStatus("unit", Version.parseVersion("1.0.0"), DEPLOYED, UUID.randomUUID(), Set.of("node1", "node2")),
                new UnitClusterStatus("unit", Version.parseVersion("1.0.1"), OBSOLETE, UUID.randomUUID(), Set.of("node1", "node2")),
                new UnitClusterStatus("unit", Version.parseVersion("1.0.2"), DEPLOYED, UUID.randomUUID(), Set.of("node1", "node2")),
                new UnitClusterStatus("unit", Version.parseVersion("1.0.3"), UPLOADING, UUID.randomUUID(),  Set.of("node1", "node2")),
                new UnitClusterStatus("unit", Version.parseVersion("1.0.4"), REMOVING, UUID.randomUUID(), Set.of("node1", "node2"))
        );

        doReturn(completedFuture(unitClusterStatuses)).when(deploymentUnitStore).getClusterStatuses("unit");

        assertThat(
                deployment.detectLatestDeployedVersion("unit"),
                willBe(equalTo(Version.parseVersion("1.0.2")))
        );
    }
}
