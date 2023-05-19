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

package org.apache.ignite.deployment.metastore;

import static org.apache.ignite.internal.rest.api.deployment.DeploymentStatus.*;
import static org.apache.ignite.internal.rest.api.deployment.DeploymentStatus.DEPLOYED;
import static org.apache.ignite.internal.rest.api.deployment.DeploymentStatus.REMOVING;
import static org.apache.ignite.internal.rest.api.deployment.DeploymentStatus.UPLOADING;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.ignite.internal.deployunit.UnitStatus;
import org.apache.ignite.internal.deployunit.metastore.DeploymentUnitStoreImpl;
import org.apache.ignite.internal.deployunit.metastore.NodeEventListener;
import org.apache.ignite.internal.deployunit.metastore.status.UnitClusterStatus;
import org.apache.ignite.internal.deployunit.metastore.status.UnitNodeStatus;
import org.apache.ignite.internal.deployunit.version.Version;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.impl.StandaloneMetaStorageManager;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.metastorage.server.persistence.RocksDbKeyValueStorage;
import org.apache.ignite.internal.rest.api.deployment.DeploymentStatus;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.inmemory.InMemoryVaultService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Test suite for {@link DeploymentUnitStoreImpl}.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class DeploymentUnitStoreImplTest {
    private static final String LOCAL_NODE = "localNode";

    private final VaultManager vaultManager = new VaultManager(new InMemoryVaultService());

    private final List<UnitNodeStatus> history = new ArrayList<>();

    private final NodeEventListener listener = (status, holders) -> {
        history.add(status);
    };

    private DeploymentUnitStoreImpl metastore;

    @WorkDirectory
    private Path workDir;

    @BeforeEach
    public void setup() {
        history.clear();
        KeyValueStorage storage = new RocksDbKeyValueStorage("test", workDir);

        MetaStorageManager metaStorageManager = StandaloneMetaStorageManager.create(vaultManager, storage);

        vaultManager.start();
        metaStorageManager.start();

        metastore = new DeploymentUnitStoreImpl(metaStorageManager, () -> LOCAL_NODE, listener);
    }

    @Test
    public void clusterStatusTest() {
        String id = "id1";
        Version version = Version.parseVersion("1.1.1");

        assertThat(metastore.createClusterStatus(id, version, Set.of()), willBe(true));

        assertThat(metastore.getClusterStatus(id, version),
                willBe(new UnitClusterStatus(id, version, UPLOADING, Set.of())));

        assertThat(metastore.updateClusterStatus(id, version, DEPLOYED), willBe(true));
        assertThat(metastore.getClusterStatus(id, version),
                willBe(new UnitClusterStatus(id, version, DEPLOYED, Set.of())));

        assertThat(metastore.remove(id, version), willBe(true));

        assertThat(metastore.getClusterStatus(id, version), willBe((UnitStatus) null));
    }

    @Test
    public void nodeStatusTest() {
        String id = "id2";
        Version version = Version.parseVersion("1.1.1");

        String node1 = "node1";
        String node2 = "node2";
        String node3 = "node3";

        assertThat(metastore.createClusterStatus(id, version, Set.of(node1, node2, node3)), willBe(true));
        assertThat(metastore.getClusterStatus(id, version),
                willBe(new UnitClusterStatus(id, version, UPLOADING, Set.of(node1, node2, node3))));

        assertThat(metastore.createNodeStatus(id, version, node1), willBe(true));
        assertThat(metastore.getNodeStatus(id, version, node1),
                willBe(new UnitNodeStatus(id, version, UPLOADING)));

        assertThat(metastore.updateNodeStatus(id, version, node1, DEPLOYED), willBe(true));
        assertThat(metastore.getNodeStatus(id, version, node1),
                willBe(new UnitNodeStatus(id, version, DEPLOYED)));

        assertThat(metastore.createNodeStatus(id, version, node2), willBe(true));
        assertThat(metastore.getNodeStatus(id, version, node2),
                willBe(new UnitNodeStatus(id, version, UPLOADING)));

        assertThat(metastore.createNodeStatus(id, version, node3), willBe(true));

        assertThat(metastore.updateClusterStatus(id, version, DEPLOYED), willBe(true));
        assertThat(metastore.getClusterStatus(id, version),
                willBe(new UnitClusterStatus(id, version, DEPLOYED, Set.of(node1, node2, node3))));

        assertThat(metastore.getClusterStatuses(id),
                willBe(List.of(new UnitClusterStatus(id, version, DEPLOYED, Set.of(node1, node2, node3))))
        );

        assertThat(metastore.remove(id, version), willBe(true));
        assertThat(metastore.getNodeStatus(id, version, node1),
                willBe((UnitNodeStatus) null));
    }

    @Test
    public void findByNodeId() {
        String id1 = "id3";
        String id2 = "id4";
        Version version = Version.parseVersion("1.1.1");

        String node1 = "node1";
        String node2 = "node2";
        String node3 = "node3";

        assertThat(metastore.createClusterStatus(id1, version, Set.of(node1, node2, node3)), willBe(true));
        assertThat(metastore.createNodeStatus(id1, version, node1), willBe(true));
        assertThat(metastore.createNodeStatus(id1, version, node2), willBe(true));
        assertThat(metastore.createNodeStatus(id1, version, node3), willBe(true));

        assertThat(metastore.createClusterStatus(id2, version, Set.of(node1, node2, node3)), willBe(true));
        assertThat(metastore.updateClusterStatus(id2, version, DEPLOYED), willBe(true));
        assertThat(metastore.getClusterStatus(id2, version),
                willBe(new UnitClusterStatus(id2, version, DEPLOYED, Set.of(node1, node2, node3))));

        assertThat(metastore.createNodeStatus(id2, version, node1), willBe(true));
        assertThat(metastore.createNodeStatus(id2, version, node2), willBe(true));
        assertThat(metastore.createNodeStatus(id2, version, node3), willBe(true));

        assertThat(metastore.findAllByNodeConsistentId(node1), willBe(Collections.emptyList()));

        assertThat(metastore.updateNodeStatus(id1, version, node1, DEPLOYED), willBe(true));
        assertThat(metastore.findAllByNodeConsistentId(node1),
                willBe(equalTo(List.of(new UnitClusterStatus(id1, version, UPLOADING, Set.of(node1, node2, node3)))
        )));

        assertThat(metastore.updateNodeStatus(id2, version, node1, DEPLOYED), willBe(true));
        assertThat(metastore.findAllByNodeConsistentId(node1), willBe(containsInAnyOrder(
                new UnitClusterStatus(id1, version, UPLOADING, Set.of(node1, node2, node3)),
                new UnitClusterStatus(id2, version, DEPLOYED, Set.of(node1, node2, node3))
        )));
    }

    @Test
    public void testNodeEventListener() {
        String id = "id5";
        Version version = Version.parseVersion("1.1.1");
        String node1 = "node1";

        assertThat(metastore.createNodeStatus(id, version, node1), willBe(true));
        assertThat(metastore.updateNodeStatus(id, version, node1, DEPLOYED), willBe(true));
        assertThat(metastore.updateNodeStatus(id, version, node1, OBSOLETE), willBe(true));
        assertThat(metastore.updateNodeStatus(id, version, node1, REMOVING), willBe(true));


        assertThat(history, equalTo(
                List.of(
                        new UnitNodeStatus(id, version, UPLOADING),
                        new UnitNodeStatus(id, version, DEPLOYED),
                        new UnitNodeStatus(id, version, OBSOLETE),
                        new UnitNodeStatus(id, version, REMOVING)
                )
        ));

    }
}
