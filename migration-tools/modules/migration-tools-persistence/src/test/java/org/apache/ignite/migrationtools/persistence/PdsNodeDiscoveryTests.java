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

package org.apache.ignite.migrationtools.persistence;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.migrationtools.tests.clusters.FullSampleCluster;
import org.apache.ignite.migrationtools.tests.containers.Ignite2ClusterContainer;
import org.assertj.core.groups.Tuple;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/** PdsNodeDiscoveryTests. */
@ExtendWith(FullSampleCluster.class)
public class PdsNodeDiscoveryTests {

    @Test
    void checkSampleClusterNodes() {
        var cfg = new IgniteConfiguration()
                .setWorkDirectory(FullSampleCluster.TEST_CLUSTER_PATH.toString())
                .setDataStorageConfiguration(new DataStorageConfiguration());
        var candidates = Ignite2PersistenceTools.nodeFolderCandidates(cfg);
        assertThat(candidates)
                .extracting(e -> e.consistentId().toString(), e -> e.subFolderFile().getName())
                .containsExactlyInAnyOrder(
                        Tuple.tuple("7b880b69-8a9e-4b84-b555-250d365e2e67", "7b880b69_8a9e_4b84_b555_250d365e2e67"),
                        Tuple.tuple("ad26bff6-5ff5-49f1-9a61-425a827953ed", "ad26bff6_5ff5_49f1_9a61_425a827953ed"),
                        Tuple.tuple("c1099d16-e7d7-49f4-925c-53329286c444", "c1099d16_e7d7_49f4_925c_53329286c444")
                );
    }

    @Nested
    class NonUuidConsistentIds {
        private final Path clusterFolder = FullSampleCluster.SAMPLE_CLUSTERS_PATH.resolve(this.getClass().getSimpleName());

        private final List<String> nodeConsistentIds = List.of("node1", "node2", "node3");

        @BeforeEach
        void setupCluster() {
            if (Files.notExists(clusterFolder)) {
                var cluster = new Ignite2ClusterContainer(
                        FullSampleCluster.CLUSTER_CFG_PATH,
                        clusterFolder,
                        nodeConsistentIds);

                try {
                    cluster.start();
                } finally {
                    cluster.doStop(false);
                }
            }
        }

        @Test
        void checkClusterNodeNames() {
            var cfg = new IgniteConfiguration()
                    .setWorkDirectory(clusterFolder.toString())
                    .setDataStorageConfiguration(new DataStorageConfiguration());

            List<Ignite2PersistenceTools.NodeFolderDescriptor> candidates = Ignite2PersistenceTools.nodeFolderCandidates(cfg);

            assertThat(candidates)
                    .extracting(Ignite2PersistenceTools.NodeFolderDescriptor::consistentId)
                    .containsExactlyInAnyOrderElementsOf(nodeConsistentIds);
        }
    }
}
