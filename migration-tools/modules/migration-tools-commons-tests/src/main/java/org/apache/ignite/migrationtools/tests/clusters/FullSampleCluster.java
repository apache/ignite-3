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

package org.apache.ignite.migrationtools.tests.clusters;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.migrationtools.tests.containers.Ignite2ClusterContainer;
import org.apache.ignite.migrationtools.tests.containers.Ignite2ClusterWithSamples;
import org.jetbrains.annotations.Nullable;
import org.testcontainers.utility.MountableFile;

/** Cluster with all the samples from all the caches. */
public class FullSampleCluster extends Ignite2ClusterWithSamples {
    public static final Path SAMPLE_CLUSTERS_PATH = Path.of("../../resources/sample-clusters");

    public static final Path CLUSTER_CFG_PATH = SAMPLE_CLUSTERS_PATH.resolve("example-persistent-store.xml");

    public static final Path TEST_CLUSTER_PATH = SAMPLE_CLUSTERS_PATH.resolve("test-cluster");

    public static final List<String> clusterNodeIds = List.of(
            "ad26bff6-5ff5-49f1-9a61-425a827953ed",
            "c1099d16-e7d7-49f4-925c-53329286c444",
            "7b880b69-8a9e-4b84-b555-250d365e2e67"
    );

    public FullSampleCluster() {
        super(TEST_CLUSTER_PATH);
    }

    @Override
    protected Ignite2ClusterContainer createClusterContainers() {
        var cluster = new Ignite2ClusterContainer(
                CLUSTER_CFG_PATH,
                TEST_CLUSTER_PATH,
                clusterNodeIds
        );

        List<String> dependencies;
        @Nullable InputStream rs = FullSampleCluster.class.getResourceAsStream("/fullsamplecluster");
        if (rs == null) {
            throw new IllegalStateException("Could not find required resource for loading dependencies.");
        }

        try (
                rs;
                InputStreamReader irs = new InputStreamReader(rs, UTF_8);
                BufferedReader r = new BufferedReader(irs)
        ) {
            dependencies = r.lines().map(String::trim).collect(Collectors.toList());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        for (var path : dependencies) {
            cluster.withFileInClasspath(MountableFile.forHostPath(path));
        }

        return cluster;
    }
}
