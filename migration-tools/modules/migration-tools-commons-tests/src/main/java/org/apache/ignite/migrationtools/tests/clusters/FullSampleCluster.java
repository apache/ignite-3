/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.migrationtools.tests.clusters;

import java.nio.file.Path;
import java.util.List;
import org.apache.ignite.migrationtools.tests.containers.Ignite2ClusterContainer;
import org.apache.ignite.migrationtools.tests.containers.Ignite2ClusterWithSamples;

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
        return new Ignite2ClusterContainer(
                FullSampleCluster.CLUSTER_CFG_PATH,
                FullSampleCluster.TEST_CLUSTER_PATH,
                FullSampleCluster.clusterNodeIds
        );
    }
}
