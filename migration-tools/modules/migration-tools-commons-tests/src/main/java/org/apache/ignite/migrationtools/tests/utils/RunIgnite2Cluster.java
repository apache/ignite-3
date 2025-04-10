/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.migrationtools.tests.utils;

import org.apache.ignite.migrationtools.tests.clusters.FullSampleCluster;
import org.apache.ignite.migrationtools.tests.containers.Ignite2ClusterContainer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.junit.jupiter.Testcontainers;

/** RunIgnite2Cluster. */
@ExtendWith(FullSampleCluster.class)
@Testcontainers
public class RunIgnite2Cluster {
    private final Ignite2ClusterContainer cluster = new Ignite2ClusterContainer(
            FullSampleCluster.CLUSTER_CFG_PATH,
            FullSampleCluster.TEST_CLUSTER_PATH,
            FullSampleCluster.clusterNodeIds
    );

    @Test
    void runIgnite2ContainerWithSamples() {
        try (cluster) {
            cluster.start();
            System.out.println("Cluster started. Feel free to pause and debug.");
        }
    }
}
