/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.migrationtools.tests.utils;

import org.apache.ignite.migrationtools.tests.containers.Ignite3ClusterContainer;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

/** RunIgnite3Cluster. */
@Testcontainers
public class RunIgnite3Cluster {

    private final Ignite3ClusterContainer cluster = new Ignite3ClusterContainer();

    @Test
    void runIgnite3() {
        try (cluster) {
            cluster.start();
            System.out.println("Cluster started. Feel free to pause and debug.");
        }
    }
}
