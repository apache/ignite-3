/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.cluster.management.rest;

import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.runtime.server.EmbeddedServer;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.MockNode;
import org.apache.ignite.internal.rest.api.Problem;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.StaticNodeFinder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Cluster management REST test.
 */
@MicronautTest
@ExtendWith(WorkDirectoryExtension.class)
public class RestTestBase {

    static final int PORT_BASE = 10000;

    static final List<MockNode> cluster = new ArrayList<>();

    static ClusterService clusterService;

    static ClusterManagementGroupManager clusterManager;

    @WorkDirectory
    static Path workDir;

    @Inject
    EmbeddedServer server;

    @BeforeAll
    static void setUp(TestInfo testInfo) throws IOException {
        var addr1 = new NetworkAddress("localhost", PORT_BASE);
        var addr2 = new NetworkAddress("localhost", PORT_BASE + 1);

        var nodeFinder = new StaticNodeFinder(List.of(addr1, addr2));

        cluster.add(new MockNode(testInfo, addr1, nodeFinder, workDir.resolve("node0")));
        cluster.add(new MockNode(testInfo, addr2, nodeFinder, workDir.resolve("node1")));

        for (MockNode node : cluster) {
            node.start();
        }

        clusterService = cluster.get(0).clusterService();
        clusterManager = cluster.get(0).clusterManager();
    }

    @AfterAll
    static void tearDown() {
        for (MockNode node : cluster) {
            node.beforeNodeStop();
        }

        for (MockNode node : cluster) {
            node.stop();
        }
    }

    Problem getProblem(HttpClientResponseException exception) {
        return exception.getResponse().getBody(Problem.class).orElseThrow();
    }
}
