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

package org.apache.ignite.internal.rest.cluster;

import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.runtime.server.EmbeddedServer;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.cluster.management.BaseItClusterManagementTest;
import org.apache.ignite.internal.cluster.management.ClusterInitializer;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.MockNode;
import org.apache.ignite.internal.rest.api.Problem;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.network.ClusterService;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Cluster management REST test.
 */
@MicronautTest
@ExtendWith(WorkDirectoryExtension.class)
public abstract class RestTestBase extends BaseItClusterManagementTest {
    static final List<MockNode> cluster = new ArrayList<>();

    static ClusterService clusterService;

    static ClusterInitializer clusterInitializer;

    static ClusterManagementGroupManager clusterManager;

    @WorkDirectory
    static Path workDir;

    @Inject
    EmbeddedServer server;

    @BeforeAll
    static void setUp(TestInfo testInfo) {
        cluster.addAll(createNodes(2, testInfo, workDir));

        cluster.parallelStream().forEach(MockNode::start);

        clusterService = cluster.get(0).clusterService();
        clusterInitializer = cluster.get(0).clusterInitializer();
        clusterManager = cluster.get(0).clusterManager();
    }

    @AfterAll
    static void tearDown() throws Exception {
        stopNodes(cluster);
    }

    static Problem getProblem(HttpClientResponseException exception) {
        return exception.getResponse().getBody(Problem.class).orElseThrow();
    }
}
