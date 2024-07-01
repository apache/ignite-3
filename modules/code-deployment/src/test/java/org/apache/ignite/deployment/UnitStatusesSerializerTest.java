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

package org.apache.ignite.deployment;

import static org.apache.ignite.internal.deployunit.DeploymentStatus.UPLOADING;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.deployment.version.Version;
import org.apache.ignite.internal.deployunit.DeploymentStatus;
import org.apache.ignite.internal.deployunit.metastore.status.SerializeUtils;
import org.apache.ignite.internal.deployunit.metastore.status.UnitClusterStatus;
import org.apache.ignite.internal.deployunit.metastore.status.UnitNodeStatus;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Test for {@link SerializeUtils}.
 */
public class UnitStatusesSerializerTest {
    private static List<Arguments> nodeStatusProvider() {
        return List.of(
                arguments(null, null, null, null, null),
                arguments("id", null, null, UUID.randomUUID(), null),
                arguments("id", Version.LATEST, null, UUID.randomUUID(), null),
                arguments("id", Version.LATEST, UPLOADING, UUID.randomUUID(), "node1")
        );
    }

    private static List<Arguments> clusterStatusProvider() {
        return List.of(
                arguments("id", Version.LATEST, UPLOADING, null, Set.of()),
                arguments("id", Version.LATEST, UPLOADING, UUID.randomUUID(), Set.of("node1")),
                arguments("id", Version.LATEST, UPLOADING, UUID.randomUUID(), Set.of("node1", "node2")),
                arguments("id", Version.LATEST, UPLOADING, UUID.randomUUID(), Set.of("node1", "node2", "node3"))
        );
    }

    @ParameterizedTest
    @MethodSource("nodeStatusProvider")
    public void testSerializeDeserializeNodeStatus(
            String id,
            Version version,
            DeploymentStatus status,
            UUID opId,
            String nodeId
    ) {
        UnitNodeStatus nodeStatus = new UnitNodeStatus(id, version, status, opId, nodeId);

        byte[] serialize = UnitNodeStatus.serialize(nodeStatus);

        assertThat(UnitNodeStatus.deserialize(serialize), is(nodeStatus));
    }

    @ParameterizedTest
    @MethodSource("clusterStatusProvider")
    public void testSerializeDeserializeClusterStatus(
            String id,
            Version version,
            DeploymentStatus status,
            UUID opId,
            Set<String> consistentIdLocation
    ) {
        UnitClusterStatus nodeStatus = new UnitClusterStatus(id, version, status, opId, consistentIdLocation);

        byte[] serialize = UnitClusterStatus.serialize(nodeStatus);

        assertThat(UnitClusterStatus.deserialize(serialize), is(nodeStatus));
    }
}
