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

package org.apache.ignite.internal.sql.engine.exec.mapping;

import static org.apache.ignite.internal.sql.engine.SqlQueryType.QUERY;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrowFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedFast;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.sql.api.ResultSetMetadataImpl;
import org.apache.ignite.internal.sql.engine.prepare.MultiStepPlan;
import org.apache.ignite.internal.sql.engine.schema.IgniteSystemView;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Test class to verify {@link MappingServiceImpl}.
 */
public class MappingServiceImplTest extends BaseIgniteAbstractTest {
    @Test
    public void serviceInitializationTest() {
        String localNodeName = "NODE0";

        MappingServiceImpl mappingService = createMappingService(localNodeName, List.of(localNodeName));
        mappingService.onNodeJoined(Mockito.mock(LogicalNode.class), new LogicalTopologySnapshot(1, logicalNodes(localNodeName)));

        CompletableFuture<List<MappedFragment>> mappingFuture = mappingService
                .map(new MultiStepPlan(QUERY, List.of(), new ResultSetMetadataImpl(List.of())));

        assertThat(mappingFuture, willSucceedFast());
    }

    @Test
    public void lateServiceInitializationOnTopologyLeap() {
        String localNodeName = "NODE";
        List<String> nodeNames = List.of("NODE1");

        MappingServiceImpl mappingService = createMappingService(localNodeName, nodeNames);

        CompletableFuture<List<MappedFragment>> mappingFuture = mappingService
                .map(new MultiStepPlan(QUERY, List.of(), new ResultSetMetadataImpl(List.of())));

        // Mapping should wait for service initialization.
        assertFalse(mappingFuture.isDone());

        // Join another node affect nothing.
        mappingService.onTopologyLeap(new LogicalTopologySnapshot(1, logicalNodes("NODE1", "NODE2")));
        assertThat(mappingFuture, willThrowFast(TimeoutException.class));

        // Joining local node completes initialization.
        mappingService.onTopologyLeap(new LogicalTopologySnapshot(2, logicalNodes("NODE", "NODE1", "NODE2")));

        assertThat(mappingFuture, willSucceedFast());
        assertThat(mappingService.map(new MultiStepPlan(QUERY, List.of(), new ResultSetMetadataImpl(List.of()))), willSucceedFast());
    }

    @Test
    public void lateServiceInitializationOnNodeJoin() {
        String localNodeName = "NODE";
        List<String> nodeNames = List.of("NODE1");

        MappingServiceImpl mappingService = createMappingService(localNodeName, nodeNames);

        CompletableFuture<List<MappedFragment>> mappingFuture = mappingService
                .map(new MultiStepPlan(QUERY, List.of(), new ResultSetMetadataImpl(List.of())));

        // Mapping should wait for service initialization.
        assertFalse(mappingFuture.isDone());

        // Join another node affect nothing.
        mappingService.onNodeJoined(Mockito.mock(LogicalNode.class),
                new LogicalTopologySnapshot(1, logicalNodes("NODE1", "NODE2")));

        assertThat(mappingFuture, willThrowFast(TimeoutException.class));

        // Joining local node completes initialization.
        mappingService.onNodeJoined(Mockito.mock(LogicalNode.class),
                new LogicalTopologySnapshot(2, logicalNodes("NODE", "NODE1", "NODE2")));

        assertThat(mappingFuture, willSucceedFast());
        assertThat(mappingService.map(new MultiStepPlan(QUERY, List.of(), new ResultSetMetadataImpl(List.of()))), willSucceedFast());
    }

    private static List<LogicalNode> logicalNodes(String... nodeNames) {
        return Arrays.stream(nodeNames)
                .map(name -> new LogicalNode(name, name, NetworkAddress.from("127.0.0.1:10000")))
                .collect(Collectors.toList());
    }

    private static MappingServiceImpl createMappingService(String localNodeName, List<String> nodeNames) {
        var targetProvider = new ExecutionTargetProvider() {
            @Override
            public CompletableFuture<ExecutionTarget> forTable(ExecutionTargetFactory factory, IgniteTable table) {
                return CompletableFuture.completedFuture(factory.allOf(nodeNames));
            }

            @Override
            public CompletableFuture<ExecutionTarget> forSystemView(ExecutionTargetFactory factory, IgniteSystemView view) {
                return CompletableFuture.failedFuture(new AssertionError("Not supported"));
            }
        };

        return new MappingServiceImpl(localNodeName, targetProvider, Runnable::run);
    }
}
