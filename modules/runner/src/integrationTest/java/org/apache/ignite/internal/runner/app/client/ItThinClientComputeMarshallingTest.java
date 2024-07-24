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

package org.apache.ignite.internal.runner.app.client;

import static org.apache.ignite.catalog.definitions.ColumnDefinition.column;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.catalog.ColumnType;
import org.apache.ignite.catalog.definitions.TableDefinition;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.compute.JobTarget;
import org.apache.ignite.internal.runner.app.Jobs.ArgMarshallingJob;
import org.apache.ignite.internal.runner.app.Jobs.ArgumentAndResultMarshallingJob;
import org.apache.ignite.internal.runner.app.Jobs.ArgumentStringMarshaller;
import org.apache.ignite.internal.runner.app.Jobs.JsonMarshaller;
import org.apache.ignite.internal.runner.app.Jobs.MapReduce;
import org.apache.ignite.internal.runner.app.Jobs.PojoArg;
import org.apache.ignite.internal.runner.app.Jobs.PojoJob;
import org.apache.ignite.internal.runner.app.Jobs.PojoResult;
import org.apache.ignite.internal.runner.app.Jobs.ResultMarshallingJob;
import org.apache.ignite.internal.runner.app.Jobs.ResultStringUnMarshaller;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Test for client marshaller for Compute API.
 */
@SuppressWarnings("resource")
public class ItThinClientComputeMarshallingTest extends ItAbstractThinClientTest {
    @Test
    void testClusterNodes() {
        List<ClusterNode> nodes = sortedNodes();

        assertEquals(2, nodes.size());

        assertEquals("itccmt_n_3344", nodes.get(0).name());
        assertEquals(3344, nodes.get(0).address().port());
        assertTrue(nodes.get(0).id().length() > 10);

        assertEquals("itccmt_n_3345", nodes.get(1).name());
        assertEquals(3345, nodes.get(1).address().port());
        assertTrue(nodes.get(1).id().length() > 10);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1})
    void customArgMarshaller(int targetNodeIdx) {
        // Given entry node for client.
        var node = server(0);
        // And.
        var targetNode = node(targetNodeIdx);

        // When run job with custom marshaller for string argument.
        String result = computeClientOn(node).execute(
                JobTarget.node(targetNode),
                // Accepts string argument and defines marshaller for it.
                JobDescriptor.builder(ArgMarshallingJob.class)
                        // If marshaller is defined for job, we define it on the client as well.
                        .argumentMarshaller(new ArgumentStringMarshaller())
                        .build(),
                "Input"
        );

        // Then both client and server marshaller were called.
        assertEquals("Input"
                        + ":marshalledOnClient"
                        + ":unmarshalledOnServer"
                        + ":processedOnServer",
                result
        );
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1})
    void customResultMarshaller(int targetNodeIdx) {
        // Given entry node for client.
        var node = server(0);
        // And.
        var targetNode = node(targetNodeIdx);

        // When run job with custom marshaller for string result.
        String result = computeClientOn(node).execute(
                JobTarget.node(targetNode),
                // Returns string result and defines marshaller for it.
                JobDescriptor.builder(ResultMarshallingJob.class)
                        // If result marshaller is defined for job, we define it on the client as well.
                        .resultMarshaller(new ResultStringUnMarshaller())
                        .build(),
                "Input"
        );

        // Then both client and server marshaller were called.
        assertEquals("Input"
                        + ":processedOnServer"
                        + ":marshalledOnServer"
                        + ":unmarshalledOnClient",
                result
        );
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1})
    void customResultAndArgumentMarshallerExecutedOnSameNode(int targetNodeIdx) {
        // Given entry node for client.
        var node = server(0);
        // And.
        var targetNode = node(targetNodeIdx);

        // When run job with custom marshaller for string result.
        String result = computeClientOn(node).execute(
                JobTarget.node(targetNode),
                // The job defines custom marshaller for both argument and result.
                JobDescriptor.builder(ArgumentAndResultMarshallingJob.class)
                        // The client must define both marshaller as well.
                        .argumentMarshaller(new ArgumentStringMarshaller())
                        .resultMarshaller(new ResultStringUnMarshaller())
                        .build(),
                "Input"
        );

        // Then both client and server marshaller were called for argument and for result.
        assertEquals("Input"
                        + ":marshalledOnClient"
                        + ":unmarshalledOnServer"
                        + ":processedOnServer"
                        + ":marshalledOnServer"
                        + ":unmarshalledOnClient",
                result
        );
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1})
    void pojoJobWithMarshallers(int targetNodeIdx) {
        // Given entry node for client.
        var node = server(0);
        // And.
        var targetNode = node(targetNodeIdx);

        // When run job with custom marshaller for pojo argument and result.
        PojoResult result = computeClientOn(node).execute(
                JobTarget.node(targetNode),
                // The job accepts PojoArg and returns PojoResult and defines marshaller for both.
                JobDescriptor.builder(PojoJob.class)
                        // The client must define both marshaller as well.
                        .argumentMarshaller(new JsonMarshaller<>(PojoArg.class))
                        .resultMarshaller(new JsonMarshaller<>(PojoResult.class))
                        .build(),
                new PojoArg().setIntValue(2).setStrValue("1")
        );

        // Then the job returns the expected result.
        assertEquals(3L, result.getLongValue());
    }

    @Test
    void broadcast() {
        // Given entry node.
        var node = server(0);

        // When.
        Map<ClusterNode, String> result = computeClientOn(node).executeBroadcast(
                Set.of(node(0), node(1)),
                JobDescriptor.builder(ArgumentAndResultMarshallingJob.class)
                        .argumentMarshaller(new ArgumentStringMarshaller())
                        .resultMarshaller(new ResultStringUnMarshaller())
                        .build(),
                "Input"
        );

        // Then.
        Map<ClusterNode, String> resultExpected = Map.of(
                node(0), "Input:marshalledOnClient:unmarshalledOnServer:processedOnServer:marshalledOnServer:unmarshalledOnClient",
                node(1), "Input:marshalledOnClient:unmarshalledOnServer:processedOnServer:marshalledOnServer:unmarshalledOnClient"
        );

        assertEquals(resultExpected, result);
    }

    @Test
    void colocated() {
        // Given entry node.
        var node = server(0);
        // And table API.
        var tableName = node.catalog().createTable(
                TableDefinition.builder("test")
                        .primaryKey("key")
                        .columns(
                                column("key", ColumnType.INT32),
                                column("v", ColumnType.INT32)
                        )
                        .build()
        ).name();

        // When run job with custom marshaller for string argument.
        var tup = Tuple.create().set("key", 1);

        var compute = computeClientOn(node);
        String result = compute.execute(
                JobTarget.colocated(tableName, tup),
                JobDescriptor.builder(ArgMarshallingJob.class)
                        .argumentMarshaller(new ArgumentStringMarshaller())
                        .build(),
                "Input"
        );

        // Then both client and server marshaller were called.
        assertEquals("Input"
                        + ":marshalledOnClient"
                        + ":unmarshalledOnServer"
                        + ":processedOnServer",
                result
        );
    }

    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-22787")
    void mapReduce() {
        // Given entry node.
        var node = server(0);

        // When run job with custom marshaller for string argument.
        var compute = computeClientOn(node);
        String result = compute.executeMapReduce(
                List.of(), MapReduce.class.getName(), List.of("Input_0", "Input_1")
        );

        // Then both client and server marshaller were called.
        assertEquals("Input"
                        + ":marshalledOnClient"
                        + ":unmarshalledOnServer"
                        + ":processedOnServer",
                result
        );
    }

    private static IgniteCompute computeClientOn(Ignite node) {
        return IgniteClient.builder()
                .addresses(getClientAddresses(List.of(node)).toArray(new String[0]))
                .build()
                .compute();
    }

    private ClusterNode node(int idx) {
        return sortedNodes().get(idx);
    }

    private List<ClusterNode> sortedNodes() {
        return client().clusterNodes().stream()
                .sorted(Comparator.comparing(ClusterNode::name))
                .collect(Collectors.toList());
    }
}
