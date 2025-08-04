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
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.ignite.catalog.ColumnType;
import org.apache.ignite.catalog.definitions.TableDefinition;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.compute.BroadcastJobTarget;
import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobTarget;
import org.apache.ignite.compute.TaskDescriptor;
import org.apache.ignite.internal.runner.app.Jobs.ArgMarshallingJob;
import org.apache.ignite.internal.runner.app.Jobs.ArgumentAndResultMarshallingJob;
import org.apache.ignite.internal.runner.app.Jobs.ArgumentStringListMarshaller;
import org.apache.ignite.internal.runner.app.Jobs.ArgumentStringMarshaller;
import org.apache.ignite.internal.runner.app.Jobs.JsonMarshaller;
import org.apache.ignite.internal.runner.app.Jobs.MapReduce;
import org.apache.ignite.internal.runner.app.Jobs.MapReduceTuples;
import org.apache.ignite.internal.runner.app.Jobs.PojoArg;
import org.apache.ignite.internal.runner.app.Jobs.PojoJobWithCustomMarshallers;
import org.apache.ignite.internal.runner.app.Jobs.PojoResult;
import org.apache.ignite.internal.runner.app.Jobs.ResultMarshallingJob;
import org.apache.ignite.internal.runner.app.Jobs.ResultStringListUnMarshaller;
import org.apache.ignite.internal.runner.app.Jobs.ResultStringUnMarshaller;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.table.Tuple;
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
        assertNotNull(nodes.get(0).id());

        assertEquals("itccmt_n_3345", nodes.get(1).name());
        assertEquals(3345, nodes.get(1).address().port());
        assertNotNull(nodes.get(1).id());
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1})
    void customArgMarshaller(int targetNodeIdx) {
        // Given target node.
        var targetNode = node(targetNodeIdx);

        // When run job with custom marshaller for string argument.
        String result = client().compute().execute(
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
        // Given target node.
        var targetNode = node(targetNodeIdx);

        // When run job with custom marshaller for string result.
        String result = client().compute().execute(
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
        // Given target node.
        var targetNode = node(targetNodeIdx);

        // When run job with custom marshaller for string result.
        String result = client().compute().execute(
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

    @Test
    void customResultAndArgumentMarshallerExecutedOnDifferentNode() {
        // Given client node and target node.
        String clientAddress = getClientAddresses().get(0);
        ClusterNode targetNode = node(1);

        try (IgniteClient client = IgniteClient.builder().addresses(clientAddress).build()) {
            // When run job with custom marshaller for string result.
            String result = client.compute().execute(
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
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1})
    void pojoJobWithMarshallers(int targetNodeIdx) {
        // Given target node.
        var targetNode = node(targetNodeIdx);

        // When run job with custom marshaller for pojo argument and result.
        PojoResult result = client().compute().execute(
                JobTarget.node(targetNode),
                // The job accepts PojoArg and returns PojoResult and defines marshaller for both.
                JobDescriptor.builder(PojoJobWithCustomMarshallers.class)
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
    void executeBroadcast() {
        // When.
        Collection<String> result = client().compute().execute(
                BroadcastJobTarget.nodes(node(0), node(1)),
                JobDescriptor.builder(ArgumentAndResultMarshallingJob.class)
                        .argumentMarshaller(new ArgumentStringMarshaller())
                        .resultMarshaller(new ResultStringUnMarshaller())
                        .build(),
                "Input"
        );

        // Then.
        List<String> resultExpected = List.of(
                "Input:marshalledOnClient:unmarshalledOnServer:processedOnServer:marshalledOnServer:unmarshalledOnClient",
                "Input:marshalledOnClient:unmarshalledOnServer:processedOnServer:marshalledOnServer:unmarshalledOnClient"
        );

        assertEquals(resultExpected, result);
    }

    @Test
    void submitBroadcast() {
        // When.
        Map<String, String> result = client().compute().submitAsync(
                BroadcastJobTarget.nodes(node(0), node(1)),
                JobDescriptor.builder(ArgumentAndResultMarshallingJob.class)
                        .argumentMarshaller(new ArgumentStringMarshaller())
                        .resultMarshaller(new ResultStringUnMarshaller())
                        .build(),
                "Input"
        ).thenApply(broadcastExecution -> broadcastExecution.executions().stream().collect(
                Collectors.toMap(execution -> execution.node().name(), ItThinClientComputeMarshallingTest::extractResult, (v, i) -> v)
        )).join();

        // Then.
        Map<String, String> resultExpected = Map.of(
                node(0).name(), "Input:marshalledOnClient:unmarshalledOnServer:processedOnServer:marshalledOnServer:unmarshalledOnClient",
                node(1).name(), "Input:marshalledOnClient:unmarshalledOnServer:processedOnServer:marshalledOnServer:unmarshalledOnClient"
        );

        assertEquals(resultExpected, result);
    }

    private static String extractResult(JobExecution<String> e) {
        try {
            return e.resultAsync().get();
        } catch (InterruptedException | ExecutionException ex) {
            throw new RuntimeException(ex);
        }
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
        ).qualifiedName();

        // When run job with custom marshaller for string argument.
        var tup = Tuple.create().set("key", 1);

        String result = client().compute().execute(
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
    void mapReduce() {
        // When.
        List<String> result = client().compute().executeMapReduce(
                TaskDescriptor.builder(MapReduce.class)
                        .splitJobArgumentMarshaller(new ArgumentStringListMarshaller())
                        .reduceJobResultMarshaller(new ResultStringListUnMarshaller())
                        .build(),
                // input_0 goes to 0 node and input_1 goes to 1 node
                List.of("Input_0", "Input_1")
        );

        // Then.
        assertThat(result, contains(
                "Input_0"
                        + ":listMarshalledOnClient" // Split argument marshalled on the client
                        // Job argument is marshalled even if the target node is the same because we're using submit
                        + ":marshalledOnClient"
                        + ":unmarshalledOnServer" // Job argument unmarshalled on the target node
                        + ":processedOnServer" // Job processed on the target node
                        // TODO IGNITE-24183 Avoid job argument and result marshalling on local execution
                        + ":marshalledOnServer"
                        + ":unmarshalledOnClient"
                        + ":listMarshalledOnServer" // Reduce job result marshalled on the client handler node
                        + ":listUnmarshalledOnClient", // Reduce job result unmarshalled on the client
                "Input_1"
                        + ":listMarshalledOnClient" // Split argument marshalled on the client
                        + ":marshalledOnClient" // Job argument is marshalled on the client handler node
                        + ":unmarshalledOnServer" // Job argument unmarshalled on the target node
                        + ":processedOnServer" // Job processed on the target node
                        + ":marshalledOnServer" // Job result marshalled on the target node
                        + ":unmarshalledOnClient" // Job result unmarshalled on the client handler node
                        + ":listMarshalledOnServer" // Reduce job result marshalled on the client handler node
                        + ":listUnmarshalledOnClient" // Reduce job result unmarshalled on the client
        ));
    }

    @Test
    void mapReduceTuples() {
        // When.
        Tuple result = client().compute().executeMapReduce(
                TaskDescriptor.builder(MapReduceTuples.class).build(),
                Tuple.create().set("from", "client")
        );

        // Then.
        Tuple expectedTuple = Tuple.create();
        expectedTuple.set("from", "client");
        expectedTuple.set("split", "call");
        expectedTuple.set("reduce", "call");
        expectedTuple.set("echo", "echo");

        assertThat(result, equalTo(expectedTuple));
    }
}
