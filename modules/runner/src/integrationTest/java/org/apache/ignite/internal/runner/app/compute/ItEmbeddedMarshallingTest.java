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

package org.apache.ignite.internal.runner.app.compute;

import static org.apache.ignite.catalog.definitions.ColumnDefinition.column;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.ignite.catalog.ColumnType;
import org.apache.ignite.catalog.definitions.TableDefinition;
import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobTarget;
import org.apache.ignite.internal.runner.app.Jobs.ArgMarshallingJob;
import org.apache.ignite.internal.runner.app.Jobs.ArgumentAndResultMarshallingJob;
import org.apache.ignite.internal.runner.app.Jobs.ArgumentStringMarshaller;
import org.apache.ignite.internal.runner.app.Jobs.JsonMarshaller;
import org.apache.ignite.internal.runner.app.Jobs.PojoArg;
import org.apache.ignite.internal.runner.app.Jobs.PojoJob;
import org.apache.ignite.internal.runner.app.Jobs.PojoResult;
import org.apache.ignite.internal.runner.app.Jobs.ResultStringUnMarshaller;
import org.apache.ignite.internal.runner.app.client.ItAbstractThinClientTest;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.Test;

/**
 * Test for embedded API marshallers for Compute API.
 */
@SuppressWarnings("resource")
public class ItEmbeddedMarshallingTest extends ItAbstractThinClientTest {
    @Test
    void embeddedExecOnAnotherNode() {
        // Given entry node that are not supposed to execute job.
        var node = server(0);
        // And another target node.
        var targetNode = node(1);

        // When run job with custom marshaller for pojo argument and result but for embedded.
        var embeddedCompute = node.compute();
        PojoResult result = embeddedCompute.execute(
                JobTarget.node(targetNode),
                JobDescriptor.builder(PojoJob.class)
                        .argumentMarshaller(new JsonMarshaller<>(PojoArg.class))
                        .resultMarshaller(new JsonMarshaller<>(PojoResult.class))
                        .build(),
                new PojoArg().setIntValue(2).setStrValue("1")
        );

        // Then the job returns the expected result.
        assertEquals(3L, result.getLongValue());
    }

    @Test
    void embeddedExecOnSame() {
        // Given entry node.
        var node = server(0);
        // And target node.
        var targetNode = node(0);

        // When run job with custom marshaller for pojo argument and result but for embedded.
        var embeddedCompute = node.compute();
        PojoResult result = embeddedCompute.execute(
                JobTarget.node(targetNode),
                JobDescriptor.builder(PojoJob.class)
                        .argumentMarshaller(new JsonMarshaller<>(PojoArg.class))
                        .resultMarshaller(new JsonMarshaller<>(PojoResult.class))
                        .build(),
                new PojoArg().setIntValue(2).setStrValue("1")
        );

        // Then the job returns the expected result.
        assertEquals(3L, result.getLongValue());
    }

    @Test
    void broadcastExecute() {
        // Given entry node.
        var node = server(0);

        // When.
        Map<ClusterNode, String> result = node.compute().executeBroadcast(
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
    void broadcastSubmit() {
        // Given entry node.
        var node = server(0);

        // When.
        Map<ClusterNode, String> result = node.compute().submitBroadcast(
                Set.of(node(0), node(1)),
                JobDescriptor.builder(ArgumentAndResultMarshallingJob.class)
                        .argumentMarshaller(new ArgumentStringMarshaller())
                        .resultMarshaller(new ResultStringUnMarshaller())
                        .build(),
                "Input"
        ).entrySet().stream().collect(
                Collectors.toMap(Entry::getKey, ItEmbeddedMarshallingTest::extractResult, (v, i) -> v)
        );

        // Then.
        Map<ClusterNode, String> resultExpected = Map.of(
                node(0), "Input:marshalledOnClient:unmarshalledOnServer:processedOnServer:marshalledOnServer:unmarshalledOnClient",
                node(1), "Input:marshalledOnClient:unmarshalledOnServer:processedOnServer:marshalledOnServer:unmarshalledOnClient"
        );

        assertEquals(resultExpected, result);
    }

    private static String extractResult(Entry<ClusterNode, JobExecution<String>> e) {
        try {
            return e.getValue().resultAsync().get();
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
        ).name();

        // When run job with custom marshaller for string argument.
        var tup = Tuple.create().set("key", 1);

        String result = node.compute().execute(
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

    private ClusterNode node(int idx) {
        return sortedNodes().get(idx);
    }

    private List<ClusterNode> sortedNodes() {
        return client().clusterNodes().stream()
                .sorted(Comparator.comparing(ClusterNode::name))
                .collect(Collectors.toList());
    }
}
