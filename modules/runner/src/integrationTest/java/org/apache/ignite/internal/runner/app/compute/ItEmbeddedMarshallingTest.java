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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.catalog.ColumnType;
import org.apache.ignite.catalog.definitions.TableDefinition;
import org.apache.ignite.compute.BroadcastJobTarget;
import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobTarget;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.runner.app.Jobs.ArgMarshallingJob;
import org.apache.ignite.internal.runner.app.Jobs.ArgumentAndResultMarshallingJob;
import org.apache.ignite.internal.runner.app.Jobs.ArgumentStringMarshaller;
import org.apache.ignite.internal.runner.app.Jobs.JsonMarshaller;
import org.apache.ignite.internal.runner.app.Jobs.PojoArg;
import org.apache.ignite.internal.runner.app.Jobs.PojoJob;
import org.apache.ignite.internal.runner.app.Jobs.PojoJobWithCustomMarshallers;
import org.apache.ignite.internal.runner.app.Jobs.PojoResult;
import org.apache.ignite.internal.runner.app.Jobs.ResultStringUnMarshaller;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.Test;

/**
 * Test for embedded API marshallers for Compute API.
 */
public class ItEmbeddedMarshallingTest extends ClusterPerClassIntegrationTest {
    @Test
    void pojoWithCustomMarshallersExecOnAnotherNode() {
        // Given entry node that are not supposed to execute job.
        Ignite node = node(0);
        // And another target node.
        ClusterNode targetNode = clusterNode(1);

        // When run job with custom marshaller for pojo argument and result but for embedded.
        PojoResult result = node.compute().execute(
                JobTarget.node(targetNode),
                JobDescriptor.builder(PojoJobWithCustomMarshallers.class)
                        .argumentMarshaller(new JsonMarshaller<>(PojoArg.class))
                        .resultMarshaller(new JsonMarshaller<>(PojoResult.class))
                        .build(),
                new PojoArg().setIntValue(2).setStrValue("1")
        );

        // Then the job returns the expected result.
        assertEquals(3L, result.getLongValue());
    }

    @Test
    void pojoWithCustomMarshallersExecOnSame() {
        // Given entry node.
        Ignite node = node(0);
        // And target node.
        ClusterNode targetNode = clusterNode(0);

        // When run job with custom marshaller for pojo argument and result but for embedded.
        PojoResult result = node.compute().execute(
                JobTarget.node(targetNode),
                JobDescriptor.builder(PojoJobWithCustomMarshallers.class)
                        .argumentMarshaller(new JsonMarshaller<>(PojoArg.class))
                        .resultMarshaller(new JsonMarshaller<>(PojoResult.class))
                        .build(),
                new PojoArg().setIntValue(2).setStrValue("1")
        );

        // Then the job returns the expected result.
        assertEquals(3L, result.getLongValue());
    }

    @Test
    void pojoExecOnAnotherNode() {
        // Given entry node that are not supposed to execute job.
        Ignite node = node(0);
        // And another target node.
        ClusterNode targetNode = clusterNode(1);

        // When run job with custom marshaller for pojo argument and result but for embedded.
        PojoResult result = node.compute().execute(
                JobTarget.node(targetNode),
                JobDescriptor.builder(PojoJob.class).resultClass(PojoResult.class).build(),
                new PojoArg().setIntValue(2).setStrValue("1")
        );

        // Then the job returns the expected result.
        assertEquals(3L, result.getLongValue());
    }

    @Test
    void pojoExecOnSame() {
        // Given entry node.
        Ignite node = node(0);
        // And target node.
        ClusterNode targetNode = clusterNode(0);

        // When run job with custom marshaller for pojo argument and result but for embedded.
        PojoResult result = node.compute().execute(
                JobTarget.node(targetNode),
                JobDescriptor.builder(PojoJob.class).resultClass(PojoResult.class).build(),
                new PojoArg().setIntValue(2).setStrValue("1")
        );

        // Then the job returns the expected result.
        assertEquals(3L, result.getLongValue());
    }

    @Test
    void local() {
        // Given entry node.
        Ignite node = node(0);

        // When.
        String result = node.compute().execute(
                JobTarget.node(clusterNode(0)),
                JobDescriptor.builder(ArgumentAndResultMarshallingJob.class)
                        .argumentMarshaller(new ArgumentStringMarshaller())
                        .resultMarshaller(new ResultStringUnMarshaller())
                        .build(),
                "Input"
        );

        // TODO IGNITE-24183 Avoid job argument and result marshalling on local execution
        assertEquals("Input:marshalledOnClient:unmarshalledOnServer:processedOnServer:marshalledOnServer:unmarshalledOnClient", result);
    }

    @Test
    void broadcastExecute() {
        // Given entry node.
        Ignite node = node(0);

        // When.
        Collection<String> result = node.compute().execute(
                BroadcastJobTarget.nodes(clusterNode(0), clusterNode(1)),
                JobDescriptor.builder(ArgumentAndResultMarshallingJob.class)
                        .argumentMarshaller(new ArgumentStringMarshaller())
                        .resultMarshaller(new ResultStringUnMarshaller())
                        .build(),
                "Input"
        );

        // Then.
        // TODO IGNITE-24183 Avoid job argument and result marshalling on local execution
        assertThat(result, containsInAnyOrder(
                // todo: "https://issues.apache.org/jira/browse/IGNITE-23024"
                "Input:marshalledOnClient:unmarshalledOnServer:processedOnServer:marshalledOnServer:unmarshalledOnClient",
                "Input:marshalledOnClient:unmarshalledOnServer:processedOnServer:marshalledOnServer:unmarshalledOnClient"
        ));
    }

    @Test
    void broadcastSubmit() {
        // Given entry node.
        Ignite node = node(0);

        // When.
        Map<String, String> result = node.compute().submitAsync(
                BroadcastJobTarget.nodes(clusterNode(0), clusterNode(1)),
                JobDescriptor.builder(ArgumentAndResultMarshallingJob.class)
                        .argumentMarshaller(new ArgumentStringMarshaller())
                        .resultMarshaller(new ResultStringUnMarshaller())
                        .build(),
                "Input"
        ).thenApply(broadcastExecution -> broadcastExecution.executions().stream().collect(
                Collectors.toMap(execution -> execution.node().name(), ItEmbeddedMarshallingTest::extractResult, (v, i) -> v)
        )).join();

        // Then.
        // TODO IGNITE-24183 Avoid job argument and result marshalling on local execution
        Map<String, String> resultExpected = Map.of(
                // todo: "https://issues.apache.org/jira/browse/IGNITE-23024"
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
        var node = node(0);
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

}
