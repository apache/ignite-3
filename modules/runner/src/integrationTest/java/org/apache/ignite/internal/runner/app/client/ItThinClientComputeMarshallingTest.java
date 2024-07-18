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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.compute.JobTarget;
import org.apache.ignite.internal.runner.app.client.Jobs.ArgMarshalingJob;
import org.apache.ignite.internal.runner.app.client.Jobs.ArgumentAndResultMarshalingJob;
import org.apache.ignite.internal.runner.app.client.Jobs.ArgumentStringMarshaller;
import org.apache.ignite.internal.runner.app.client.Jobs.JsonMarshaller;
import org.apache.ignite.internal.runner.app.client.Jobs.PojoArg;
import org.apache.ignite.internal.runner.app.client.Jobs.PojoJob;
import org.apache.ignite.internal.runner.app.client.Jobs.PojoResult;
import org.apache.ignite.internal.runner.app.client.Jobs.ResultMarshalingJob;
import org.apache.ignite.internal.runner.app.client.Jobs.ResultStringUnMarshaller;
import org.apache.ignite.network.ClusterNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Test for client marshalers for Compute API.
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
    void customArgMarshaller(int workerNodeIdx) {
        // Given entry node that are not supposed to execute job.
        var node = server(0);
        // And.
        var targetNode = node(workerNodeIdx);

        // When run job with custom marshaller for string argument.
        var compute = computeClientOn(node);
        String result = compute.execute(
                JobTarget.node(targetNode),
                JobDescriptor.builder(ArgMarshalingJob.class)
                        .argumentMarshaller(new ArgumentStringMarshaller())
                        .build(),
                "Input"
        );

        // Then both client and server marshaler were called.
        assertEquals("Input"
                        + ":marshalledOnClient"
                        + ":unmarshalledOnServer"
                        + ":processedOnServer",
                result
        );
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1})
    void customResultMarshaller(int workerNodeIdx) {
        // Given entry node that are not supposed to execute job.
        var node = server(0);
        // And another target node.
        var targetNode = node(workerNodeIdx);

        // When run job with custom marshaller for string result.
        var compute = computeClientOn(node);
        String result = compute.execute(
                JobTarget.node(targetNode),
                JobDescriptor.builder(ResultMarshalingJob.class)
                        .resultMarshaller(new ResultStringUnMarshaller())
                        .build(),
                "Input"
        );

        // Then both client and server marshaler were called.
        assertEquals("Input"
                        + ":processedOnServer"
                        + ":marshalledOnServer"
                        + ":unmarshalledOnClient",
                result
        );
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1})
    void customResultAndArgumentMarshallerExecutedOnSameNode(int workerNodeIdx) {
        // Given entry node that are not supposed to execute job.
        var node = server(0);
        // And another target node.
        var targetNode = node(workerNodeIdx);

        // When run job with custom marshaller for string result.
        var compute = computeClientOn(node);
        String result = compute.execute(
                JobTarget.node(targetNode),
                JobDescriptor.builder(ArgumentAndResultMarshalingJob.class)
                        .argumentMarshaller(new ArgumentStringMarshaller())
                        .resultMarshaller(new ResultStringUnMarshaller())
                        .build(),
                "Input"
        );

        // Then both client and server marshaler were called.
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
    void pojoJobWithMarshalers(int workerNodeIdx) {
        // Given entry node that are not supposed to execute job.
        var node = server(0);
        // And another target node.
        var targetNode = node(workerNodeIdx);

        // When run job with custom marshaller for string result.
        var compute = computeClientOn(node);
        PojoResult result = compute.execute(
                JobTarget.node(targetNode),
                JobDescriptor.builder(PojoJob.class)
                        .argumentMarshaller(new JsonMarshaller<>(PojoArg.class))
                        .resultMarshaller(new JsonMarshaller<>(PojoResult.class))
                        .build(),
                new PojoArg().setIntValue(2).setStrValue("1")
        );

        assertEquals(3L, result.longValue);
    }

    private IgniteCompute computeClientOn(Ignite node) {
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
