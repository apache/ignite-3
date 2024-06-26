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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.compute.JobTarget;
import org.apache.ignite.marshaling.ByteArrayMarshaler;
import org.apache.ignite.marshaling.Marshaler;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;

/**
 * TBD.
 */
@SuppressWarnings("resource")
public class ItThinClientComputeMarshallingTest extends ItAbstractThinClientTest {
    /** Test trace id. */
    private static final UUID TRACE_ID = UUID.randomUUID();

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

    @Test
    void customArgMarshaller() {
        // Given entry node that are not supposed to execute job.
        var node = server(0);
        // And another target node.
        var targetNode = node(1);

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
        assertEquals("Input:marshalledOnClient:unmarshalledOnServer", result);
    }

    @Test
    void customResultMarshaller() {
        // Given entry node that are not supposed to execute job.
        var node = server(0);
        // And another target node.
        var targetNode = node(1);

        // When run job with custom marshaller for string argument.
        var compute = computeClientOn(node);
        String result = compute.execute(
                JobTarget.node(targetNode),
                JobDescriptor.builder(ResultMarshalingJob.class)
                        .resultMarshaller(new ResultStringMarshaller())
                        .build(),
                "Input"
        );

        assertEquals("Input:processedOnServer:marshalledOnServer:unmarshalledOnClient", result);
    }

    private IgniteCompute computeClientOn(Ignite node) {
        return IgniteClient.builder()
                .addresses(getClientAddresses(List.of(node)).toArray(new String[0]))
                .build()
                .compute();
    }

    static class ArgumentStringMarshaller implements ByteArrayMarshaler<String> {
        @Override
        public byte @Nullable [] marshal(@Nullable String object) {
            return ByteArrayMarshaler.super.marshal(object + ":marshalledOnClient");
        }
    }

    static class ResultStringMarshaller implements ByteArrayMarshaler<String> {
        @Override
        public @Nullable String unmarshal(byte @Nullable [] raw) {
            return ByteArrayMarshaler.super.unmarshal(raw) + ":unmarshalledOnClient";
        }
    }

    static class ArgMarshalingJob implements ComputeJob<String, String> {
        @Override
        public CompletableFuture<String> executeAsync(JobExecutionContext context, @Nullable String arg) {
            return completedFuture(arg);
        }

        @Override
        public Marshaler<String, byte[]> inputMarshaler() {
            return new ByteArrayMarshaler<>() {
                @Override
                public String unmarshal(byte @Nullable [] raw) {
                    return ByteArrayMarshaler.super.unmarshal(raw) + ":unmarshalledOnServer";
                }
            };
        }
    }

    static class ResultMarshalingJob implements ComputeJob<String, String> {

        @Override
        public CompletableFuture<String> executeAsync(JobExecutionContext context, @Nullable String arg) {
            return completedFuture(arg + ":processedOnServer");
        }

        @Override
        public Marshaler<String, byte[]> resultMarshaler() {
            return new ByteArrayMarshaler<>() {
                @Override
                public byte @Nullable [] marshal(@Nullable String object) {
                    return ByteArrayMarshaler.super.marshal(object + ":marshalledOnServer");
                }
            };
        }
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
