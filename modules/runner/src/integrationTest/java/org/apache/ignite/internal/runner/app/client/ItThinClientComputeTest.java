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

package org.apache.ignite.internal.runner.app.client;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;
import org.apache.ignite.client.IgniteClientException;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.tx.TransactionException;
import org.junit.jupiter.api.Test;

/**
 * Thin client compute integration test.
 */
public class ItThinClientComputeTest extends ItAbstractThinClientTest {
    @Test
    void testClusterNodes() {
        List<ClusterNode> nodes = sortedNodes();

        assertEquals(2, nodes.size());

        assertEquals("ItThinClientComputeTest_null_3344", nodes.get(0).name());
        assertEquals(3344, nodes.get(0).address().port());
        assertTrue(nodes.get(0).id().length() > 10);

        assertEquals("ItThinClientComputeTest_null_3345", nodes.get(1).name());
        assertEquals(3345, nodes.get(1).address().port());
        assertTrue(nodes.get(1).id().length() > 10);
    }

    @Test
    void testExecuteOnSpecificNode() {
        String res1 = client().compute().execute(Set.of(node(0)), NodeNameJob.class).join();
        String res2 = client().compute().execute(Set.of(node(1)), NodeNameJob.class).join();

        assertEquals("ItThinClientComputeTest_null_3344", res1);
        assertEquals("ItThinClientComputeTest_null_3345", res2);
    }

    @Test
    void testExecuteOnRandomNode() {
        String res = client().compute().execute(new HashSet<>(sortedNodes()), NodeNameJob.class).join();

        assertTrue(Set.of("ItThinClientComputeTest_null_3344", "ItThinClientComputeTest_null_3345").contains(res));
    }

    @Test
    void testBroadcastOneNode() {
        Map<ClusterNode, CompletableFuture<String>> futuresPerNode = client().compute().broadcast(
                Set.of(node(1)),
                NodeNameJob.class,
                "_",
                123);

        assertEquals(1, futuresPerNode.size());

        String res = futuresPerNode.get(node(1)).join();

        assertEquals("ItThinClientComputeTest_null_3345__123", res);
    }

    @Test
    void testBroadcastAllNodes() {
        Map<ClusterNode, CompletableFuture<String>> futuresPerNode = client().compute().broadcast(
                new HashSet<>(sortedNodes()),
                NodeNameJob.class,
                "_",
                123);

        assertEquals(2, futuresPerNode.size());

        String res1 = futuresPerNode.get(node(0)).join();
        String res2 = futuresPerNode.get(node(1)).join();

        assertEquals("ItThinClientComputeTest_null_3344__123", res1);
        assertEquals("ItThinClientComputeTest_null_3345__123", res2);
    }

    @Test
    void testExecuteWithArgs() {
        var nodes = new HashSet<>(client().clusterNodes());
        String res = client().compute().execute(nodes, ConcatJob.class, 1, "2", 3.3).join();

        assertEquals("1_2_3.3", res);
    }

    @Test
    void testJobErrorPropagatesToClientWithClassAndMessage() {
        CompletionException ex = assertThrows(
                CompletionException.class,
                () ->  client().compute().execute(Set.of(node(0)), ErrorJob.class).join());

        IgniteClientException cause = (IgniteClientException) ex.getCause();

        assertThat(cause.getMessage(), containsString("TransactionException: Custom job error"));
    }

    @Test
    void testAllSupportedArgTypes() {
        testEchoArg(Byte.MAX_VALUE);
        testEchoArg(Short.MAX_VALUE);
        testEchoArg(Integer.MAX_VALUE);
        testEchoArg(Long.MAX_VALUE);
        testEchoArg(Float.MAX_VALUE);
        testEchoArg(Double.MAX_VALUE);
        testEchoArg(BigDecimal.TEN);
        testEchoArg(UUID.randomUUID());
        testEchoArg("string");
        testEchoArg(new byte[] {1, 2, 3});
        testEchoArg(new BitSet(10));
        testEchoArg(LocalDate.now());
        testEchoArg(LocalTime.now());
        testEchoArg(LocalDateTime.now());
        testEchoArg(Instant.now());
        testEchoArg(true);
        testEchoArg(BigInteger.TEN);
    }

    private void testEchoArg(Object arg) {
        Object res = client().compute().execute(Set.of(node(0)), EchoJob.class, arg).join();

        if (arg instanceof byte[]) {
            assertArrayEquals((byte[]) arg, (byte[]) res);
        } else {
            assertEquals(arg, res);
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

    private static class NodeNameJob implements ComputeJob<String> {
        @Override
        public String execute(JobExecutionContext context, Object... args) {
            return context.ignite().name() + Arrays.stream(args).map(Object::toString).collect(Collectors.joining("_"));
        }
    }

    private static class ConcatJob implements ComputeJob<String> {
        @Override
        public String execute(JobExecutionContext context, Object... args) {
            return Arrays.stream(args).map(Object::toString).collect(Collectors.joining("_"));
        }
    }

    private static class ErrorJob implements ComputeJob<String> {
        @Override
        public String execute(JobExecutionContext context, Object... args) {
            throw new TransactionException("Custom job error");
        }
    }

    private static class EchoJob implements ComputeJob<Object> {
        @Override
        public Object execute(JobExecutionContext context, Object... args) {
            return args[0];
        }
    }
}
