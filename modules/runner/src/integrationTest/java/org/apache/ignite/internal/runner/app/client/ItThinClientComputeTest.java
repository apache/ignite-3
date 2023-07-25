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

import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;
import static org.apache.ignite.lang.ErrorGroups.Table.COLUMN_ALREADY_EXISTS_ERR;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.compute.arg.MappedArgs;
import org.apache.ignite.compute.arg.PojoArgs;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.serialization.JsonObjectSerializer;
import org.apache.ignite.serialization.descriptor.GenericClassDescriptor;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Thin client compute integration test.
 */
@SuppressWarnings("resource")
public class ItThinClientComputeTest extends ItAbstractThinClientTest {
    /** Test trace id. */
    private static final UUID TRACE_ID = UUID.randomUUID();

    @Test
    void testClusterNodes() {
        List<ClusterNode> nodes = sortedNodes();

        assertEquals(2, nodes.size());

        assertEquals("itcct_n_3344", nodes.get(0).name());
        assertEquals(3344, nodes.get(0).address().port());
        assertTrue(nodes.get(0).id().length() > 10);

        assertEquals("itcct_n_3345", nodes.get(1).name());
        assertEquals(3345, nodes.get(1).address().port());
        assertTrue(nodes.get(1).id().length() > 10);
    }

    @Test
    void testExecuteOnSpecificNode() {
        String res1 = client().compute().<String>execute(Set.of(node(0)), List.of(), NodeNameJob.class.getName()).join();
        String res2 = client().compute().<String>execute(Set.of(node(1)), List.of(), NodeNameJob.class.getName()).join();

        assertEquals("itcct_n_3344", res1);
        assertEquals("itcct_n_3345", res2);
    }

    @Test
    void testExecuteOnRandomNode() {
        String res = client().compute().<String>execute(new HashSet<>(sortedNodes()), List.of(), NodeNameJob.class.getName()).join();

        assertTrue(Set.of("itcct_n_3344", "itcct_n_3345").contains(res));
    }

    @Test
    void testBroadcastOneNode() {
        Map<ClusterNode, CompletableFuture<String>> futuresPerNode = client().compute().broadcast(
                Set.of(node(1)),
                List.of(),
                NodeNameJob.class.getName(),
                PojoArgs.fromArray("_", 123)
        );

        assertEquals(1, futuresPerNode.size());

        String res = futuresPerNode.get(node(1)).join();

        assertEquals("itcct_n_3345__123", res);
    }

    @Test
    void testBroadcastAllNodes() {
        Map<ClusterNode, CompletableFuture<String>> futuresPerNode = client().compute().broadcast(
                new HashSet<>(sortedNodes()),
                List.of(),
                NodeNameJob.class.getName(),
                PojoArgs.fromArray("_", 123)
        );

        assertEquals(2, futuresPerNode.size());

        String res1 = futuresPerNode.get(node(0)).join();
        String res2 = futuresPerNode.get(node(1)).join();

        assertEquals("itcct_n_3344__123", res1);
        assertEquals("itcct_n_3345__123", res2);
    }

    @Test
    void testExecuteWithArgs() {
        var nodes = new HashSet<>(client().clusterNodes());
        String res = client().compute().<String>execute(nodes, List.of(), ConcatJob.class.getName(), PojoArgs.fromArray(1, "2", 3.3)).join();

        assertEquals("1_2_3.3", res);
    }

    @Test
    void testIgniteExceptionInJobPropagatesToClientWithMessageAndCodeAndTraceId() {
        CompletionException ex = assertThrows(
                CompletionException.class,
                () ->  client().compute().<String>execute(Set.of(node(0)), List.of(), IgniteExceptionJob.class.getName()).join());

        var cause = (IgniteException) ex.getCause();

        assertThat(cause.getMessage(), containsString("Custom job error"));
        assertEquals(TRACE_ID, cause.traceId());
        assertEquals(COLUMN_ALREADY_EXISTS_ERR, cause.code());
        assertInstanceOf(CustomException.class, cause);
        assertNull(cause.getCause()); // No stack trace by default.
    }

    @Test
    void testExceptionInJobPropagatesToClientWithClassAndMessage() {
        CompletionException ex = assertThrows(
                CompletionException.class,
                () ->  client().compute().<String>execute(Set.of(node(0)), List.of(), ExceptionJob.class.getName()).join());

        var cause = (IgniteException) ex.getCause();

        assertThat(cause.getMessage(), containsString("ArithmeticException: math err"));
        assertEquals(INTERNAL_ERR, cause.code());
        assertNull(cause.getCause()); // No stack trace by default.
    }

    @Test
    void testExceptionInJobWithSendServerExceptionStackTraceToClientPropagatesToClientWithStackTrace() {
        // Second node has sendServerExceptionStackTraceToClient enabled.
        CompletionException ex = assertThrows(
                CompletionException.class,
                () ->  client().compute().execute(Set.of(node(1)), List.of(), ExceptionJob.class.getName()).join());

        var cause = (IgniteException) ex.getCause();

        assertThat(cause.getMessage(), containsString("ArithmeticException: math err"));
        assertEquals(INTERNAL_ERR, cause.code());

        assertNotNull(cause.getCause());
        assertThat(cause.getCause().getMessage(), containsString(
                "at org.apache.ignite.internal.runner.app.client.ItThinClientComputeTest$"
                        + "ExceptionJob.execute(ItThinClientComputeTest.java:"));
    }

    @ParameterizedTest
    @CsvSource({"1,3344", "2,3345", "3,3345", "10,3344"})
    void testExecuteColocatedRunsComputeJobOnKeyNode(int key, int port) {
        var table = TABLE_NAME;
        var keyTuple = Tuple.create().set(COLUMN_KEY, key);
        var keyPojo = new TestPojo(key);

        String tupleRes = client().compute().<String>executeColocated(table, keyTuple, List.of(), NodeNameJob.class.getName()).join();
        String pojoRes = client().compute().<TestPojo, String>executeColocated(
                table,
                keyPojo,
                Mapper.of(TestPojo.class),
                List.of(),
                NodeNameJob.class.getName()
        ).join();

        String expectedNode = "itcct_n_" + port;
        assertEquals(expectedNode, tupleRes);
        assertEquals(expectedNode, pojoRes);
    }

    @Test
    void testExecuteOnUnknownUnitWithLatestVersionThrows() {
        CompletionException ex = assertThrows(
                CompletionException.class,
                () -> client().compute().<String>execute(
                        Set.of(node(0)),
                        List.of(new DeploymentUnit("u", "latest")),
                        NodeNameJob.class.getName()
                ).join());

        var cause = (IgniteException) ex.getCause();
        assertThat(cause.getMessage(), containsString("Deployment unit u:latest doesn't exist"));

        // TODO IGNITE-19823 DeploymentUnitNotFoundException is internal, does not propagate to client.
        assertEquals(INTERNAL_ERR, cause.code());
    }

    @Test
    void testExecuteColocatedOnUnknownUnitWithLatestVersionThrows() {
        CompletionException ex = assertThrows(
                CompletionException.class,
                () -> client().compute().<String>executeColocated(
                        TABLE_NAME,
                        Tuple.create().set(COLUMN_KEY, 1),
                        List.of(new DeploymentUnit("u", "latest")),
                        NodeNameJob.class.getName()
                ).join());

        var cause = (IgniteException) ex.getCause();
        assertThat(cause.getMessage(), containsString("Deployment unit u:latest doesn't exist"));

        // TODO IGNITE-19823 DeploymentUnitNotFoundException is internal, does not propagate to client.
        assertEquals(INTERNAL_ERR, cause.code());
    }

    @Test
    public void testPojoClass() {
        IgniteClient client = client();

        ArgClass arg1 = new ArgClass();
        arg1.setId("id1");
        arg1.setList(List.of("key1", "value1"));

        ArgClass arg2 = new ArgClass();
        arg2.setId("id2");
        arg2.setList(List.of("key2", "value2"));

        ArgClass arg3 = new ArgClass();
        arg3.setId("id3");
        arg3.setList(List.of("key3", "value3"));

        JsonObjectSerializer ser = new JsonObjectSerializer();

        MappedArgs args = MappedArgs.fromArray(PojoJobMapper.class, ser.serialize(arg1), ser.serialize(arg2), ser.serialize(arg3));
        Object join = client.compute()
                .<PojoClass>execute(Set.of(node(0)), List.of(), PojoClassJob.class.getName(), args).join();
        PojoClass pojo = ser.deserialize((byte[]) join, PojoClass.class);

        assertEquals("id1id2id3", pojo.id);
        assertEquals(List.of("key1", "value1", "key2", "value2", "key3", "value3"),
                pojo.list);
    }

    @Test
    public void testGenericArgs() {
        IgniteClient client = client();

        GenericArg<String> strArg = new GenericArg<>();
        strArg.setId("id1");
        strArg.setType("type1");

        GenericArg<Integer> intArg = new GenericArg<>();
        intArg.setId("id2");
        intArg.setType(1);

        GenericArg<Double> doubleArg = new GenericArg<>();
        doubleArg.setId("id3");
        doubleArg.setType(Math.PI);

        JsonObjectSerializer ser = new JsonObjectSerializer();

        MappedArgs args = MappedArgs.fromArray(GenericArgMapper1.class, ser.serialize(strArg), ser.serialize(intArg), ser.serialize(doubleArg));
        String result = client.compute()
                .<String>execute(Set.of(node(0)), List.of(), GenericJob.class.getName(), args).join();

        assertEquals("id1type1id21id3" + Math.PI, result);


        MappedArgs args2 = MappedArgs.fromArray(GenericArgMapper2.class, ser.serialize(doubleArg), ser.serialize(intArg), ser.serialize(strArg));
        String result2 = client.compute()
                .<String>execute(Set.of(node(0)), List.of(), GenericJob.class.getName(), args2).join();

        assertEquals("id3" + Math.PI + "id21id1type1", result2);
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
        testEchoArg(BigInteger.TEN);
    }

    private void testEchoArg(Object arg) {
        Object res = client().compute().execute(Set.of(node(0)), List.of(), EchoJob.class.getName(), PojoArgs.fromArray(arg, arg.toString())).join();

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
            if (args == null) {
                return null;
            }

            return Arrays.stream(args).map(o -> o == null ? "null" : o.toString()).collect(Collectors.joining("_"));
        }
    }

    private static class IgniteExceptionJob implements ComputeJob<String> {
        @Override
        public String execute(JobExecutionContext context, Object... args) {
            throw new CustomException(TRACE_ID, COLUMN_ALREADY_EXISTS_ERR, "Custom job error", null);
        }
    }

    private static class ExceptionJob implements ComputeJob<String> {
        @Override
        public String execute(JobExecutionContext context, Object... args) {
            throw new ArithmeticException("math err");
        }
    }

    private static class EchoJob implements ComputeJob<Object> {
        @Override
        public Object execute(JobExecutionContext context, Object... args) {
            var value = args[0];

            if (!(value instanceof byte[])) {
                var expectedString = (String) args[1];
                var valueString = value == null ? "null" : value.toString();
                assertEquals(expectedString, valueString, "Unexpected string representation of value");
            }

            return args[0];
        }
    }

    /**
     * Custom public exception class.
     */
    public static class CustomException extends IgniteException {
        public CustomException(UUID traceId, int code, String message, Throwable cause) {
            super(traceId, code, message, cause);
        }
    }

    private static class PojoClassJob implements ComputeJob<PojoClass> {
        @Override
        public PojoClass execute(JobExecutionContext context, Object... args) {
            StringBuilder id = new StringBuilder();
            List<String> list = new ArrayList<>();
            for (Object arg : args) {
                ArgClass argCast = (ArgClass) arg;
                id.append(argCast.id);
                list.addAll(argCast.list);
            }
            return new PojoClass(id.toString(), list);
        }
    }

    private static class PojoJobMapper implements org.apache.ignite.compute.arg.Mapper {

        @Override
        public Object[] map(Object[] content) {
            JsonObjectSerializer serializer = new JsonObjectSerializer();
            return Arrays.stream(content).map(arg -> serializer.deserialize((byte[]) arg, ArgClass.class)).toArray();
        }
    }

    private static class PojoClass {
        private String id;
        private List<String> list;

        public PojoClass() {
            this(null, null);
        }

        public PojoClass(String id, List<String> list) {
            this.id = id;
            this.list = list;
        }

        public String getId() {
            return id;
        }

        public List<String> getList() {
            return list;
        }

        public void setId(String id) {
            this.id = id;
        }

        public void setList(List<String> list) {
            this.list = list;
        }
    }

    private static class ArgClass {
        private String id;
        private List<String> list;

        public String getId() {
            return id;
        }

        public void setList(List<String> list) {
            this.list = list;
        }

        public void setId(String id) {
            this.id = id;
        }

        public List<String> getList() {
            return list;
        }
    }

    private static class GenericJob implements ComputeJob<String> {

        @Override
        public String execute(JobExecutionContext context, Object... args) {
            StringBuilder result = new StringBuilder();
            for (Object arg : args) {
                GenericArg<?> argCast = (GenericArg<?>) arg;
                result.append(argCast.id).append(argCast.type);
            }
            return result.toString();
        }
    }

    private static class GenericArgMapper1 implements org.apache.ignite.compute.arg.Mapper {

        @Override
        public Object[] map(Object[] content) {
            JsonObjectSerializer serializer = new JsonObjectSerializer();
            Object[] result = new Object[3];

            for (int i = 0; i < content.length; i++) {
                Object o = content[i];
                if (i == 0) {
                    result[i] = serializer.deserialize((byte[]) o, new GenericClassDescriptor<GenericArg<String>>() { });
                } else if (i == 1) {
                    result[i] = serializer.deserialize((byte[]) o, new GenericClassDescriptor<GenericArg<Integer>>() { });
                } else {
                    result[i] = serializer.deserialize((byte[]) o, new GenericClassDescriptor<GenericArg<Double>>() { });
                }
            }
            return result;
        }
    }
    private static class GenericArgMapper2 implements org.apache.ignite.compute.arg.Mapper {

        @Override
        public Object[] map(Object[] content) {
            JsonObjectSerializer serializer = new JsonObjectSerializer();
            Object[] result = new Object[3];

            for (int i = 0; i < content.length; i++) {
                Object o = content[i];
                if (i == 0) {
                    result[i] = serializer.deserialize((byte[]) o, new GenericClassDescriptor<GenericArg<Double>>() { });
                } else if (i == 1) {
                    result[i] = serializer.deserialize((byte[]) o, new GenericClassDescriptor<GenericArg<Integer>>() { });
                } else {
                    result[i] = serializer.deserialize((byte[]) o, new GenericClassDescriptor<GenericArg<String>>() { });
                }
            }
            return result;
        }
    }



    private static class GenericArg<T> {
        private String id;
        private T type;

        public String getId() {
            return id;
        }

        public T getType() {
            return type;
        }

        public void setId(String id) {
            this.id = id;
        }

        public void setType(T type) {
            this.type = type;
        }
    }
}
