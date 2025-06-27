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

package org.apache.ignite.internal.runner.app;

import static java.util.Comparator.comparing;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toList;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.compute.task.MapReduceJob;
import org.apache.ignite.compute.task.MapReduceTask;
import org.apache.ignite.compute.task.TaskExecutionContext;
import org.apache.ignite.marshalling.ByteArrayMarshaller;
import org.apache.ignite.marshalling.Marshaller;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.Nullable;

/** Jobs and marshallers definitions that are used in tests. */
public class Jobs {
    /** Marshal string argument and adds ":marshalledOnClient" to it. */
    public static class ArgumentStringMarshaller implements ByteArrayMarshaller<String> {
        @Override
        public byte @Nullable [] marshal(@Nullable String object) {
            return ByteArrayMarshaller.super.marshal(object + ":marshalledOnClient");
        }
    }

    /** Marshal list of strings argument and adds ":marshalledOnClient" to each string. */
    public static class ArgumentStringListMarshaller implements ByteArrayMarshaller<List<String>> {
        @Override
        public byte @Nullable [] marshal(@Nullable List<String> object) {
            List<String> strings = object.stream().map(str -> str + ":listMarshalledOnClient").collect(toList());
            return ByteArrayMarshaller.super.marshal(strings);
        }
    }

    /**
     * Job that unmarshals input bytes to string and adds ":unmarshalledOnServer" to it, then processes string argument and
     * adds ":processedOnServer" to it.
     */
    public static class ArgMarshallingJob implements ComputeJob<String, String> {
        @Override
        public CompletableFuture<String> executeAsync(JobExecutionContext context, @Nullable String arg) {
            return completedFuture(arg + ":processedOnServer");
        }

        @Override
        public Marshaller<String, byte[]> inputMarshaller() {
            return new ByteArrayMarshaller<>() {
                @Override
                public String unmarshal(byte @Nullable [] raw) {
                    return ByteArrayMarshaller.super.unmarshal(raw) + ":unmarshalledOnServer";
                }
            };
        }
    }

    /** Unmarshals result bytes to string and adds ":unmarshalledOnClient" to it. */
    public static class ResultStringUnMarshaller implements ByteArrayMarshaller<String> {
        @Override
        public @Nullable String unmarshal(byte @Nullable [] raw) {
            return ByteArrayMarshaller.super.unmarshal(raw) + ":unmarshalledOnClient";
        }
    }

    /** Unmarshals result bytes to a list of strings and adds ":unmarshalledOnClient" to each string. */
    public static class ResultStringListUnMarshaller implements ByteArrayMarshaller<List<String>> {
        @Override
        public byte @Nullable [] marshal(@Nullable List<String> object) {
            List<String> strings = object.stream().map(str -> str + ":listMarshalledOnServer").collect(toList());
            return ByteArrayMarshaller.super.marshal(strings);
        }

        @Override
        public @Nullable List<String> unmarshal(byte @Nullable [] raw) {
            List<String> strings = ByteArrayMarshaller.super.unmarshal(raw);
            return strings.stream().map(str -> str + ":listUnmarshalledOnClient").collect(toList());
        }
    }

    /**
     * Job that unmarshals input bytes to string and adds ":unmarshalledOnServer" to it, then processes string argument and
     * adds ":processedOnServer" to it. Then marshals result to bytes and adds ":marshalledOnServer" to it.
     */
    public static class ArgumentAndResultMarshallingJob implements ComputeJob<String, String> {
        @Override
        public CompletableFuture<String> executeAsync(JobExecutionContext context, @Nullable String arg) {
            return completedFuture(arg + ":processedOnServer");
        }

        @Override
        public Marshaller<String, byte[]> inputMarshaller() {
            return new ByteArrayMarshaller<>() {
                @Override
                public String unmarshal(byte @Nullable [] raw) {
                    return ByteArrayMarshaller.super.unmarshal(raw) + ":unmarshalledOnServer";
                }
            };
        }

        @Override
        public Marshaller<String, byte[]> resultMarshaller() {
            return new ByteArrayMarshaller<>() {
                @Override
                public byte @Nullable [] marshal(@Nullable String object) {
                    return ByteArrayMarshaller.super.marshal(object + ":marshalledOnServer");
                }
            };
        }
    }

    /**
     * Job that unmarshals input bytes to string and adds ":unmarshalledOnServer" to it, then processes string argument and
     * adds ":processedOnServer" to it. Then marshals result to bytes and adds ":marshalledOnServer" to it.
     */
    public static class ResultMarshallingJob implements ComputeJob<String, String> {
        @Override
        public CompletableFuture<String> executeAsync(JobExecutionContext context, @Nullable String arg) {
            return completedFuture(arg + ":processedOnServer");
        }

        @Override
        public Marshaller<String, byte[]> resultMarshaller() {
            return new ByteArrayMarshaller<>() {
                @Override
                public byte @Nullable [] marshal(@Nullable String object) {
                    return ByteArrayMarshaller.super.marshal(object + ":marshalledOnServer");
                }
            };
        }
    }

    private static class JsonMarshalling {
        private static final ObjectMapper mapper = new ObjectMapper();

        private static byte[] marshal(Object object) {
            try {
                return mapper.writeValueAsBytes(object);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }

        private static <T> T unmarshal(byte[] raw, Class<T> clazz) {
            try {
                return mapper.readValue(raw, clazz);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /** Marshaller that uses Jackson to marshal/unmarshal objects to/from JSON. */
    public static class JsonMarshaller<T> implements Marshaller<T, byte[]> {
        private final Class<T> clazz;

        public JsonMarshaller(Class<T> clazz) {
            this.clazz = clazz;
        }

        @Override
        public byte @Nullable [] marshal(@Nullable T object) {
            return JsonMarshalling.marshal(object);
        }

        @Override
        public T unmarshal(byte @Nullable [] raw) {
            return JsonMarshalling.unmarshal(raw, clazz);
        }
    }

    /** Job that accepts and returns user-defined POJOs with custom marshaller. */
    public static class PojoJobWithCustomMarshallers implements ComputeJob<PojoArg, PojoResult> {
        @Override
        public CompletableFuture<PojoResult> executeAsync(JobExecutionContext context, @Nullable PojoArg arg) {
            var numberFromStr = Integer.parseInt(arg.strValue);
            return completedFuture(new PojoResult().setLongValue(arg.intValue + numberFromStr));
        }

        @Override
        public Marshaller<PojoArg, byte[]> inputMarshaller() {
            return new JsonMarshaller<>(PojoArg.class);
        }

        @Override
        public Marshaller<PojoResult, byte[]> resultMarshaller() {
            return new JsonMarshaller<>(PojoResult.class);
        }
    }

    /** Job that accepts and returns user-defined POJOs. */
    public static class PojoJob implements ComputeJob<PojoArg, PojoResult> {
        @Override
        public CompletableFuture<PojoResult> executeAsync(JobExecutionContext context, @Nullable PojoArg arg) {
            return completedFuture(new PojoResult().setLongValue(getSum(arg)));
        }

        private static long getSum(@Nullable PojoArg arg) {
            var numberFromStr = Integer.parseInt(arg.strValue);
            var sum = arg.intValue + numberFromStr;
            if (arg.childPojo != null) {
                return sum + getSum(arg.getChildPojo());
            } else {
                return sum;
            }
        }
    }

    /** Job that accepts POJO and returns a string value. */
    public static class PojoArgNativeResult implements ComputeJob<PojoArg, String> {
        @Override
        public CompletableFuture<String> executeAsync(JobExecutionContext context, @Nullable PojoArg arg) {
            return completedFuture(arg.strValue);
        }
    }

    /** POJO argument for {@link PojoJobWithCustomMarshallers} and {@link PojoJob}. */
    public static class PojoArg {
        String strValue;
        int intValue;
        PojoArg childPojo;

        public PojoArg() {
        }

        public String getStrValue() {
            return strValue;
        }

        public PojoArg setStrValue(String strValue) {
            this.strValue = strValue;
            return this;
        }

        public int getIntValue() {
            return intValue;
        }

        public PojoArg setIntValue(int intValue) {
            this.intValue = intValue;
            return this;
        }

        public PojoArg getChildPojo() {
            return childPojo;
        }

        public PojoArg setChildPojo(PojoArg value) {
            this.childPojo = value;
            return this;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            PojoArg pojoArg = (PojoArg) obj;
            return intValue == pojoArg.intValue && strValue.equals(pojoArg.strValue);
        }

        @Override
        public int hashCode() {
            return strValue.hashCode() + intValue;
        }

        @Override
        public String toString() {
            return "PojoArg{" + "strValue='" + strValue + '\'' + ", intValue=" + intValue + '}';
        }
    }

    /** POJO result for {@link PojoJobWithCustomMarshallers} and {@link PojoJob}. */
    public static class PojoResult {
        long longValue;

        public PojoResult() {
        }

        public PojoResult setLongValue(long longValue) {
            this.longValue = longValue;
            return this;
        }

        public long getLongValue() {
            return longValue;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            PojoResult that = (PojoResult) obj;
            return longValue == that.longValue;
        }

        @Override
        public int hashCode() {
            return Long.hashCode(longValue);
        }

        @Override
        public String toString() {
            return "PojoResult{" + "longValue=" + longValue + '}';
        }
    }

    /** MapReduce task that splits input list into two parts and sends them to different nodes. */
    public static class MapReduce implements MapReduceTask<List<String>, String, String, List<String>> {
        @Override
        public CompletableFuture<List<MapReduceJob<String, String>>> splitAsync(
                TaskExecutionContext taskContext,
                @Nullable List<String> input) {

            List<ClusterNode> nodes = new ArrayList<>(taskContext.ignite().cluster().nodes());
            nodes.sort(comparing(ClusterNode::name));

            var mapJobDescriptor = JobDescriptor.builder(ArgumentAndResultMarshallingJob.class)
                    .argumentMarshaller(new ArgumentStringMarshaller())
                    .resultMarshaller(new ResultStringUnMarshaller())
                    .build();

            return completedFuture(List.of(
                    MapReduceJob.<String, String>builder()
                            .jobDescriptor(mapJobDescriptor)
                            .node(nodes.get(0))
                            .args(input.get(0))
                            .build(),
                    MapReduceJob.<String, String>builder()
                            .jobDescriptor(mapJobDescriptor)
                            .node(nodes.get(1))
                            .args(input.get(1))
                            .build()
            ));
        }

        @Override
        public CompletableFuture<List<String>> reduceAsync(TaskExecutionContext taskContext, Map<UUID, String> results) {
            return completedFuture(new ArrayList<>(results.values()));
        }

        @Override
        public @Nullable Marshaller<List<String>, byte[]> splitJobInputMarshaller() {
            return new ArgumentStringListMarshaller();
        }

        @Override
        public @Nullable Marshaller<List<String>, byte[]> reduceJobResultMarshaller() {
            return new ResultStringListUnMarshaller();
        }
    }

    /** MapReduce that adds a column to the tuple on each step. */
    public static class MapReduceTuples implements MapReduceTask<Tuple, Tuple, Tuple, Tuple> {
        @Override
        public CompletableFuture<List<MapReduceJob<Tuple, Tuple>>> splitAsync(TaskExecutionContext taskContext, @Nullable Tuple input) {
            List<ClusterNode> nodes = new ArrayList<>(taskContext.ignite().cluster().nodes());
            Tuple jobsInput = Tuple.copy(input);
            jobsInput.set("split", "call");

            var mapJobDescriptor = JobDescriptor.builder(EchoTupleJob.class).build();

            return completedFuture(List.of(
                    MapReduceJob.<Tuple, Tuple>builder()
                            .jobDescriptor(mapJobDescriptor)
                            .node(nodes.get(0))
                            .args(jobsInput)
                            .build(),
                    MapReduceJob.<Tuple, Tuple>builder()
                            .jobDescriptor(mapJobDescriptor)
                            .node(nodes.get(1))
                            .args(jobsInput)
                            .build()
            ));
        }

        @Override
        public CompletableFuture<Tuple> reduceAsync(TaskExecutionContext taskContext, Map<UUID, Tuple> results) {
            Tuple reduceResult = Tuple.copy((Tuple) results.values().toArray()[0]);
            reduceResult.set("reduce", "call");

            return completedFuture(reduceResult);
        }
    }

    static class EchoTupleJob implements ComputeJob<Tuple, Tuple> {
        @Override
        public @Nullable CompletableFuture<Tuple> executeAsync(JobExecutionContext context, @Nullable Tuple arg) {
            var tuple = Tuple.copy(arg);
            tuple.set("echo", "echo");

            return completedFuture(tuple);
        }
    }

    /** MapReduce task that takes two strings from the input pojo, sends them to different nodes then combines results into pojo. */
    public static class MapReducePojo implements MapReduceTask<TwoStringPojo, String, String, TwoStringPojo> {
        @Override
        public CompletableFuture<List<MapReduceJob<String, String>>> splitAsync(
                TaskExecutionContext taskContext,
                @Nullable TwoStringPojo input
        ) {

            List<ClusterNode> nodes = new ArrayList<>(taskContext.ignite().cluster().nodes());

            var mapJobDescriptor = JobDescriptor.builder(ArgumentAndResultMarshallingJob.class)
                    .argumentMarshaller(new ArgumentStringMarshaller())
                    .resultMarshaller(new ResultStringUnMarshaller())
                    .build();

            return completedFuture(List.of(
                    MapReduceJob.<String, String>builder()
                            .jobDescriptor(mapJobDescriptor)
                            .node(nodes.get(0))
                            .args(input.firstString)
                            .build(),
                    MapReduceJob.<String, String>builder()
                            .jobDescriptor(mapJobDescriptor)
                            .node(nodes.get(1))
                            .args(input.secondString)
                            .build()
            ));
        }

        @Override
        public CompletableFuture<TwoStringPojo> reduceAsync(TaskExecutionContext taskContext, Map<UUID, String> results) {
            List<String> res = new ArrayList<>(results.values());
            return completedFuture(new TwoStringPojo(res.get(0), res.get(1)));
        }
    }

    /** Simple two string pojo. */
    public static class TwoStringPojo {
        public String firstString;
        public String secondString;

        public TwoStringPojo() {
        }

        public TwoStringPojo(String firstString, String secondString) {
            this.firstString = firstString;
            this.secondString = secondString;
        }
    }
}
