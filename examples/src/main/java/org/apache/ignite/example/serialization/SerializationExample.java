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

package org.apache.ignite.example.serialization;

import java.util.Objects;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.compute.JobTarget;
import org.apache.ignite.deployment.DeploymentUnit;
import org.apache.ignite.marshalling.Marshaller;
import org.apache.ignite.table.Tuple;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import static java.util.concurrent.CompletableFuture.completedFuture;

public class SerializationExample {

    private static final String DEPLOYMENT_UNIT_NAME = "computeExampleUnit";
    private static final String DEPLOYMENT_UNIT_VERSION = "1.0.0";

    private final IgniteClient client;
    private final IgniteCompute compute;

    private SerializationExample(IgniteClient client) {
        this.client = client;
        this.compute = client.compute();
    }

    public static void main(String[] args) throws Exception {
        try (IgniteClient client = IgniteClient.builder()
                .addresses("127.0.0.1:10800")
                .build()) {

            SerializationExample ex = new SerializationExample(client);

            ex.runNativeSerialization();
            ex.runTupleSerialization();
            ex.runPojoAutoSerialization();
            ex.runPojoCustomJsonSerialization();
        }
    }

    /** Using native types: primitives or wrappers, so no marshallers needed, as Ignite auto-serializes them. */
    private void runNativeSerialization() {
        System.out.println("\n[Native] Running Integer decrement job...");

        JobDescriptor<Integer, Integer> job = JobDescriptor.builder(IntegerDecrementJob.class)
                .units(new DeploymentUnit(DEPLOYMENT_UNIT_NAME, DEPLOYMENT_UNIT_VERSION))
                .build();

        Integer result = compute.execute(JobTarget.anyNode(client.cluster().nodes()), job, 5);
        System.out.println("[Native] Input=5, Result=" + result);
    }

    /** Using Tuples, no marshallers needed as tuples are also auto-serialized by Ignite. */
    private void runTupleSerialization() {
        System.out.println("\n[Tuple] Running tuple transform job...");

        JobDescriptor<Tuple, Tuple> job = JobDescriptor.builder(TupleTransformJob.class)
                .units(new DeploymentUnit(DEPLOYMENT_UNIT_NAME, DEPLOYMENT_UNIT_VERSION))
                .build();

        Tuple arg = Tuple.create().set("word", "GridGain").set("upper", true);
        Tuple res = compute.execute(JobTarget.anyNode(client.cluster().nodes()), job, arg);
        System.out.println("[Tuple] Transformed: " + res.stringValue("result"));
    }

    /**
     * Using POJO auto-serialization: no custom marshallers needed, as POJOs are automatically marshalled to tuples if no marshaller is
     * defined.
     */
    private void runPojoAutoSerialization() {
        System.out.println("\n[POJO auto] Running POJO job without custom marshallers...");

        JobDescriptor<AutoSerialazableArg, AutoSerializableResult> job = JobDescriptor.<AutoSerialazableArg, AutoSerializableResult>builder(AutoPojoJob.class)
                .units(new DeploymentUnit(DEPLOYMENT_UNIT_NAME, DEPLOYMENT_UNIT_VERSION))
                .build();

        AutoSerializableResult res = compute.execute(
                JobTarget.anyNode(client.cluster().nodes()),
                job,
                new AutoSerialazableArg("ignite", true)
        );

        System.out.printf("[POJO auto] original=%s, result=%s, length=%d%n",
                res.original, res.result, res.length);
    }

    /**
     * Using POJO with custom marshallers: job runs on the server and sets input/result marshallers, meanwhile client sets argument/result marshallers on
     * descriptor.
     */
    private void runPojoCustomJsonSerialization() {
        System.out.println("\n[POJO custom] Running POJO job with JSON marshallers on both sides...");

        JobDescriptor<JsonArg, JsonResult> job = JobDescriptor.<JsonArg, JsonResult>builder(PojoJsonJob.class.getName())
                .argumentMarshaller(new JsonArgMarshaller())
                .resultMarshaller(new JsonResultMarshaller())
                .units(new DeploymentUnit(DEPLOYMENT_UNIT_NAME, DEPLOYMENT_UNIT_VERSION))
                .build();

        JsonResult res = compute.execute(
                JobTarget.anyNode(client.cluster().nodes()),
                job,
                new JsonArg("ignite", false)
        );

        System.out.printf("[POJO custom] original=%s, result=%s, length=%d%n",
                res.original, res.result, res.length);
    }

    /** Jobs and marshallers */

    /** Using wrapper type for auto-serialization. */
    private static class IntegerDecrementJob implements ComputeJob<Integer, Integer> {
        @Override
        public CompletableFuture<Integer> executeAsync(JobExecutionContext ctx, Integer arg) {
            return completedFuture(arg == null ? null : arg - 1);
        }
    }

    /** Tuple job: transform a word to upper/lower case. */
    private static class TupleTransformJob implements ComputeJob<Tuple, Tuple> {
        @Override
        public CompletableFuture<Tuple> executeAsync(JobExecutionContext ctx, Tuple arg) {
            String word = Objects.requireNonNull(arg).stringValue("word");
            boolean upper = arg.booleanValue("upper");
            return completedFuture(Tuple.create()
                    .set("original", word)
                    .set("result",
                            upper ? word != null ? word.toUpperCase() : null : (word == null ? null : word.toLowerCase())));
        }
    }

    /** POJO auto-serialization job. */
    private static class AutoPojoJob implements ComputeJob<AutoSerialazableArg, AutoSerializableResult> {
        @Override
        public CompletableFuture<AutoSerializableResult> executeAsync(JobExecutionContext ctx, AutoSerialazableArg arg) {
            String w = arg == null ? null : arg.word;
            boolean upper = arg != null && arg.upper;
            AutoSerializableResult r = new AutoSerializableResult();
            r.original = Objects.requireNonNull(w);
            r.result = upper ? w.toUpperCase() : w.toLowerCase();
            r.length = w.length();
            return completedFuture(r);
        }
    }


    /** Using POJO and defining a custom JSON marshalling on both server and client. */
    private static class PojoJsonJob implements ComputeJob<JsonArg, JsonResult> {
        @Override
        public CompletableFuture<JsonResult> executeAsync(JobExecutionContext ctx, JsonArg arg) {
            String w = arg == null ? null : arg.word;
            boolean upper = arg != null && arg.upper;
            JsonResult r = new JsonResult();
            r.original = Objects.requireNonNull(w);
            r.result = upper ? w.toUpperCase() : w.toLowerCase();
            r.length = w.length();
            return completedFuture(r);
        }

        @Override
        public Marshaller<JsonArg, byte[]> inputMarshaller() {
            return new JsonArgServerMarshaller();
        }

        @Override
        public Marshaller<JsonResult, byte[]> resultMarshaller() {
            return new JsonResultServerMarshaller();
        }
    }

    /** Client-side marshallers */
    private static class JsonArgMarshaller implements Marshaller<JsonArg, byte[]> {
        private static final ObjectMapper MAPPER = new ObjectMapper();

        @Override
        public byte[] marshal(JsonArg o) {
            try {
                return MAPPER.writeValueAsBytes(o);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public JsonArg unmarshal(byte[] raw) {
            try {
                return MAPPER.readValue(raw, JsonArg.class);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static class JsonResultMarshaller implements Marshaller<JsonResult, byte[]> {
        private static final ObjectMapper MAPPER = new ObjectMapper();

        @Override
        public byte[] marshal(JsonResult o) {
            try {
                return MAPPER.writeValueAsBytes(o);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public JsonResult unmarshal(byte[] raw) {
            try {
                return MAPPER.readValue(raw, JsonResult.class);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /** Server-side marshallers */
    private static class JsonArgServerMarshaller extends JsonArgMarshaller {
    }

    private static class JsonResultServerMarshaller extends JsonResultMarshaller {
    }
}

