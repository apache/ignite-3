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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.marshaling.ByteArrayMarshaler;
import org.apache.ignite.marshaling.Marshaler;
import org.jetbrains.annotations.Nullable;

class Jobs {

    static class ArgumentStringMarshaller implements ByteArrayMarshaler<String> {
        @Override
        public byte @Nullable [] marshal(@Nullable String object) {
            return ByteArrayMarshaler.super.marshal(object + ":marshalledOnClient");
        }
    }

    static class ArgMarshalingJob implements ComputeJob<String, String> {
        @Override
        public CompletableFuture<String> executeAsync(JobExecutionContext context, @Nullable String arg) {
            return completedFuture(arg + ":processedOnServer");
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

    static class ResultStringUnMarshaller implements ByteArrayMarshaler<String> {
        @Override
        public @Nullable String unmarshal(byte @Nullable [] raw) {
            return ByteArrayMarshaler.super.unmarshal(raw) + ":unmarshalledOnClient";
        }
    }

    static class ArgumentAndResultMarshalingJob implements ComputeJob<String, String> {
        @Override
        public CompletableFuture<String> executeAsync(JobExecutionContext context, @Nullable String arg) {
            return completedFuture(arg + ":processedOnServer");
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

    static class JsonMarshalling {
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

    static class JsonMarshaller<T> implements Marshaler<T, byte[]> {
        private final Class<T> clazz;

        JsonMarshaller(Class<T> clazz) {
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

    static class PojoJob implements ComputeJob<PojoArg, PojoResult> {
        @Override
        public CompletableFuture<PojoResult> executeAsync(JobExecutionContext context, @Nullable PojoArg arg) {
            var numberFromStr = Integer.parseInt(arg.strValue);
            return completedFuture(new PojoResult().setLongValue(arg.intValue + numberFromStr));
        }

        @Override
        public Marshaler<PojoArg, byte[]> inputMarshaler() {
            return new JsonMarshaller<>(PojoArg.class);
        }

        @Override
        public Marshaler<PojoResult, byte[]> resultMarshaler() {
            return new JsonMarshaller<>(PojoResult.class);
        }
    }

    static class PojoArg {
        String strValue;
        int intValue;

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

    static class PojoResult {
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
}
