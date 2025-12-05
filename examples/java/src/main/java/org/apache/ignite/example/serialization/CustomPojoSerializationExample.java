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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;

import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.compute.JobTarget;
import org.apache.ignite.deployment.DeploymentUnit;
import org.apache.ignite.marshalling.Marshaller;

/**
 * Example demonstrating custom POJO serialization using custom marshallers
 * on both client and server side.
 */
public class CustomPojoSerializationExample {

    /**
     * Name of the deployment unit used in this example.
     */
    public static final String DEPLOYMENT_UNIT_NAME = "customPojoSerializationExampleUnit";

    /**
     * Version of the deployment unit used in this example.
     */
    public static final String DEPLOYMENT_UNIT_VERSION = "1.0.0";

    /**
     * Runs the POJO job using custom JSON marshallers set on both client and server.
     *
     * @param client Ignite client instance.
     */
    static void runPojoCustomJsonSerialization(IgniteClient client) {

        System.out.println("\n[POJO custom] Running POJO job with custom JSON marshallers both on the client and on the server");

        JobDescriptor<JsonArg, JsonResult> job = JobDescriptor.builder(CustomPojoSerializationJob.class)
                .argumentMarshaller(new JsonArgMarshaller())
                .resultMarshaller(new JsonResultMarshaller())
                .units(new DeploymentUnit(DEPLOYMENT_UNIT_NAME, DEPLOYMENT_UNIT_VERSION))
                .build();

        JsonResult res = client.compute().execute(
                JobTarget.anyNode(client.cluster().nodes()),
                job,
                new JsonArg("ignite", false)
        );

        System.out.printf("[POJO custom] original=%s, result=%s, length=%d%n",
                res.getOriginalWord(), res.getResultWord(), res.getLength());
    }

    /**
     * Marshaller for JSON result objects.
     */
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
}
