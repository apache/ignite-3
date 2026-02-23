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

import static org.apache.ignite.example.util.DeployComputeUnit.deployIfNotExist;
import static org.apache.ignite.example.util.DeployComputeUnit.undeployUnit;

import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.example.util.DeployComputeUnit;
/**
 * This example demonstrates the usage of the { @link IgniteCompute#executeAsync(JobTarget, JobDescriptor, Object)} API.
 *
 * <p>See {@code README.md} in the {@code examples} directory for execution instructions.</p>
 */

public class SerializationExample {

    private static final String DEPLOYMENT_UNIT_NATIVE = "nativeSerializationExampleUnit";
    private static final String DEPLOYMENT_UNIT_CUSTOM = "customPojoSerializationExampleUnit";
    private static final String DEPLOYMENT_UNIT_AUTO = "pojoAutoSerializationExampleUnit";
    private static final String DEPLOYMENT_UNIT_TUPLE = "tupleSerializationExampleUnit";
    private static final String VERSION = "1.0.0";

    public static void main(String[] args) throws Exception {
        try (IgniteClient client = IgniteClient.builder()
                .addresses("127.0.0.1:10800")
                .build()) {

            DeployComputeUnit.processDeploymentUnit(args);

            deployIfNotExist(DEPLOYMENT_UNIT_NATIVE, VERSION, DeployComputeUnit.getJarPath());
            NativeTypeSerializationExample.runNativeSerialization(client);

            deployIfNotExist(DEPLOYMENT_UNIT_TUPLE, VERSION, DeployComputeUnit.getJarPath());
            TupleSerializationExample.runTupleSerialization(client);

            deployIfNotExist(DEPLOYMENT_UNIT_AUTO, VERSION, DeployComputeUnit.getJarPath());
            PojoAutoSerializationExample.runPojoAutoSerialization(client);

            deployIfNotExist(DEPLOYMENT_UNIT_CUSTOM, VERSION, DeployComputeUnit.getJarPath());
            CustomPojoSerializationExample.runPojoCustomJsonSerialization(client);

        } finally {

            System.out.println("Cleaning up resources...");
            undeployUnit(DEPLOYMENT_UNIT_CUSTOM, VERSION);
            undeployUnit(DEPLOYMENT_UNIT_AUTO, VERSION);
            undeployUnit(DEPLOYMENT_UNIT_NATIVE, VERSION);
            undeployUnit(DEPLOYMENT_UNIT_TUPLE, VERSION);

        }
    }
}
