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

import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.compute.JobTarget;
import org.apache.ignite.deployment.DeploymentUnit;

class NativeTypeSerializationExample {

    private static final String DEPLOYMENT_UNIT_NAME = "nativeSerializationExampleUnit";
    private static final String DEPLOYMENT_UNIT_VERSION = "1.0.0";

    /** Using native types: primitives or wrappers, so no marshallers needed, as Ignite autoserializes native types. */
    static void runNativeSerialization(IgniteClient client) {

            System.out.println("\n[Native] Running Integer decrement job...");

            JobDescriptor<Integer, Integer> job = JobDescriptor.builder(IntegerDecrementJob.class)
                    .units(new DeploymentUnit(DEPLOYMENT_UNIT_NAME, DEPLOYMENT_UNIT_VERSION))
                    .build();

            Integer result = client.compute().execute(JobTarget.anyNode(client.cluster().nodes()), job, 5);
            System.out.println("[Native] Input=5, Result=" + result);
    }
}

