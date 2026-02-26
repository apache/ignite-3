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
import org.apache.ignite.table.Tuple;

public class TupleSerializationExample {

    public static final String DEPLOYMENT_UNIT_NAME = "tupleSerializationExampleUnit";
    public static final String DEPLOYMENT_UNIT_VERSION = "1.0.0";

    /** Using tuples, no marshallers needed as tuples are also autoserialized by Ignite. */
    static void runTupleSerialization(IgniteClient client) {

        System.out.println("\n[Tuple] Running tuple transform job...");

        JobDescriptor<Tuple, Tuple> job = JobDescriptor.builder(TupleTransformJob.class)
                .units(new DeploymentUnit(DEPLOYMENT_UNIT_NAME, DEPLOYMENT_UNIT_VERSION))
                .build();

        Tuple arg = Tuple.create().set("key", "value");
        Tuple res = client.compute().execute(JobTarget.anyNode(client.cluster().nodes()), job, arg);

        System.out.println("Result from job: " + res);

        System.out.println("[Tuple] Transformed: " + res.stringValue("key"));
    }
}

