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

package org.apache.ignite.example.code.deployment;

import static org.apache.ignite.example.util.DeployComputeUnit.deployIfNotExist;
import static org.apache.ignite.example.util.DeployComputeUnit.undeployUnit;

import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.compute.JobTarget;
import org.apache.ignite.deployment.DeploymentUnit;
import org.apache.ignite.example.util.DeployComputeUnit;

/**
 * This example demonstrates the usage of the { @link IgniteCompute#execute(BroadcastJobTarget, JobDescriptor, Object)} API.
 *
 * <p>See {@code README.md} in the {@code examples} directory for execution instructions.</p>
 */


public class CodeDeploymentExample {

    /** Deployment unit name. */
    private static final String DEPLOYMENT_UNIT_NAME = "codeDeploymentExampleUnit";

    /** Deployment unit version. */
    private static final String DEPLOYMENT_UNIT_VERSION = "1.0.0";


    public static void main(String[] args) throws Exception {
        DeployComputeUnit.processDeploymentUnit(args);
        System.out.println("\nConnecting to server...");

        try (IgniteClient client = IgniteClient.builder().addresses("127.0.0.1:10800").build()) {

            System.out.println("\nConfiguring compute job...");

            deployIfNotExist(DEPLOYMENT_UNIT_NAME, DEPLOYMENT_UNIT_VERSION, DeployComputeUnit.getJarPath());

            JobDescriptor<String, String> job = JobDescriptor.builder(MyJob.class)
                    .units(new DeploymentUnit(DEPLOYMENT_UNIT_NAME, DEPLOYMENT_UNIT_VERSION)).resultClass(String.class).build();

            JobTarget target = JobTarget.anyNode(client.cluster().nodes());

            System.out.println("\nExecuting compute job'" + "'...");

            String result = client.compute().execute(target, job, "Hello from job");

            System.out.println("\n=== Result ===\n" + result);
        } finally {
            System.out.println("Cleaning up resources...");
            undeployUnit(DEPLOYMENT_UNIT_NAME, DEPLOYMENT_UNIT_VERSION);
        }
    }
}
