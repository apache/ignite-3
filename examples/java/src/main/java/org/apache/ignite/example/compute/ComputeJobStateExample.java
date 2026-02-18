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

package org.apache.ignite.example.compute;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.compute.JobStatus.FAILED;
import static org.apache.ignite.example.util.DeployComputeUnit.deployIfNotExist;
import static org.apache.ignite.example.util.DeployComputeUnit.undeployUnit;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.compute.JobTarget;
import org.apache.ignite.deployment.DeploymentUnit;
import org.apache.ignite.example.code.deployment.AbstractDeploymentUnitExample;

/**
 * This code demonstrates the usage of the {@link JobExecution} interface that allows to get job statuses and, for example, handle
 * failures.
 *
 * <p>See {@code README.md} in the {@code examples} directory for execution instructions.</p>
 */

public class ComputeJobStateExample {

    /** Deployment unit name. */
    private static final String DEPLOYMENT_UNIT_NAME = "computeExampleUnit";

    /** Deployment unit version. */
    private static final String DEPLOYMENT_UNIT_VERSION = "1.0.0";

    /**
     * Main method of the example.
     *
     * @param args The command line arguments.
     * @throws Exception if any error occurs.
     */
    public static void main(String[] args) throws Exception {

        AbstractDeploymentUnitExample.processDeploymentUnit(args);

        //--------------------------------------------------------------------------------------
        //
        // Creating a client to connect to the cluster.
        //
        //--------------------- -----------------------------------------------------------------

        System.out.println("\nConnecting to server...");

        try (IgniteClient client = IgniteClient.builder()
                .addresses("127.0.0.1:10800")
                .build()
        ) {
            //--------------------------------------------------------------------------------------
            //
            // Configuring compute job.
            //
            //--------------------------------------------------------------------------------------

            System.out.println("\nConfiguring compute job...");

            deployIfNotExist(DEPLOYMENT_UNIT_NAME, DEPLOYMENT_UNIT_VERSION, AbstractDeploymentUnitExample.getJarPath());

            CompletableFuture<JobExecution<Void>> execution = client.compute().submitAsync(JobTarget.anyNode(client.cluster().nodes()),
                    JobDescriptor.builder(WordPrintJob.class)
                            .units(new DeploymentUnit(DEPLOYMENT_UNIT_NAME, DEPLOYMENT_UNIT_VERSION))
                            .build(), null
            );

            execution.get().stateAsync().thenApply(state -> {
                if (state.status() == FAILED) {
                    System.out.println("\nJob failed...");
                }
                return null;
            });
        } finally {

            System.out.println("Cleaning up resources");
            undeployUnit(DEPLOYMENT_UNIT_NAME, DEPLOYMENT_UNIT_VERSION);

        }
    }

    /**
     * Job that prints provided word.
     */
    public static class WordPrintJob implements ComputeJob<String, Void> {
        /** {@inheritDoc} */
        @Override
        public CompletableFuture<Void> executeAsync(JobExecutionContext context, String arg) {
            System.out.println("\nProcessing word '" + arg + "' at node '" + context.ignite().name() + "'.");

            return completedFuture(null);
        }
    }
}
