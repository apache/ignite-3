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

import static java.sql.DriverManager.getConnection;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.example.util.DeployComputeUnit.deployUnit;
import static org.apache.ignite.example.util.DeployComputeUnit.deploymentExists;
import static org.apache.ignite.example.util.DeployComputeUnit.undeployUnit;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.compute.JobExecutionOptions;
import org.apache.ignite.compute.JobTarget;
import org.apache.ignite.deployment.DeploymentUnit;
import org.apache.ignite.example.code.deployment.AbstractDeploymentUnitExample;

/**
 * This example demonstrates the usage of the
 * {@link IgniteCompute#execute(JobTarget, JobDescriptor, Object)} API with different job priorities.
 *
 * <p>Find instructions on how to run the example in the <code>README.md</code>
 * file located in the "examples" directory root.</p>
 *
 * <h2>Execution Modes</h2>
 *
 * <p>There are two modes of execution:</p>
 *
 * <h3>1. Automated : The JAR Deployment for  deployment unit is automated </h3>
 *
 * <h4>1.1 With IDE</h4>
 * <ul>
 *     <li>
 *         <b>Run from an IDE</b><br>
 *         Launch the example directly from the IDE. If the required deployment
 *         unit is not present, the example automatically builds and deploys the
 *         necessary JAR.
 *     </li>
 * </ul>
 *
 * <h4>1.2 Without IDE</h4>
 * <ul>
 *     <li>
 *         <b>Run from the command line</b><br>
 *         Start the example using a Java command where the classpath includes all required
 *         dependencies:<br>
 *         {@code
 *         java -cp "{user.home}\.m2\repository\org\apache\ignite\ignite-core\3.1.0-SNAPSHOT\
 *         ignite-core-3.1.0-SNAPSHOT.jar{other required jars}"
 *         <example-main-class> runFromIDE=false jarPath="{path-to-examples-jar}"}
 *         <br>
 *         In this mode, {@code runFromIDE=false} indicates command-line execution, and
 *         {@code jarPath} must reference the examples JAR used as the deployment unit.
 *     </li>
 * </ul>
 *
 * <h3>2. Manual (with IDE) :  The JAR Deployment for  deployment unit is manual</h3>
 *
 * <p>Before running this example, complete the following steps related to
 * code deployment:</p>
 *
 * <ol>
 *     <li>
 *         Build the <code>ignite-examples-x.y.z.jar</code> file:<br>
 *         {@code ./gradlew :ignite-examples:jar}
 *     </li>
 *     <li>
 *         Deploy the generated JAR as a deployment unit using the CLI:<br>
 *         {@code
 *         cluster unit deploy computeExampleUnit \
 *         --version 1.0.0 \
 *         --path=$IGNITE_HOME/examples/build/libs/ignite-examples-x.y.z.jar}
 *     </li>
 * </ol>
 */

public class ComputeJobPriorityExample extends AbstractDeploymentUnitExample {
    /** Deployment unit name. */
    private static final String DEPLOYMENT_UNIT_NAME = "computeExampleUnit";

    /** Deployment unit version. */
    private static final String DEPLOYMENT_UNIT_VERSION = "1.0.0";

    /**
     * Main method of the example.
     *
     * @param args The command line arguments.
     */
    public static void main(String[] args) throws Exception {

        processDeploymentUnit(args);

        //--------------------------------------------------------------------------------------
        //
        // Creating a client to connect to the cluster.
        //
        //--------------------------------------------------------------------------------------

        System.out.println("\nConnecting to server...");

        try (IgniteClient client = IgniteClient.builder().addresses("127.0.0.1:10800").build()) {
            //--------------------------------------------------------------------------------------
            //
            // Configuring compute job.
            //
            //--------------------------------------------------------------------------------------

            System.out.println("\nConfiguring compute job...");

            // 1) Check if deployment unit already exists
            if (deploymentExists(DEPLOYMENT_UNIT_NAME, DEPLOYMENT_UNIT_VERSION)) {
                System.out.println("Deployment unit already exists. Skip deploy.");
            } else {
                System.out.println("Deployment unit not found. Deploying...");
                deployUnit(DEPLOYMENT_UNIT_NAME, DEPLOYMENT_UNIT_VERSION, jarPath);
                System.out.println(" Deployment completed " + DEPLOYMENT_UNIT_NAME + ".");
            }

            JobDescriptor<Integer, String> lowPriorityJob = JobDescriptor.builder(LowPriorityJob.class)
                    .options(JobExecutionOptions.builder().priority(0).maxRetries(5).build())
                    .units(new DeploymentUnit(DEPLOYMENT_UNIT_NAME, DEPLOYMENT_UNIT_VERSION))
                    .build();

            JobDescriptor<Integer, String> highPriorityJob = JobDescriptor.builder(HighPriorityJob.class)
                    .options(JobExecutionOptions.builder().priority(1).build())
                    .units(new DeploymentUnit(DEPLOYMENT_UNIT_NAME, DEPLOYMENT_UNIT_VERSION))
                    .build();

            JobTarget jobTarget = JobTarget.anyNode(client.cluster().nodes());

            Collection<CompletableFuture<Void>> jobFutures = new ArrayList<>();

            for (int i = 0; i < 100; i++) {
                //--------------------------------------------------------------------------------------
                //
                // Executing compute job using configured jobTarget.
                //
                //--------------------------------------------------------------------------------------

                System.out.println("\nExecuting compute jobs for arg '" + i + "'...");

                CompletableFuture<Void> lowPriorityJobFuture = client.compute().executeAsync(jobTarget, lowPriorityJob, i)
                        .thenAccept(System.out::println);

                jobFutures.add(lowPriorityJobFuture);

                CompletableFuture<Void> highPriorityJobFuture = client.compute().executeAsync(jobTarget, highPriorityJob, i)
                        .thenAccept(System.out::println);

                jobFutures.add(highPriorityJobFuture);
            }

            //--------------------------------------------------------------------------------------
            //
            // Waiting for the compute jobs to be completed.
            //
            //--------------------------------------------------------------------------------------

            CompletableFuture.allOf(jobFutures.toArray(new CompletableFuture[0])).join();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {

            System.out.println("Cleaning up resources");
            undeployUnit(DEPLOYMENT_UNIT_NAME, DEPLOYMENT_UNIT_VERSION);
        }
    }

    /**
     * High-priority job.
     */
    public static class HighPriorityJob implements ComputeJob<Integer, String> {
        /** {@inheritDoc} */
        @Override
        public CompletableFuture<String> executeAsync(JobExecutionContext context, Integer arg) {
            System.out.println("\nHighPriorityJob started for arg '" + arg + "' at node '" + context.ignite().name() + "'.");

            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            String result = "HighPriorityJob [arg=" + arg + ", res=" + arg * 100 + "]";

            System.out.println("\nHighPriorityJob finished for arg '" + arg + "' at node '" + context.ignite().name() + "'.");

            return completedFuture(result);
        }
    }

    /**
     * Low-priority job.
     */
    public static class LowPriorityJob implements ComputeJob<Integer, String> {
        /** {@inheritDoc} */
        @Override
        public CompletableFuture<String> executeAsync(JobExecutionContext context, Integer arg) {
            System.out.println("\nLowPriorityJob started for arg '" + arg + "' at node '" + context.ignite().name() + "'.");

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            String result = "LowPriorityJob [arg=" + arg + ", res=" + arg * 10 + "]";

            System.out.println("\nLowPriorityJob finished for arg '" + arg + "' at node '" + context.ignite().name() + "'.");

            return completedFuture(result);
        }
    }
}
