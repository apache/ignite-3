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
import static org.apache.ignite.example.util.DeployComputeUnit.deployUnit;
import static org.apache.ignite.example.util.DeployComputeUnit.deploymentExists;
import static org.apache.ignite.example.util.DeployComputeUnit.undeployUnit;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.compute.JobTarget;
import org.apache.ignite.deployment.DeploymentUnit;
import org.apache.ignite.example.code.deployment.AbstractDeploymentUnitExample;
import org.apache.ignite.lang.CancelHandle;
import org.apache.ignite.lang.CancellationToken;

/**
 * This example demonstrates the usage of the
 * {@link IgniteCompute#executeAsync(JobTarget, JobDescriptor, Object, CancellationToken)} API.
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

public class ComputeCancellationExample  extends AbstractDeploymentUnitExample {
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

            // 1) Check if deployment unit already exists
            if (deploymentExists(DEPLOYMENT_UNIT_NAME, DEPLOYMENT_UNIT_VERSION)) {
                System.out.println("Deployment unit already exists. Skip deploy.");
            } else {
                System.out.println("Deployment unit not found. Deploying...");
                deployUnit(DEPLOYMENT_UNIT_NAME, DEPLOYMENT_UNIT_VERSION, jarPath);
                System.out.println(" Deployment completed " + DEPLOYMENT_UNIT_NAME + ".");
            }

            JobDescriptor<Object, Void> job = JobDescriptor.builder(InfiniteJob.class)
                    .units(new DeploymentUnit(DEPLOYMENT_UNIT_NAME, DEPLOYMENT_UNIT_VERSION))
                    .build();

            JobTarget jobTarget = JobTarget.anyNode(client.cluster().nodes());

            //--------------------------------------------------------------------------------------
            //
            // Creating CancelHandle.
            //
            //--------------------------------------------------------------------------------------

            CancelHandle cancelHandle = CancelHandle.create();

            //--------------------------------------------------------------------------------------
            //
            // Executing compute job using configured jobTarget.
            //
            //--------------------------------------------------------------------------------------

            System.out.println("\nExecuting compute job...");

            CompletableFuture<Void> resultFuture = client.compute().executeAsync(jobTarget, job, null, cancelHandle.token());

            //--------------------------------------------------------------------------------------
            //
            // Cancelling compute job execution.
            //
            //--------------------------------------------------------------------------------------

            System.out.println("\nCancelling compute job...");

            cancelHandle.cancel();

            try {
                resultFuture.join();
            } catch (CompletionException ex) {
                System.out.println("\nThe compute job was cancelled: " + ex.getMessage());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {

            System.out.println("Cleaning up resources");
            undeployUnit(DEPLOYMENT_UNIT_NAME, DEPLOYMENT_UNIT_VERSION);


        }
    }

    /**
     * The job to interrupt.
     */
    public static class InfiniteJob implements ComputeJob<Object, Void> {
        /** {@inheritDoc} */
        @Override
        public CompletableFuture<Void> executeAsync(JobExecutionContext context, Object arg) {
            try {
                new CountDownLatch(1).await();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            return completedFuture(null);
        }
    }
}
