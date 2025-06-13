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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.compute.JobTarget;
import org.apache.ignite.deployment.DeploymentUnit;

/**
 * This example demonstrates the usage of the
 * {@link IgniteCompute#executeAsync(JobTarget, JobDescriptor, Object)} API.
 *
 * <p>Find instructions on how to run the example in the README.md file located in the "examples" directory root.
 *
 * <p>The following steps related to code deployment should be additionally executed before running the current example:
 * <ol>
 *     <li>
 *         Build "ignite-examples-x.y.z.jar" using the next command:<br>
 *         {@code ./gradlew :ignite-examples:jar}
 *     </li>
 *     <li>
 *         Create a new deployment unit using the CLI tool:<br>
 *         {@code cluster unit deploy computeExampleUnit \
 *          --version 1.0.0 \
 *          --path=$IGNITE_HOME/examples/build/libs/ignite-examples-x.y.z.jar}
 *     </li>
 * </ol>
 */
public class ComputeAsyncExample {
    /** Deployment unit name. */
    private static final String DEPLOYMENT_UNIT_NAME = "computeExampleUnit";

    /** Deployment unit version. */
    private static final String DEPLOYMENT_UNIT_VERSION = "1.0.0";

    /**
     * Main method of the example.
     *
     * @param args The command line arguments.
     */
    public static void main(String[] args) throws ExecutionException, InterruptedException {
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

            JobDescriptor<String, Integer> job = JobDescriptor.builder(WordLengthJob.class)
                    .units(new DeploymentUnit(DEPLOYMENT_UNIT_NAME, DEPLOYMENT_UNIT_VERSION))
                    .build();

            JobTarget jobTarget = JobTarget.anyNode(client.cluster().nodes());

            //--------------------------------------------------------------------------------------
            //
            // Iterating through all words in the sentence and creating compute jobs.
            //
            //--------------------------------------------------------------------------------------

            Collection<CompletableFuture<Integer>> jobFutures = new ArrayList<>();

            String phrase = "Count characters using compute job";

            for (String word : phrase.split(" ")) {
                //--------------------------------------------------------------------------------------
                //
                // Executing compute job using configured jobTarget.
                //
                //--------------------------------------------------------------------------------------

                System.out.println("\nExecuting compute job for word '" + word + "'...");

                CompletableFuture<Integer> jobFuture = client.compute().executeAsync(jobTarget, job, word);

                jobFutures.add(jobFuture);
            }

            //--------------------------------------------------------------------------------------
            //
            // Waiting for the compute jobs to be completed.
            //
            //--------------------------------------------------------------------------------------

            List<Integer> results = jobFutures.stream()
                    .map(CompletableFuture::join)
                    .collect(Collectors.toList());

            int sum = results.stream().mapToInt(i -> i).sum();

            //--------------------------------------------------------------------------------------
            //
            // Printing the result.
            //
            //--------------------------------------------------------------------------------------

            System.out.println("\nTotal number of characters in the words is '" + sum + "'.");
        }
    }

    /**
     * Job that calculates length of the provided word.
     */
    private static class WordLengthJob implements ComputeJob<String, Integer> {
        /** {@inheritDoc} */
        @Override
        public CompletableFuture<Integer> executeAsync(JobExecutionContext context, String arg) {
            assert arg != null;

            System.out.println("\nProcessing word '" + arg + "' at node '" + context.ignite().name() + "'.");

            return completedFuture(arg.length());
        }
    }
}
