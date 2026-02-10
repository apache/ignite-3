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
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.compute.TaskDescriptor;
import org.apache.ignite.compute.task.MapReduceJob;
import org.apache.ignite.compute.task.MapReduceTask;
import org.apache.ignite.compute.task.TaskExecutionContext;
import org.apache.ignite.deployment.DeploymentUnit;

/**
 * This example demonstrates the usage of the
 * {@link IgniteCompute#executeMapReduce(TaskDescriptor, Object)} API.
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
 *         {@code cluster unit deploy receiverExampleUnit \
 *          --version 1.0.0 \
 *          --path=$IGNITE_HOME/examples/build/libs/ignite-examples-x.y.z.jar}
 *     </li>
 * </ol>
 */
public class ComputeMapReduceExample {
    /** Deployment unit name. */
    private static final String DEPLOYMENT_UNIT_NAME = "computeExampleUnit";

    /** Deployment unit version. */
    private static final String DEPLOYMENT_UNIT_VERSION = "1.0.0";

    /**
     * Main method of the example.
     *
     * @param args The command line arguments.
     */
    public static void main(String[] args) {
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

            System.out.println("\nConfiguring map reduce task...");


            TaskDescriptor<String, Integer> taskDescriptor = TaskDescriptor.builder(PhraseWordLengthCountMapReduceTask.class)
                    .units(new DeploymentUnit(DEPLOYMENT_UNIT_NAME, DEPLOYMENT_UNIT_VERSION))
                    .build();

            //--------------------------------------------------------------------------------------
            //
            // Executing map reduce task using configured taskDescriptor.
            //
            //--------------------------------------------------------------------------------------

            System.out.println("\nExecuting map reduce task...");

            String phrase = "Count characters using map reduce";

            Integer result = client.compute().executeMapReduce(taskDescriptor, phrase);

            //--------------------------------------------------------------------------------------
            //
            // Printing the result.
            //
            //--------------------------------------------------------------------------------------

            System.out.println("\nTotal number of characters in the words is '" + result + "'.");
        }
    }

    /**
     * MapReduce task that splits the input phrase into words and sends them to different nodes for the processing.
     */
    public static class PhraseWordLengthCountMapReduceTask implements MapReduceTask<String, String, Integer, Integer> {
        /** {@inheritDoc} */
        @Override
        public CompletableFuture<List<MapReduceJob<String, Integer>>> splitAsync(
                TaskExecutionContext taskContext,
                String input) {
            assert input != null;

            var job = JobDescriptor.builder(WordLengthJob.class)
                    .units(new DeploymentUnit(DEPLOYMENT_UNIT_NAME, DEPLOYMENT_UNIT_VERSION))
                    .build();

            List<MapReduceJob<String, Integer>> jobs = new ArrayList<>();

            for (String word : input.split(" ")) {
                jobs.add(
                        MapReduceJob.<String, Integer>builder()
                                .jobDescriptor(job)
                                .nodes(taskContext.ignite().cluster().nodes())
                                .args(word)
                                .build()
                );
            }

            return completedFuture(jobs);
        }

        /** {@inheritDoc} */
        @Override
        public CompletableFuture<Integer> reduceAsync(TaskExecutionContext taskContext, Map<UUID, Integer> results) {
            return completedFuture(results.values().stream()
                    .reduce(Integer::sum)
                    .orElseThrow());
        }
    }

    /**
     * Job that counts length of the provided word.
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
