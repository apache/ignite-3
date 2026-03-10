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
import static org.apache.ignite.example.util.DeployComputeUnit.deployIfNotExist;
import static org.apache.ignite.example.util.DeployComputeUnit.undeployUnit;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.compute.JobTarget;
import org.apache.ignite.deployment.DeploymentUnit;
import org.apache.ignite.example.util.DeployComputeUnit;
import org.apache.ignite.marshalling.ByteArrayMarshaller;
import org.apache.ignite.marshalling.Marshaller;

/**
 * This example demonstrates the usage of the {@link IgniteCompute#execute} API with a custom result marshaller.
 *
 * <p>See {@code README.md} in the {@code examples} directory for execution instructions.</p>
 */

public class ComputeWithCustomResultMarshallerExample {
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

        DeployComputeUnit.processDeploymentUnit(args);

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


            deployIfNotExist(DEPLOYMENT_UNIT_NAME, DEPLOYMENT_UNIT_VERSION, DeployComputeUnit.getJarPath());

            JobDescriptor<String, WordInfoResult> job = JobDescriptor.builder(WordInfoJob.class)
                    .resultMarshaller(new WordInfoResultMarshaller())
                    .units(new DeploymentUnit(DEPLOYMENT_UNIT_NAME, DEPLOYMENT_UNIT_VERSION))
                    .build();

            JobTarget jobTarget = JobTarget.anyNode(client.cluster().nodes());

            ArrayList<WordInfoResult> results = new ArrayList<>();

            String phrase = "Count characters using compute job";

            for (String word : phrase.split(" ")) {
                //--------------------------------------------------------------------------------------
                //
                // Executing compute job using configured jobTarget.
                //
                //--------------------------------------------------------------------------------------

                System.out.println("\nExecuting compute job for the word '" + word + "'...");

                WordInfoResult result = client.compute().execute(jobTarget, job, word);

                results.add(result);
            }

            //--------------------------------------------------------------------------------------
            //
            // Printing the results.
            //
            //--------------------------------------------------------------------------------------

            for (WordInfoResult result : results)
                System.out.println("The length of the word '" + result.word + "'" + " is " + result.length + ".");
        } finally {

            System.out.println("Cleaning up resources");
            undeployUnit(DEPLOYMENT_UNIT_NAME, DEPLOYMENT_UNIT_VERSION);

        }
    }

    /** Marshals WordInfoResult into a byte array. */
    public static class WordInfoResultMarshaller implements ByteArrayMarshaller<WordInfoResult> {
    }

    /**
     * The result object for the WordInfoJob.
     */
    public static class WordInfoResult implements Serializable {
        /** Serial version UID. */
        private static final long serialVersionUID = -6036698690089270464L;

        /** Word. */
        String word;

        /** Length. */
        int length;

        /**
         * Constructor.
         */
        public WordInfoResult() {
        }

        /**
         * Constructor.
         *
         * @param word Word.
         * @param length Length.
         */
        WordInfoResult(String word, int length) {
            this.word = word;
            this.length = length;
        }

        /** {@inheritDoc} */
        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            WordInfoResult result = (WordInfoResult) o;

            if (length != result.length) {
                return false;
            }
            return word != null ? word.equals(result.word) : result.word == null;
        }

        /** {@inheritDoc} */
        @Override
        public int hashCode() {
            int result = word != null ? word.hashCode() : 0;
            result = 31 * result + length;
            return result;
        }

        /** {@inheritDoc} */
        @Override
        public String toString() {
            return "WordInfoResult{" +
                    "word='" + word + '\'' +
                    ", length=" + length +
                    '}';
        }
    }

    /**
     * Job that calculates length of the provided word.
     */
    public static class WordInfoJob implements ComputeJob<String, WordInfoResult> {
        /** {@inheritDoc} */
        @Override
        public CompletableFuture<WordInfoResult> executeAsync(JobExecutionContext context, String arg) {
            assert arg != null;

            System.out.println("\nProcessing word '" + arg + "' at node '" + context.ignite().name() + "'.");

            WordInfoResult result = new WordInfoResult(arg, arg.length());

            return completedFuture(result);
        }

        /** {@inheritDoc} */
        @Override
        public Marshaller<WordInfoResult, byte[]> resultMarshaller() {
            return new WordInfoResultMarshaller();
        }
    }
}
