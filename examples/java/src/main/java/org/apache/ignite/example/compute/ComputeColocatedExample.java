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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.compute.JobTarget;
import org.apache.ignite.deployment.DeploymentUnit;
import org.apache.ignite.example.code.deployment.AbstractDeploymentUnitExample;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;

/**
 * This example demonstrates the usage of the { @link IgniteCompute#execute(JobTarget, JobDescriptor, Object)} API with colocated JobTarget.
 *
 * <p>See {@code README.md} in the {@code examples} directory for execution instructions.</p>
 */

public class ComputeColocatedExample {
    /** Number of accounts to load. */
    private static final int ACCOUNTS_COUNT = 100;

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
        //--------------------------------------------------------------------------------------

        System.out.println("\nConnecting to server...");

        try (IgniteClient client = IgniteClient.builder()
                .addresses("127.0.0.1:10800")
                .build()
        ) {

            try {
                //--------------------------------------------------------------------------------------
                //
                // Creating table.
                //
                //--------------------------------------------------------------------------------------

                client.sql().executeScript(
                        "CREATE TABLE accounts ("
                                + "accountNumber INT PRIMARY KEY,"
                                + "name          VARCHAR)"
                );

                //--------------------------------------------------------------------------------------
                //
                // Creating a record view for the 'accounts' table.
                //
                //--------------------------------------------------------------------------------------

                RecordView<Tuple> view = client.tables().table("accounts").recordView();

                //--------------------------------------------------------------------------------------
                //
                // Creating account records.
                //
                //--------------------------------------------------------------------------------------

                System.out.println("\nCreating account records...");

                for (int i = 0; i < ACCOUNTS_COUNT; i++) {
                    view.insert(null, account(i));
                }

                //--------------------------------------------------------------------------------------
                //
                // Configuring compute job.
                //
                //--------------------------------------------------------------------------------------

                System.out.println("\nConfiguring compute job...");

                deployIfNotExist(DEPLOYMENT_UNIT_NAME, DEPLOYMENT_UNIT_VERSION, AbstractDeploymentUnitExample.getJarPath());

                JobDescriptor<Integer, Void> job = JobDescriptor.builder(PrintAccountInfoJob.class)
                        .units(new DeploymentUnit(DEPLOYMENT_UNIT_NAME, DEPLOYMENT_UNIT_VERSION))
                        .build();

                int accountNumber = ThreadLocalRandom.current().nextInt(ACCOUNTS_COUNT);

                JobTarget jobTarget = JobTarget.colocated("accounts", accountKey(accountNumber));

                //--------------------------------------------------------------------------------------
                //
                // Executing compute job for the specific accountNumber.
                //
                //--------------------------------------------------------------------------------------

                System.out.println("\nExecuting compute job for the accountNumber '" + accountNumber + "'...");

                client.compute().execute(jobTarget, job, accountNumber);
            } finally {

                System.out.println("Cleaning up resources");
                undeployUnit(DEPLOYMENT_UNIT_NAME, DEPLOYMENT_UNIT_VERSION);

                /* Drop table */
                System.out.println("\nDropping the table...");

                client.sql().executeScript("DROP TABLE IF EXISTS accounts");
            }

        }
    }

    /**
     * Job that prints account info for the provided accountNumber.
     */
    public static class PrintAccountInfoJob implements ComputeJob<Integer, Void> {
        /** {@inheritDoc} */
        @Override
        public CompletableFuture<Void> executeAsync(JobExecutionContext context, Integer arg) {
            assert arg != null;

            Tuple accountKey = accountKey(arg);

            RecordView<Tuple> view = context.ignite().tables().table("accounts").recordView();

            Tuple account = view.get(null, accountKey);

            System.out.println("Account info [accountNumber=" + account.intValue(0) +
                    ", name=" + account.stringValue(1) + "]");

            return completedFuture(null);
        }
    }

    /**
     * Creates account Tuple.
     *
     * @param accountNumber Account number.
     * @return Tuple.
     */
    private static Tuple account(int accountNumber) {
        return Tuple.create()
                .set("accountNumber", accountNumber)
                .set("name", "name" + ThreadLocalRandom.current().nextInt());
    }

    /**
     * Creates account key Tuple.
     *
     * @param accountNumber Account number.
     * @return Tuple.
     */
    private static Tuple accountKey(int accountNumber) {
        return Tuple.create()
                .set("accountNumber", accountNumber);
    }
}
