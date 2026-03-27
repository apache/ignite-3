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
import static org.apache.ignite.compute.BroadcastJobTarget.table;
import static org.apache.ignite.example.util.DeployComputeUnit.deployIfNotExist;
import static org.apache.ignite.example.util.DeployComputeUnit.undeployUnit;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.compute.BroadcastJobTarget;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.deployment.DeploymentUnit;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.table.QualifiedName;
import org.apache.ignite.table.partition.Partition;
import org.apache.ignite.tx.Transaction;

/**
 * This example demonstrates the usage of the {@link IgniteCompute#execute(BroadcastJobTarget, JobDescriptor, Object)} API.
 *
 * <p>It also shows how to use {@link BroadcastJobTarget#table} to execute a partition-aware job on every node holding
 * a partition of a table. Each job instance reads {@link JobExecutionContext#partition()} to discover its partition and
 * filters rows with the {@code __PARTITION_ID} virtual SQL column, guaranteeing local query execution without
 * cross-node data movement.
 *
 * <p>See {@code README.md} in the {@code examples} directory for execution instructions.</p>
 */
public class ComputeBroadcastExample {
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

        //--------------------------------------------------------------------------------------
        //
        // Creating a client to connect to the cluster.
        //
        //--------------------------------------------------------------------------------------

        System.out.println("Connecting to server...");

        try (IgniteClient client = IgniteClient.builder()
                .addresses("127.0.0.1:10800")
                .build()
        ) {

            try {

                //--------------------------------------------------------------------------------------
                //
                // Prerequisites for the example:
                // 1. Create table and schema for the example.
                // 2. Create a new deployment unit for the compute job.
                //
                //--------------------------------------------------------------------------------------

                setupTablesAndSchema(client);
                deployIfNotExist(DEPLOYMENT_UNIT_NAME, DEPLOYMENT_UNIT_VERSION);

                //--------------------------------------------------------------------------------------
                //
                // Configuring compute job.
                //
                //--------------------------------------------------------------------------------------

                System.out.println("Configuring compute job...");

                JobDescriptor<String, Void> job = JobDescriptor.builder(HelloMessageJob.class)
                        .units(new DeploymentUnit(DEPLOYMENT_UNIT_NAME, DEPLOYMENT_UNIT_VERSION))
                        .build();

                BroadcastJobTarget target = table("Person");

                //--------------------------------------------------------------------------------------
                //
                // Executing compute job using configured jobTarget.
                //
                //--------------------------------------------------------------------------------------

                System.out.println("Executing compute job...");

                client.compute().execute(target, job, "John");

                System.out.println("Compute job executed...");

                //--------------------------------------------------------------------------------------
                //
                // Executing compute job using a custom by specifying a fully qualified table name .
                //
                //

                QualifiedName customSchemaTable = QualifiedName.parse("CUSTOM_SCHEMA.MY_QUALIFIED_TABLE");
                client.compute().execute(table(customSchemaTable),
                        JobDescriptor.builder(HelloMessageJob.class)
                                .units(new DeploymentUnit(DEPLOYMENT_UNIT_NAME, DEPLOYMENT_UNIT_VERSION))
                                .build(), null
                );

                QualifiedName customSchemaTableName = QualifiedName.of("PUBLIC", "MY_TABLE");
                client.compute().execute(table(customSchemaTableName),
                        JobDescriptor.builder(HelloMessageJob.class)
                                .units(new DeploymentUnit(DEPLOYMENT_UNIT_NAME, DEPLOYMENT_UNIT_VERSION))
                                .build(), null
                );

                //--------------------------------------------------------------------------------------
                //
                // Executing a partition-aware compute job using BroadcastJobTarget.table().
                //
                // One instance of PartitionQueryJob runs on each node that holds a partition of the
                // Person table. Each instance reads context.partition() to determine its partition,
                // then queries only that partition's rows via the __PARTITION_ID virtual SQL column.
                // This guarantees local execution: no row is fetched from a remote node.
                //
                // The results (one Long per partition) are collected and summed on the client side.
                //
                //--------------------------------------------------------------------------------------

                System.out.println("Executing partition-aware compute job...");

                JobDescriptor<Void, Long> partitionQueryJob = JobDescriptor.builder(PartitionQueryJob.class)
                        .units(new DeploymentUnit(DEPLOYMENT_UNIT_NAME, DEPLOYMENT_UNIT_VERSION))
                        .build();

                Collection<Long> partitionCounts = client.compute().execute(table("Person"), partitionQueryJob, null);

                long totalPersons = partitionCounts.stream().mapToLong(Long::longValue).sum();

                System.out.println("Total person count across all partitions: " + totalPersons);
            } finally {

                System.out.println("Cleaning up resources");
                undeployUnit(DEPLOYMENT_UNIT_NAME, DEPLOYMENT_UNIT_VERSION);

                // Drop tables
                System.out.println("Dropping the tables...");

                client.sql().executeScript("DROP TABLE IF EXISTS Person");
                client.sql().executeScript("DROP TABLE IF EXISTS PUBLIC.MY_TABLE");
                client.sql().executeScript("DROP SCHEMA IF EXISTS CUSTOM_SCHEMA CASCADE");
            }
        }
    }

    /**
     * Sets up the tables and schema required for the broadcast example.
     *
     * <p>This setup ensures the example is self-contained and can run
     * without external dependencies, enabling automated execution.</p>
     *
     * @param client The Ignite client to use for executing SQL statements.
     */
    private static void setupTablesAndSchema(IgniteClient client) {
        client.sql().executeScript("DROP SCHEMA IF EXISTS CUSTOM_SCHEMA CASCADE");
        client.sql().executeScript("CREATE SCHEMA CUSTOM_SCHEMA");
        client.sql().executeScript("CREATE TABLE CUSTOM_SCHEMA.MY_QUALIFIED_TABLE ("
                + "ID INT PRIMARY KEY, MESSAGE VARCHAR(255))");

        client.sql().executeScript("CREATE SCHEMA IF NOT EXISTS PUBLIC");
        client.sql().executeScript("CREATE TABLE IF NOT EXISTS PUBLIC.MY_TABLE ("
                + "ID INT PRIMARY KEY, MESSAGE VARCHAR(255))");

        client.sql().executeScript("CREATE TABLE PERSON ("
                + "ID INT PRIMARY KEY, FIRST_NAME VARCHAR(100),"
                + "LAST_NAME VARCHAR(100), AGE INT)");

        client.sql().executeScript("INSERT INTO PERSON VALUES "
                + "(1, 'John', 'Doe', 36),"
                + "(2, 'Jane', 'Smith', 35),"
                + "(3, 'Robert', 'Johnson', 25)");
    }

    /**
     * Job that prints hello message with provided name.
     */
    public static class HelloMessageJob implements ComputeJob<String, Void> {
        /** {@inheritDoc} */
        @Override
        public CompletableFuture<Void> executeAsync(JobExecutionContext context, String arg) {
            System.out.println("Hello " + arg + "!");

            return completedFuture(null);
        }
    }

    /**
     * Job that counts persons in a single table partition using the {@code __PARTITION_ID} virtual SQL column.
     *
     * <p>This job is designed for use with {@link BroadcastJobTarget#table}, which routes one instance to each node
     * that holds a partition of the target table. Each instance calls {@link JobExecutionContext#partition()} to
     * identify its partition and filters rows by {@code __PARTITION_ID}, so the SQL query reads only local data.
     */
    public static class PartitionQueryJob implements ComputeJob<Void, Long> {
        /** {@inheritDoc} */
        @Override
        public CompletableFuture<Long> executeAsync(JobExecutionContext context, Void arg) {
            Partition partition = context.partition();

            assert partition != null : "Partition must be non-null when using BroadcastJobTarget.table()";

            long count = 0;

            try (ResultSet<SqlRow> rs = context.ignite().sql().execute(
                    (Transaction) null,
                    "SELECT COUNT(*) FROM Person WHERE __PARTITION_ID = ?",
                    partition.id()
            )) {
                if (rs.hasNext()) {
                    count = rs.next().longValue(0);
                }
            }

            System.out.println("Partition " + partition.id() + " on node '" + context.ignite().name() + "': " + count + " person(s).");

            return completedFuture(count);
        }
    }
}
