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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.table.partition.Partition;
import org.apache.ignite.table.partition.PartitionDistribution;

/**
 * This example demonstrates a partition-aware Map/Reduce pattern using the {@link IgniteCompute#executeMapReduce} API
 * together with the {@code __PARTITION_ID} virtual SQL column.
 *
 * <p>In the split phase, {@link PersonCountByPartitionTask} enumerates all partitions of the {@code Person} table
 * via {@link PartitionDistribution} and dispatches one job per partition to the node that currently holds the primary
 * replica. Each {@link PartitionPersonCountJob} filters rows with {@code __PARTITION_ID} and returns the local count.
 * In the reduce phase the counts are summed.
 *
 * <p><b>Note:</b> {@link PartitionDistribution#primaryReplicas()} captures partition locations at a point in time.
 * If a partition is reassigned between the split and job execution, a job may run on a non-primary node, making the
 * SQL query non-local. For guaranteed local execution use {@link org.apache.ignite.compute.BroadcastJobTarget#table}
 * instead — see {@code ComputeBroadcastExample} for that approach.
 *
 * <p>See {@code README.md} in the {@code examples} directory for execution instructions.
 */
public class ComputePartitionQueryMapReduceExample {
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

        try (IgniteClient client = IgniteClient.builder().addresses("127.0.0.1:10800").build()) {
            try {

                //--------------------------------------------------------------------------------------
                //
                // Prerequisites for the example:
                // 1. Create table and insert sample data.
                // 2. Deploy the compute unit that contains the job classes.
                //
                //--------------------------------------------------------------------------------------

                client.sql().executeScript(
                        "CREATE TABLE IF NOT EXISTS Person ("
                                + "ID INT PRIMARY KEY, FIRST_NAME VARCHAR(100),"
                                + "LAST_NAME VARCHAR(100), AGE INT)"
                );

                client.sql().executeScript(
                        "INSERT INTO Person VALUES "
                                + "(1, 'John', 'Doe', 36),"
                                + "(2, 'Jane', 'Smith', 35),"
                                + "(3, 'Robert', 'Johnson', 25)"
                );

                deployIfNotExist(DEPLOYMENT_UNIT_NAME, DEPLOYMENT_UNIT_VERSION);

                //--------------------------------------------------------------------------------------
                //
                // Executing partition-aware map reduce task.
                //
                // PersonCountByPartitionTask splits work by enumerating all table partitions and
                // dispatching one PartitionPersonCountJob per partition to its primary node.
                // Each job queries its partition locally using the __PARTITION_ID virtual column.
                // The reduce phase sums the per-partition counts into a single total.
                //
                //--------------------------------------------------------------------------------------

                System.out.println("Configuring partition-aware map reduce task...");

                TaskDescriptor<Void, Long> taskDescriptor = TaskDescriptor
                        .builder(PersonCountByPartitionTask.class)
                        .units(new DeploymentUnit(DEPLOYMENT_UNIT_NAME, DEPLOYMENT_UNIT_VERSION))
                        .build();

                System.out.println("Executing partition-aware map reduce task...");

                Long totalCount = client.compute().executeMapReduce(taskDescriptor, null);

                System.out.println("Total person count across all partitions: " + totalCount);

            } finally {

                System.out.println("Cleaning up resources");
                undeployUnit(DEPLOYMENT_UNIT_NAME, DEPLOYMENT_UNIT_VERSION);

                System.out.println("Dropping the table...");
                client.sql().executeScript("DROP TABLE IF EXISTS Person");
            }
        }
    }

    /**
     * MapReduce task that counts persons across all partitions of the {@code Person} table.
     *
     * <p>The split phase uses {@link PartitionDistribution#primaryReplicas()} to get the current primary replica node
     * for each partition, then creates one {@link PartitionPersonCountJob} per partition targeted at that node.
     * The reduce phase sums the per-partition counts.
     */
    public static class PersonCountByPartitionTask implements MapReduceTask<Void, Long, Long, Long> {
        /** {@inheritDoc} */
        @Override
        public CompletableFuture<List<MapReduceJob<Long, Long>>> splitAsync(
                TaskExecutionContext taskContext,
                Void input) {
            // Run a SQL query to advance the node's observable timestamp tracker to the current
            // server time. MapReduce jobs are submitted server-side using the node's own tracker
            // (not the client's). Without this step, individual jobs may use a read timestamp
            // that predates inserts committed by the client before the task was submitted.
            try (ResultSet<SqlRow> rs = taskContext.ignite().sql().execute("SELECT COUNT(*) FROM Person")) {
                while (rs.hasNext()) {
                    rs.next();
                }
            }

            JobDescriptor<Long, Long> jobDescriptor = JobDescriptor.builder(PartitionPersonCountJob.class)
                    .units(new DeploymentUnit(DEPLOYMENT_UNIT_NAME, DEPLOYMENT_UNIT_VERSION))
                    .build();

            PartitionDistribution distribution = taskContext.ignite().tables()
                    .table("Person")
                    .partitionDistribution();

            Map<Partition, ClusterNode> primaryReplicas = distribution.primaryReplicas();

            List<MapReduceJob<Long, Long>> jobs = new ArrayList<>();

            for (Map.Entry<Partition, ClusterNode> entry : primaryReplicas.entrySet()) {
                jobs.add(
                        MapReduceJob.<Long, Long>builder()
                                .jobDescriptor(jobDescriptor)
                                .nodes(Set.of(entry.getValue()))
                                .args(entry.getKey().id())
                                .build()
                );
            }

            return completedFuture(jobs);
        }

        /** {@inheritDoc} */
        @Override
        public CompletableFuture<Long> reduceAsync(TaskExecutionContext taskContext, Map<UUID, Long> results) {
            long total = results.values().stream().mapToLong(Long::longValue).sum();

            return completedFuture(total);
        }
    }

    /**
     * Job that counts persons in a single partition, identified by partition ID passed as the job argument.
     *
     * <p>The {@code __PARTITION_ID} virtual SQL column is used to filter rows to those belonging to the target
     * partition. The partition ID is provided by {@link PersonCountByPartitionTask} during the split phase.
     */
    public static class PartitionPersonCountJob implements ComputeJob<Long, Long> {
        /** {@inheritDoc} */
        @Override
        public CompletableFuture<Long> executeAsync(JobExecutionContext context, Long partitionId) {
            assert partitionId != null;

            long count = 0;

            try (ResultSet<SqlRow> rs = context.ignite().sql().execute(
                    "SELECT COUNT(*) FROM Person WHERE __PARTITION_ID = ?",
                    partitionId
            )) {
                if (rs.hasNext()) {
                    count = rs.next().longValue(0);
                }
            }

            System.out.println("Partition " + partitionId + " on node '" + context.ignite().name() + "': " + count + " person(s).");

            return completedFuture(count);
        }
    }
}
