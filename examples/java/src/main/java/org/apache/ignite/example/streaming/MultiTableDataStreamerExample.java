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

package org.apache.ignite.example.streaming;

import static org.apache.ignite.example.util.DeployComputeUnit.deployIfNotExist;
import static org.apache.ignite.example.util.DeployComputeUnit.undeployUnit;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.SubmissionPublisher;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.deployment.DeploymentUnit;
import org.apache.ignite.example.util.DeployComputeUnit;
import org.apache.ignite.table.DataStreamerReceiverDescriptor;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;

/**
 * This example demonstrates how to use the streaming API to implement a receiver that processes data containing customer and address
 * information, and updates two separate tables on the server.
 *
 * <p>See {@code README.md} in the {@code examples} directory for execution instructions.</p>
 */


public class MultiTableDataStreamerExample {

    private static final String DEPLOYMENT_UNIT_NAME = "streamerReceiverExampleUnit";

    /** Deployment unit version. */
    private static final String DEPLOYMENT_UNIT_VERSION = "1.0.0";

    /**
     * Runs the MultiTableDataStreamerExample.
     *
     * @param arg The command line arguments.
     * @throws Exception if any error occurs.
     */
    public static void main(String[] arg) throws Exception {

        DeployComputeUnit.processDeploymentUnit(arg);

        try (IgniteClient client = IgniteClient.builder()
                .addresses("127.0.0.1:10800")
                .build()) {

            deployIfNotExist(DEPLOYMENT_UNIT_NAME, DEPLOYMENT_UNIT_VERSION, DeployComputeUnit.getJarPath());

            /* Create tables using client SQL API */
            client.sql().executeScript(
                    "CREATE TABLE IF NOT EXISTS Customers ("
                            + "id INT PRIMARY KEY,"
                            + "name VARCHAR(255),"
                            + "addressId INT"
                            + ")"
            );

            client.sql().executeScript(
                    "CREATE TABLE IF NOT EXISTS Addresses ("
                            + "id INT PRIMARY KEY,"
                            + "street VARCHAR(255),"
                            + "city VARCHAR(255)"
                            + ")"
            );

            client.sql().executeScript("INSERT INTO Addresses (id, street, city) VALUES (1, '123 Elm Street', 'Springfield')");
            client.sql().executeScript("INSERT INTO Addresses (id, street, city) VALUES (2, '456 Oak Avenue', 'Shelbyville')");
            client.sql().executeScript("INSERT INTO Addresses (id, street, city) VALUES (3, '789 Pine Road', 'Capitol City')");

            client.sql().executeScript("INSERT INTO Customers (id, name, addressId) VALUES (1, 'John Doe', 1)");
            client.sql().executeScript("INSERT INTO Customers (id, name, addressId) VALUES (2, 'Jane Smith', 2)");
            client.sql().executeScript("INSERT INTO Customers (id, name, addressId) VALUES (3, 'Robert Johnson', 3)");

            DataStreamerReceiverDescriptor<Tuple, Void, Tuple> desc = DataStreamerReceiverDescriptor
                    .builder(TwoTableReceiver.class)
                    .units(new DeploymentUnit(DEPLOYMENT_UNIT_NAME, DEPLOYMENT_UNIT_VERSION))
                    .build();

            /* List<Tuple> is the source data. Those tuples do not conform to any table and can have arbitrary data */
            List<Tuple> sourceData = IntStream.range(1, 10)
                    .mapToObj(i -> Tuple.create()
                            .set("customerId", i)
                            .set("customerName", "Customer " + i)
                            .set("addressId", i)
                            .set("street", "Street " + i)
                            .set("city", "City " + i))
                    .collect(Collectors.toList());

            CompletableFuture<Void> streamerFut;

            RecordView<Tuple> customersTableView = client.tables().table("Customers").recordView();

            /* Extract the target table key from each source item; since the source has "customerId" but the target table uses "id", the function maps customerId to id accordingly */
            Function<Tuple, Tuple> keyFunc = sourceItem -> Tuple.create().set("id", sourceItem.intValue("customerId"));

            /* Extract the data payload sent to the receiver. In this case, we use the entire source item as the payload */
            Function<Tuple, Tuple> payloadFunc = Function.identity();

            /* Stream data using a publisher */
            try (var publisher = new SubmissionPublisher<Tuple>()) {
                streamerFut = customersTableView.streamData(
                        publisher,
                        desc,
                        keyFunc,
                        payloadFunc,
                        null, /* Optional receiver arguments */
                        null, /* Result subscriber */
                        null /* Options */
                );

                for (Tuple item : sourceData) {
                    publisher.submit(item);
                }
            }

            streamerFut.join();

            // Drop tables
            System.out.println("\nDropping the tables...");
            client.sql().executeScript("DROP TABLE IF EXISTS Customers");
            client.sql().executeScript("DROP TABLE IF EXISTS Addresses");
        } finally {
            System.out.println("Cleaning up resources");
            undeployUnit(DEPLOYMENT_UNIT_NAME, DEPLOYMENT_UNIT_VERSION);
        }
    }
}

