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

import static java.sql.DriverManager.getConnection;
import static org.apache.ignite.example.util.DeployComputeUnit.deployIfNotExist;
import static org.apache.ignite.example.util.DeployComputeUnit.undeployUnit;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.SubmissionPublisher;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.deployment.DeploymentUnit;
import org.apache.ignite.example.code.deployment.AbstractDeploymentUnitExample;
import org.apache.ignite.table.DataStreamerReceiverDescriptor;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;

/**
 * This example demonstrates how to use the streaming API to how to implement a receiver that processes data containing customer and address
 * information, and updates two separate tables on the server.
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


public class MultiTableDataStreamerExample extends AbstractDeploymentUnitExample {

    private static final String DEPLOYMENT_UNIT_NAME = "streamerReceiverExampleUnit";

    /** Deployment unit version. */
    private static final String DEPLOYMENT_UNIT_VERSION = "1.0.0";

    public static void main(String[] arg) throws Exception {

        processDeploymentUnit(arg);

        try (IgniteClient client = IgniteClient.builder()
                .addresses("127.0.0.1:10800")
                .build()) {

            deployIfNotExist(DEPLOYMENT_UNIT_NAME, DEPLOYMENT_UNIT_VERSION, jarPath);

            /* Create 'accounts' table via JDBC */
            try (
                    Connection conn = getConnection("jdbc:ignite:thin://127.0.0.1:10800/");
                    Statement stmt = conn.createStatement()
            ) {
                stmt.executeUpdate(
                        "CREATE TABLE IF NOT EXISTS Customers ("
                                + "    id INT PRIMARY KEY,"
                                + "    name VARCHAR(255),"
                                + "    addressId INT"
                                + ")"
                );

                stmt.executeUpdate(
                        "CREATE TABLE IF NOT EXISTS Addresses ("
                                + "    id INT PRIMARY KEY,"
                                + "    street VARCHAR(255),"
                                + "    city VARCHAR(255)"
                                + ")"
                );

                stmt.executeUpdate("INSERT INTO Addresses (id, street, city) VALUES (1, '123 Elm Street', 'Springfield')");
                stmt.executeUpdate("INSERT INTO Addresses (id, street, city) VALUES (2, '456 Oak Avenue', 'Shelbyville')");
                stmt.executeUpdate("INSERT INTO Addresses (id, street, city) VALUES (3, '789 Pine Road', 'Capitol City')");

                stmt.executeUpdate("INSERT INTO Customers (id, name, addressId) VALUES (1, 'John Doe', 1)");
                stmt.executeUpdate("INSERT INTO Customers (id, name, addressId) VALUES (2, 'Jane Smith', 2)");
                stmt.executeUpdate("INSERT INTO Customers (id, name, addressId) VALUES (3, 'Robert Johnson', 3)");

            }

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
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {

            System.out.println("Cleaning up resources");
            undeployUnit(DEPLOYMENT_UNIT_NAME, DEPLOYMENT_UNIT_VERSION);

            /* Drop table */
            System.out.println("\nDropping the table...");
            try (
                    Connection conn = getConnection("jdbc:ignite:thin://127.0.0.1:10800/");
                    Statement stmt = conn.createStatement()
            ) {
                stmt.executeUpdate("DROP TABLE IF EXISTS Customers");
                stmt.executeUpdate("DROP TABLE IF EXISTS Addresses");
            }

        }
    }
}

