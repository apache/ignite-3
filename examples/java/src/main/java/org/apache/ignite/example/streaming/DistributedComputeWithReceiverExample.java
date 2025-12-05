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
import static org.apache.ignite.catalog.definitions.ColumnDefinition.column;
import static org.apache.ignite.example.util.DeployComputeUnit.deployUnit;
import static org.apache.ignite.example.util.DeployComputeUnit.deploymentExists;
import static org.apache.ignite.example.util.DeployComputeUnit.undeployUnit;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import java.util.concurrent.SubmissionPublisher;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.catalog.ColumnType;
import org.apache.ignite.catalog.definitions.TableDefinition;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.deployment.DeploymentUnit;
import org.apache.ignite.example.code.deployment.AbstractDeploymentUnitExample;
import org.apache.ignite.table.DataStreamerReceiver;
import org.apache.ignite.table.DataStreamerReceiverContext;
import org.apache.ignite.table.DataStreamerReceiverDescriptor;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;

/**
 * This example demonstrates how to use the streaming API to simulate a fraud detection process, which typically involves intensive
 * processing of each transaction using ML models.
 *
 * <p>Find instructions on how to run the example in the {@code README.md}
 * file located in the {@code examples} directory root.</p>
 *
 * <h2>Execution Modes</h2>
 *
 * <p>There are two modes of execution:</p>
 *
 * <h3>1. Automated : The JAR Deployment for  deployment unit is automated </h3>
 *
 * <h4>1.1 With IDE</h4>
 * <ul>
 *   <li>
 *     <b>Run from an IDE</b><br>
 *     Launch the example directly from the IDE. If the required deployment
 *     unit is not present, the example automatically builds and deploys the
 *     necessary JAR.
 *   </li>
 * </ul>
 *
 * <h3>1.2 Without IDE</h3>
 * <ul>
 *   <li>
 *     <b>Run from the command line</b><br>
 *     Start the example using a Java command where the classpath includes
 *     all required dependencies:
 *
 *     <pre>{@code
 * java -cp "{user.home}\\.m2\\repository\\org\\apache\\ignite\\ignite-core\\3.1.0-SNAPSHOT\\
 * ignite-core-3.1.0-SNAPSHOT.jar{other required jars}"
 * <example-main-class> runFromIDE=false jarPath="{path-to-examples-jar}"
 *     }</pre>
 *
 *     In this mode, {@code runFromIDE=false} indicates command-line execution,
 *     and {@code jarPath} must reference the examples JAR used as the
 *     deployment unit.
 *   </li>
 * </ul>
 *
 * <h2>2. Manual (with IDE): The JAR deployment for the deployment unit is manual</h2>
 *
 * <p>Before running this example, complete the following steps related to
 * code deployment:</p>
 *
 * <ol>
 *   <li>
 *     Build the {@code ignite-examples-x.y.z.jar} file:<br>
 *     {@code ./gradlew :ignite-examples:jar}
 *   </li>
 *   <li>
 *     Deploy the generated JAR as a deployment unit using the CLI:<br>
 *     <pre>{@code
 * cluster unit deploy computeExampleUnit \
 *     --version 1.0.0 \
 *     --path=$IGNITE_HOME/examples/build/libs/ignite-examples-x.y.z.jar
 *     }</pre>
 *   </li>
 * </ol>
 */


public class DistributedComputeWithReceiverExample extends AbstractDeploymentUnitExample {

    private static final String DEPLOYMENT_UNIT_NAME = "streamerReceiverExampleUnit";

    /** Deployment unit version. */
    private static final String DEPLOYMENT_UNIT_VERSION = "1.0.0";
    private static final Path JAR_PATH = Path.of("build/libs/serialization-example-1.0.0.jar"); // Output jar

    public static void main(String[] arg) throws Exception {

        processDeploymentUnit(arg);

        try (IgniteClient client = IgniteClient.builder()
                .addresses("127.0.0.1:10800")
                .build()) {

//            // 1) Check if deployment unit already exists
            if (deploymentExists(DEPLOYMENT_UNIT_NAME, DEPLOYMENT_UNIT_VERSION)) {
                System.out.println("Deployment unit already exists. Skip deploy.");
            } else {
                System.out.println("Deployment unit not found. Deploying...");
                deployUnit(DEPLOYMENT_UNIT_NAME, DEPLOYMENT_UNIT_VERSION, JAR_PATH);
                System.out.println(" Deployment completed " + DEPLOYMENT_UNIT_NAME + ".");
            }

            /* Source data is a list of financial transactions */
            /* We distribute this processing across the cluster, then gather and return results */
            List<Tuple> sourceData = IntStream.range(1, 10)
                    .mapToObj(i -> Tuple.create()
                            .set("txId", i)
                            .set("txData", "{some-json-data}"))
                    .collect(Collectors.toList());

            DataStreamerReceiverDescriptor<Tuple, Void, Tuple> desc = DataStreamerReceiverDescriptor
                    .builder(FraudDetectorReceiver.class)
                    .units(new DeploymentUnit(DEPLOYMENT_UNIT_NAME, DEPLOYMENT_UNIT_VERSION))
                    .build();

            CompletableFuture<Void> streamerFut;

        /* Streaming requires a target table to partition data.
        /* Use a dummy table for this scenario, because we are not going to store any data */
            TableDefinition txDummyTableDef = TableDefinition.builder("tx_dummy")
                    .columns(column("id", ColumnType.INTEGER))
                    .primaryKey("id")
                    .build();

            Table dummyTable = client.catalog().createTable(txDummyTableDef);


            /* Source data has "txId" field, but target dummy table has "id" column, so keyFunc maps "txId" to "id" */
            Function<Tuple, Tuple> keyFunc = sourceItem -> Tuple.create().set("id", sourceItem.value("txId"));

        /* Payload function is used to extract the payload (data that goes to the receiver) from the source item.
        /* In our case, we want to use the whole source item as the payload */
            Function<Tuple, Tuple> payloadFunc = Function.identity();

            Flow.Subscriber<Tuple> resultSubscriber = new Flow.Subscriber<>() {
                @Override
                public void onSubscribe(Flow.Subscription subscription) {
                    subscription.request(Long.MAX_VALUE);
                }

                @Override
                public void onNext(Tuple item) {
                    System.out.println("Transaction processed: " + item);
                }

                @Override
                public void onError(Throwable throwable) {
                    System.err.println("Error during streaming: " + throwable.getMessage());
                }

                @Override
                public void onComplete() {
                    System.out.println("Streaming completed.");
                }
            };

            try (var publisher = new SubmissionPublisher<Tuple>()) {
                streamerFut = dummyTable.recordView().streamData(
                        publisher,
                        desc,
                        keyFunc,
                        payloadFunc,
                        null, /* Optional Receiver arguments*/
                        resultSubscriber,
                        null /* DataStreamer options */
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
            //   undeployUnit(DEPLOYMENT_UNIT_NAME, DEPLOYMENT_UNIT_VERSION);

            /* Drop table */
            System.out.println("\nDropping the table...");
            try (
                    Connection conn = getConnection("jdbc:ignite:thin://127.0.0.1:10800/");
                    Statement stmt = conn.createStatement()
            ) {
                stmt.executeUpdate("DROP TABLE IF EXISTS tx_dummy");
            }

        }
    }


    public static class FraudDetectorReceiver implements DataStreamerReceiver<Tuple, Void, Tuple> {
        @Override
        public CompletableFuture<List<Tuple>> receive(List<Tuple> page, DataStreamerReceiverContext ctx, Void arg) {
            List<Tuple> results = new ArrayList<>(page.size());

            for (Tuple tx : page) {
                results.add(detectFraud(tx));
            }

            return CompletableFuture.completedFuture(results);
        }

        private static Tuple detectFraud(Tuple txInfo) {
            /* Simulate fraud detection processing */
            double fraudRisk = Math.random();

            /* Add result to the tuple and return */
            return txInfo.set("fraudRisk", fraudRisk);
        }
    }
}