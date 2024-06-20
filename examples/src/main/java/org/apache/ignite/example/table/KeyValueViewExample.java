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

package org.apache.ignite.example.table;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Tuple;

/**
 * This example demonstrates the usage of the {@link KeyValueView} API.
 *
 * <p>To run the example, please do the following:
 * <ol>
 *     <li>Open the Ignite 3 project in your IDE of choice.</li>
 *     <li>
 *         Download the Ignite 3 ZIP packaging with DB and CLI parts.
 *         Or build them from the Ignite 3 sources (see {@code DEVNOTES.md}).
 *         Unpack.
 *     </li>
 *     <li>
 *         Prepare the environment variables:<br>
 *         <code>
 *             export IGNITE_HOME=/path/to/ignite3-db-VERSION<br>
 *             export IGNITE_CLI_HOME=/path/to/ignite3-cli-VERSION<br>
 *             export IGNITE_SRC_HOME=/path/to/ignite/sources
 *         </code>
 *     </li>
 *     <li>
 *         Override the default configuration file:<br>
 *         {@code echo "CONFIG_FILE=$IGNITE_SRC_HOME/examples/config/ignite-config.conf" >> $IGNITE_HOME/etc/vars.env}
 *     </li>
 *     <li>
 *         Start an Ignite node using the startup script from the DB part:<br>
 *         {@code ${IGNITE_HOME}/bin/ignite3db start}
 *     </li>
 *     <li>
 *         Initialize the cluster using Ignite 3 CLI from the CLI part:<br>
 *         {@code $IGNITE_CLI_HOME/bin/ignite3 cluster init --name myCluster1 --metastorage-group defaultNode
 *         --cluster-management-group defaultNode}
 *     </li>
 *     <li>Run the example from the IDE.</li>
 *     <li>
 *         Stop the Ignite node using the startup script:<br>
 *         {@code ${IGNITE_HOME}/bin/ignite3db stop}
 *     </li>
 * </ol>
 */
public class KeyValueViewExample {
    /**
     * Main method of the example.
     *
     * @param args The command line arguments.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        //--------------------------------------------------------------------------------------
        //
        // Creating 'accounts' table.
        //
        //--------------------------------------------------------------------------------------

        try (
                Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1:10800/");
                Statement stmt = conn.createStatement()
        ) {
            stmt.executeUpdate(
                    "CREATE TABLE accounts ("
                            + "accountNumber INT PRIMARY KEY,"
                            + "firstName     VARCHAR,"
                            + "lastName      VARCHAR,"
                            + "balance       DOUBLE)"
            );
        }

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
            // Creating a key-value view for the 'accounts' table.
            //
            //--------------------------------------------------------------------------------------

            KeyValueView<Tuple, Tuple> kvView = client.tables().table("accounts").keyValueView();

            //--------------------------------------------------------------------------------------
            //
            // Performing the 'put' operation.
            //
            //--------------------------------------------------------------------------------------

            System.out.println("\nInserting a key-value pair into the 'accounts' table...");

            Tuple key = Tuple.create()
                    .set("accountNumber", 123456);

            Tuple value = Tuple.create()
                    .set("firstName", "Val")
                    .set("lastName", "Kulichenko")
                    .set("balance", 100.00d);

            kvView.put(null, key, value);

            //--------------------------------------------------------------------------------------
            //
            // Performing the 'get' operation.
            //
            //--------------------------------------------------------------------------------------

            System.out.println("\nRetrieving a value using KeyValueView API...");

            value = kvView.get(null, key);

            System.out.println(
                    "\nRetrieved value:\n"
                            + "    Account Number: " + key.intValue("accountNumber") + '\n'
                            + "    Owner: " + value.stringValue("firstName") + " " + value.stringValue("lastName") + '\n'
                            + "    Balance: $" + value.doubleValue("balance"));
        } finally {
            System.out.println("\nDropping the table...");

            try (
                    Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1:10800/");
                    Statement stmt = conn.createStatement()
            ) {
                stmt.executeUpdate("DROP TABLE accounts");
            }
        }
    }
}
