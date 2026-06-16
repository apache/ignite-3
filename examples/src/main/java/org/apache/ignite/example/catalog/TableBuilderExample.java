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
package org.apache.ignite.example.catalog;

import static org.apache.ignite.catalog.definitions.ColumnDefinition.column;

import org.apache.ignite.catalog.ColumnType;
import org.apache.ignite.catalog.IgniteCatalog;
import org.apache.ignite.catalog.definitions.TableDefinition;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;

/**
 * This example demonstrates the usage of the catalog API to create tables.
 *
 * <p>Find instructions on how to run the example in the README.md file located in the "examples" directory root.
 */
public class TableBuilderExample {
    public static void main(String[] args) {

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
            IgniteCatalog catalog = client.catalog();

            //--------------------------------------------------------------------------------------
            //
            // Creating a table by using a TableDefinition builder.
            //
            //--------------------------------------------------------------------------------------

            catalog.createTable(
                    TableDefinition.builder("sampleTable3")
                            .primaryKey("myKey")
                            .columns(
                                    column("myKey", ColumnType.INT32),
                                    column("myValue", ColumnType.VARCHAR)
                            )
                            .build()
            );

            //--------------------------------------------------------------------------------------
            //
            // Putting a new value into a table.
            //
            //--------------------------------------------------------------------------------------

            Table myTable = client.tables().table("sampleTable3");
            myTable.keyValueView().put(null, Tuple.create().set("myKey", 1), Tuple.create().set("myValue", "John"));

            //--------------------------------------------------------------------------------------
            //
            // Getting a value from the table.
            //
            //--------------------------------------------------------------------------------------

            Tuple value = myTable.keyValueView().get(null, Tuple.create().set("myKey", 1));
            System.out.println(
                    "\nRetrieved value:\n" +
                    value.stringValue("myValue")
            );
        }
    }
}
