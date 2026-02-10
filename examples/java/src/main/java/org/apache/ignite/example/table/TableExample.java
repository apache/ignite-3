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

import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.table.IgniteTables;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.QualifiedName;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;

/**
 * This example demonstrates the usage of the {@link KeyValueView} API.
 *
 * <p>Find instructions on how to run the example in the README.md file located in the "examples" directory root.
 */
public class TableExample {
    /**
     * Main method of the example.
     *
     * @param args The command line arguments.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {

        try (IgniteClient client = IgniteClient.builder()
                .addresses("127.0.0.1:10800")
                .build()
        ) {
            // Create a table to work with later
            client.sql().execute("CREATE TABLE IF NOT EXISTS Person (" +
                    "id int primary key," +
                    "city_id int," +
                    "name varchar," +
                    "age int," +
                    "company varchar)"
            );

            // Get the tables API to interact with database tables
            IgniteTables tableApi = client.tables();

            // Retrieve a list of all existing tables in the cluster
            //List<Table> existingTables = tableApi.tables();

            // Get the first table from the list (for demonstration purposes)
            //Table firstTable = existingTables.get(0);

            // Access a specific table by its simple name
            //Table specificTable = tableApi.table("MY_TABLE");

            // Create a qualified table name by parsing a string (schema.table format)
            QualifiedName qualifiedTableName = QualifiedName.parse("PUBLIC.Person");

            // Alternative way to create qualified name using schema and table parts
            //QualifiedName qualifiedTableName = QualifiedName.of("PUBLIC", "MY_TABLE");

            // Access a table using the qualified name (includes schema)
            Table myTable = tableApi.table(qualifiedTableName);

            RecordView<Tuple> personTableView = myTable.recordView();

            Tuple personTuple = Tuple.create()
                    .set("id", 1)
                    .set("city_id", 3)
                    .set("name", "John Doe")
                    .set("age", 32)
                    .set("company", "Apache");

            personTableView.upsert(null, personTuple);

            Tuple personIdTuple = Tuple.create()
                    .set("id", 1);
            Tuple insertedPerson = personTableView.get(null, personIdTuple);

            System.out.println("Person name: " + insertedPerson.stringValue("name"));
        }
    }
}
