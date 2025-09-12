/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.example.table;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.table.IgniteTables;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.QualifiedName;
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
            // Get the tables API to interact with database tables
            IgniteTables tableApi = client.tables();

            // Retrieve a list of all existing tables in the cluster
            List<Table> existingTables = tableApi.tables();

            // Get the first table from the list (for demonstration purposes)
            Table firstTable = existingTables.get(0);

            // Access a specific table by its simple name
            Table specificTable = tableApi.table("MY_TABLE");

            // Create a qualified table name by parsing a string (schema.table format)
            QualifiedName qualifiedTableName = QualifiedName.parse("PUBLIC.MY_QUALIFIED_TABLE");

            // Alternative way to create qualified name using schema and table parts
            //QualifiedName qualifiedTableName = QualifiedName.of("PUBLIC", "MY_TABLE");

            // Access a table using the qualified name (includes schema)
            Table myTable = tableApi.table(qualifiedTableName);
        }
    }
}
