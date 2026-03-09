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

import static org.apache.ignite.table.criteria.Criteria.and;
import static org.apache.ignite.table.criteria.Criteria.columnValue;
import static org.apache.ignite.table.criteria.Criteria.equalTo;
import static org.apache.ignite.table.criteria.Criteria.greaterThan;

import java.util.Map.Entry;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.lang.AsyncCursor;
import org.apache.ignite.lang.Cursor;
import org.apache.ignite.table.IgniteTables;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.Transaction;

public class QueryExample {

    public static void main(String[] args) throws Exception {
        // Initialize Ignite client with connection configuration
        try (IgniteClient client = IgniteClient.builder()
                .addresses("127.0.0.1:10800")
                .build()) {
            System.out.println("Creating Person table");
            client.sql().execute("CREATE TABLE IF NOT EXISTS Person (id int primary key,  city varchar,  name varchar,  age int,  company varchar, city_id int);");
            client.sql().execute("INSERT INTO Person (id, city, name, age, company, city_id) VALUES (1, 'London', 'John Doe', 42, 'Apache', 101);");
            client.sql().execute("INSERT INTO Person (id, city, name, age, company, city_id) VALUES (2, 'New York', 'Jane Doe', 36, 'Apache', 102);");

            // Get a table
            IgniteTables tablesApi = client.tables();
            Table myTable = tablesApi.table("Person");

            // Example 1: Query without transaction
            performQueryWithoutTransaction(myTable);

            // Example 2: Query within transaction
            performQueryWithTransaction(client, myTable);

            // Example 3: Query asynchronously
            performQueryAsync(myTable);

            System.out.println("Dropping Person table.");
            client.sql().execute("DROP TABLE IF EXISTS PERSON;");
        }
    }

    /**
     * Demonstrates querying with an implicit transaction.
     *
     * @param table  Table instance to query.
     */
    public static void performQueryWithoutTransaction(Table table) {
        System.out.println("[ Example 1 ] Performing query without transaction");

        try (Cursor<Entry<Tuple, Tuple>> cursor = table.keyValueView().query(
                null, // Implicit transaction
                // Query criteria
                and(
                        columnValue("name", equalTo("John Doe")),
                        columnValue("age", greaterThan(20))
                )
        )) {
            // Process query results (keeping original cursor iteration pattern)
            // As an example, println all matched values.
            while (cursor.hasNext()) {
                printRecord(cursor.next());
            }
        }
    }

    /**
     * Demonstrates querying with an explicit transaction.
     *
     * @param client Ignite client used to start the transaction.
     * @param table  Table instance to query.
     */
    public static void performQueryWithTransaction(IgniteClient client, Table table) {
        System.out.println("[ Example 2 ] Performing query with transaction");

        Transaction transaction = client.transactions().begin();

        try (Cursor<Entry<Tuple, Tuple>> cursor = table.keyValueView().query(
                transaction,
                // Query criteria
                and(
                        columnValue("name", equalTo("John Doe")),
                        columnValue("age", greaterThan(20))
                )
        )) {
            // Process query results
            // As an example, println all matched values.
            while (cursor.hasNext()) {
                printRecord(cursor.next());
            }

            // Commit transaction if all operations succeed
            transaction.commit();
        } catch (Exception e) {
            // Rollback transaction on error
            transaction.rollback();
            throw new RuntimeException("Transaction failed", e);
        }
    }

    public static void performQueryAsync(Table table) {
        System.out.println("[ Example 3 ] Performing asynchronous query");

        AsyncCursor<Entry<Tuple, Tuple>> result = table.keyValueView().queryAsync(
                        null, // Implicit transaction
                        and(
                                columnValue("name", equalTo("John Doe")),
                                columnValue("age", greaterThan(20))
                        )
                )
                .join();

        for (Entry<Tuple, Tuple> tupleTupleEntry : result.currentPage()) {
            printRecord(tupleTupleEntry);
        }
    }

    private static void printRecord(Entry<Tuple, Tuple> record) {
        int personId = record.getKey().intValue("id");

        Tuple personValueTuple = record.getValue();

        String personName = personValueTuple.stringValue("name");
        int personCityId = personValueTuple.intValue("city_id");
        String personCompany = personValueTuple.stringValue("company");

        System.out.println("Record: { "
                + "id=" + personId +
                ", name=" + personName +
                ", cityId= " + personCityId +
                ", company=" + personCompany
                + " }");
    }
}
