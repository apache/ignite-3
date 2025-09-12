package org.apache.ignite.example.table;

import static org.apache.ignite.table.criteria.Criteria.and;
import static org.apache.ignite.table.criteria.Criteria.columnValue;
import static org.apache.ignite.table.criteria.Criteria.equalTo;
import static org.apache.ignite.table.criteria.Criteria.greaterThan;

import java.util.Iterator;
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

            // Get a table
            IgniteTables tablesApi = client.tables();
            Table myTable = tablesApi.table("MY_TABLE");

            // Example 1: Query without transaction
            performQueryWithoutTransaction(myTable);

            // Example 2: Query within transaction
            performQueryWithTransaction(client, myTable);

            // Example 3: Query asynchronously
            performQueryAsync(myTable);
        }
    }

    /**
     * Demonstrates querying with an implicit transaction.
     */
    public static void performQueryWithoutTransaction(Table table) {
        try (Cursor<Entry<Tuple, Tuple>> cursor = table.keyValueView().query(
                null, // Implicit transaction
                // Query criteria
                and(
                        columnValue("name", equalTo("John Doe")),
                        columnValue("age", greaterThan(20))
                )
        )) {
            // Process query results (keeping original cursor iteration pattern)
            // ...
        }
    }

    /**
     * Demonstrates querying with an explicit transaction.
     */
    public static void performQueryWithTransaction(IgniteClient client, Table table) {
        Transaction transaction = client.transactions().begin();

        try (Cursor<Entry<Tuple, Tuple>> cursor = table.keyValueView().query(
                transaction,
                // Query criteria
                and(
                        columnValue("name", equalTo("John Doe")),
                        columnValue("age", greaterThan(20))
                )
        )) {
            // ...

            // Commit transaction if all operations succeed
            transaction.commit();
        } catch (Exception e) {
            // Rollback transaction on error
            transaction.rollback();
            throw new RuntimeException("Transaction failed", e);
        }
    }

    public static void performQueryAsync(Table table) {
        AsyncCursor<Entry<Tuple, Tuple>> result = table.keyValueView().queryAsync(
                        null, // Implicit transaction
                        and(
                                columnValue("name", equalTo("John Doe")),
                                columnValue("age", greaterThan(20))
                        )
                )
                .join();
        Iterator<Entry<Tuple, Tuple>> iterator = result.currentPage().iterator();

        while (iterator.hasNext()) {
            Entry<Tuple, Tuple> item = iterator.next();
            System.out.println(
                    "\nRetrieved value:\n"
                            + "    Value: " + item.getValue());
        }
    }
}
