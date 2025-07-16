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

import org.apache.ignite.catalog.SortOrder;
import org.apache.ignite.catalog.annotations.Column;
import org.apache.ignite.catalog.annotations.ColumnRef;
import org.apache.ignite.catalog.annotations.Id;
import org.apache.ignite.catalog.annotations.Index;
import org.apache.ignite.catalog.annotations.Table;
import org.apache.ignite.catalog.annotations.Zone;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;

/**
 * This example demonstrates the usage of the @Table annotation to create tables.
 *
 * <p>Find instructions on how to run the example in the README.md file located in the "examples" directory root.
 */
public class RecordViewPojoExample {

    //--------------------------------------------------------------------------------------
    //
    // Defining a table with an annotation.
    //
    //--------------------------------------------------------------------------------------

    @Table(value = "pojo_sample",
            zone = @Zone(value = "zone_test", replicas = 2, storageProfiles = "default"),
            colocateBy = {@ColumnRef("id"), @ColumnRef("id_str")},
            indexes = @Index(value = "ix_sample", columns = {@ColumnRef("f_name"), @ColumnRef("l_name")}))

    //--------------------------------------------------------------------------------------
    //
    // Creating a POJO for table columns.
    //
    //--------------------------------------------------------------------------------------

    public static class Pojo {
        @Id
        Integer id;

        @Id(SortOrder.DEFAULT)
        @Column(value = "id_str", length = 20)
        String idStr;

        @Column("f_name")
        String firstName;

        @Column("l_name")
        String lastName;

        String str;
    }

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

            //--------------------------------------------------------------------------------------
            //
            // Creating a table.
            //
            //--------------------------------------------------------------------------------------

            org.apache.ignite.table.Table myTable = client.catalog().createTable(Pojo.class);

            //--------------------------------------------------------------------------------------
            //
            // Putting a new value into a table.
            //
            //--------------------------------------------------------------------------------------

            RecordView<Tuple> view = myTable.recordView();
            Tuple insertTuple = Tuple.create()
                    .set("id", 1)
                    .set("id_str", "sample")
                    .set("f_name", "John")
                    .set("l_name", "Smith");
            view.insert(null, insertTuple);

            //--------------------------------------------------------------------------------------
            //
            // Getting a value from the table.
            //
            //--------------------------------------------------------------------------------------

            Tuple getTuple = view.get(null, insertTuple);
            System.out.println(
                    "\nRetrieved record: " +
                            getTuple.stringValue("f_name")
            );
        }
    }
}
