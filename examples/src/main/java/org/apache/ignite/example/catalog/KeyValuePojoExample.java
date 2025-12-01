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
import org.apache.ignite.table.KeyValueView;

/**
 * This example demonstrates the usage of the @Table annotation to create tables.
 *
 * <p>Find instructions on how to run the example in the README.md file located in the "examples" directory root.
 */
public class KeyValuePojoExample {

    //--------------------------------------------------------------------------------------
    //
    // Defining a table with an annotation.
    //
    //--------------------------------------------------------------------------------------

    @Table(value = "kv_pojo",
            zone = @Zone(value = "zone_test", replicas = 2, storageProfiles = "default"),
            colocateBy = {@ColumnRef("id"), @ColumnRef("id_str")},
            indexes = @Index(value = "ix", columns = {@ColumnRef("f_name"), @ColumnRef("l_name")}))

    //--------------------------------------------------------------------------------------
    //
    // Creating a POJO for the key.
    //
    //--------------------------------------------------------------------------------------

    public static class PojoKey {
        @Id
        Integer id;

        @Id(SortOrder.DEFAULT)
        @Column(value = "id_str", length = 20)
        String idStr;

        public PojoKey(Integer id, String idStr) {
            this.id = id;
            this.idStr = idStr;
        }
    }

    //--------------------------------------------------------------------------------------
    //
    // Creating POJO for the values.
    //
    //--------------------------------------------------------------------------------------

    public static class PojoValue {
        @Column("f_name")
        private String firstName;

        @Column("l_name")
        private String lastName;

        public PojoValue(String firstName, String lastName) {
            this.firstName = firstName;
            this.lastName = lastName;
        }
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

            org.apache.ignite.table.Table myTable = client.catalog().createTable(PojoKey.class, PojoValue.class);

            //--------------------------------------------------------------------------------------
            //
            // Putting a new value into a table.
            //
            //--------------------------------------------------------------------------------------

            KeyValueView<PojoKey, PojoValue> kvView = myTable.keyValueView(PojoKey.class, PojoValue.class);
            PojoKey key = new PojoKey(1, "sample");
            PojoValue putValue = new PojoValue("John", "Smith");
            kvView.put(null, key, putValue);

            //--------------------------------------------------------------------------------------
            //
            // Getting a value from the table.
            //
            //--------------------------------------------------------------------------------------

            PojoValue getValue = kvView.get(null, key);
            System.out.println(
                    "\nRetrieved values:\n"
                            + "    Account ID: " + key.id + '\n'
                            + "    First name: " + getValue.firstName + '\n'
                            + "    Last name" + getValue.lastName);

        }
    }
}

