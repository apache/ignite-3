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

package org.apache.ignite.example.client;


import org.apache.ignite.client.IgniteClient;

/**
 * This example demonstrates the usage of the basic client API without any fine-tuning.
 * Configurations like this are sufficient for most common scenarios.
 */

public class IgniteClientExample {
    public static void main(String[] args) throws Exception {
        //--------------------------------------------------------------------------------------
        //
        // Creating client connector instance and connecting to cluster.
        //
        //--------------------------------------------------------------------------------------
        try (IgniteClient client = IgniteClient.builder()
                .addresses("127.0.0.1:10800")
                .build()
        ) {
            //--------------------------------------------------------------------------------------
            //
            // You can use the client here.
            //
            //--------------------------------------------------------------------------------------
            client.sql().execute(null, "CREATE TABLE IF NOT EXISTS Person (id int primary key, name varchar, age int);");
        }
    }
}
