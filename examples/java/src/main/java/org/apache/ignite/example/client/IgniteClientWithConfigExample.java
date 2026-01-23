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

import org.apache.ignite.client.BasicAuthenticator;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.client.IgniteClientAuthenticator;
import org.apache.ignite.client.RetryLimitPolicy;

/**
 * This example demonstrates the usage of the basic client API with additional configuration parameters.
 * Configurations like this are sufficient for most common scenarios.
 */

public class IgniteClientWithConfigExample {
    public static void main(String[] args) throws Exception {

        //--------------------------------------------------------------------------------------
        //
        // Creating client authenticator instance with basic auth.
        //
        //--------------------------------------------------------------------------------------
        IgniteClientAuthenticator auth = BasicAuthenticator.builder().username("myUser").password("myPassword").build();

        //--------------------------------------------------------------------------------------
        //
        // Creating client connector instance and connecting to cluster.
        //
        //--------------------------------------------------------------------------------------
        try (IgniteClient client = IgniteClient.builder()
                .addresses("127.0.0.1:10800")
                // Client authentication.
                .authenticator(auth)
                // Client will use system-default logger.
                .loggerFactory(System::getLogger)
                // Client will have metrics enabled.
                .metricsEnabled(true)
                // Client connection will time out if not established in 5 seconds.
                .connectTimeout(5000)
                // Client will send heartbeat messages every 30 seconds.
                .heartbeatInterval(30000)
                // Heartbeat messages will time out after 5 seconds
                .heartbeatTimeout(5000)
                // Operations will time out if not received in 3 seconds.
                .operationTimeout(3000)
                // Client will check the connection to all addresses
                // and, if necessary, attempt to reconnect to them every 30 seconds.
                .backgroundReconnectInterval(30000)
                // The client will attempt to reconnect 8 times.
                .retryPolicy(new RetryLimitPolicy().retryLimit(8))
                .build()
        ) {
            //--------------------------------------------------------------------------------------
            //
            // You can use the client here.
            //
            //--------------------------------------------------------------------------------------
            client.sql().execute("CREATE TABLE IF NOT EXISTS Person (id int primary key, name varchar, age int);");
        }
    }
}
