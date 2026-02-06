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

package org.apache.ignite.internal.client;

import org.apache.ignite.Ignite;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.table.ItDataConsistencyTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

/**
 * Test data consistency in mixed read-write load initiated from a client.
 */
public class ItClientDataConsistencyTest extends ItDataConsistencyTest {
    private IgniteClient client;

    @BeforeEach
    public void startClient() {
        client = IgniteClient.builder().addresses("127.0.0.1:10800").build();
    }

    @AfterEach
    void closeClient() {
        client.close();
    }

    @Override
    protected Ignite assignNodeForIteration(int workerId) {
        return client;
    }
}
