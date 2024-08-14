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

package org.apache.ignite.internal.table.partition;

import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.table.partition.PartitionManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

/**
 * Thin client implementation of partition management test suite.
 */
public class ItThinClientPartitionManagerTest extends ItAbstractPartitionManagerTest {
    private IgniteClient client;

    @BeforeEach
    public void startClient() {
        client = IgniteClient.builder()
                .addresses("localhost")
                .reconnectThrottlingPeriod(0)
                .build();
    }

    @AfterEach
    public void stopClient() {
        client.close();
    }

    @Override
    protected PartitionManager partitionManager() {
        return client.tables().table(TABLE_NAME).partitionManager();
    }
}
