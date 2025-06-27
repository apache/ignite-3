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

package org.apache.ignite.internal.table;

import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.table.IgniteTables;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

/**
 * Tests to check that rows can be inserted and retrieved through thin client.
 */
public class ItTablePutGetThinTest extends ItTablePutGetEmbeddedTest {
    private static IgniteClient client;

    @BeforeAll
    static void startClient() {
        client = IgniteClient.builder()
                .addresses("127.0.0.1:10800")
                .build();
    }

    @AfterAll
    static void stopClient() {
        client.close();
    }

    @Override
    IgniteTables tables() {
        return client.tables();
    }
}
