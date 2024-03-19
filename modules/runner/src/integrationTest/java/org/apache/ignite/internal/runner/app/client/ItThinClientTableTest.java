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

package org.apache.ignite.internal.runner.app.client;

import org.junit.jupiter.api.BeforeAll;

/**
 * Tests table operations with thin client.
 */
@SuppressWarnings("resource")
public class ItThinClientTableTest extends ItAbstractThinClientTest {
    private static final String TABLE_NAME2 = "TBL2";

    @BeforeAll
    void createTable() {
        String query = "CREATE TABLE " + TABLE_NAME2
                + " (val1 VARCHAR, key1 INT, val2 BIGINT, key2 VARCHAR, PRIMARY KEY (key1, key2)) "
                + "colocate by (key2, key1)";

        client().sql().createSession().execute(null, query);
    }
}
