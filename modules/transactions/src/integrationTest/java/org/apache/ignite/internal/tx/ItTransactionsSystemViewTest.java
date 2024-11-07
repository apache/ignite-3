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

package org.apache.ignite.internal.tx;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;

import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.internal.sql.engine.util.MetadataMatcher;
import org.apache.ignite.sql.ColumnType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * End-to-end tests to verify {@code TRANSACTIONS} system view.
 */
public class ItTransactionsSystemViewTest extends BaseSqlIntegrationTest {
    @BeforeAll
    void beforeAll() {
        await(systemViewManager().completeRegistration());
    }

    @Test
    public void metadataTest() {
        assertQuery("SELECT * FROM SYSTEM.TRANSACTIONS")
                .columnMetadata(
                        new MetadataMatcher().name("COORDINATOR_NODE").type(ColumnType.STRING).nullable(false),
                        new MetadataMatcher().name("STATE").type(ColumnType.STRING).nullable(true),
                        new MetadataMatcher().name("ID").type(ColumnType.STRING).nullable(true),
                        new MetadataMatcher().name("START_TIME").type(ColumnType.TIMESTAMP).nullable(true),
                        new MetadataMatcher().name("TYPE").type(ColumnType.STRING).nullable(true),
                        new MetadataMatcher().name("PRIORITY").type(ColumnType.STRING).nullable(true)
                )
                // Executing a query creates an implicit transaction.
                .returnRowCount(1)
                .check();
    }
}
