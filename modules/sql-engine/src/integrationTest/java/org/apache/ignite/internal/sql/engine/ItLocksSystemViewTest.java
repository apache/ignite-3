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

package org.apache.ignite.internal.sql.engine;

import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;

import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.internal.sql.engine.util.MetadataMatcher;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.tx.Transaction;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * End-to-end tests to verify {@code LOCKS} system view.
 */
public class ItLocksSystemViewTest extends BaseSqlIntegrationTest {
    @Override
    protected int initialNodes() {
        return 2;
    }

    @BeforeAll
    void beforeAll() {
        await(systemViewManager().completeRegistration());
    }

    @Test
    public void testMetadata() {
        assertQuery("SELECT * FROM SYSTEM.LOCKS")
                .columnMetadata(
                        new MetadataMatcher().name("OWNING_NODE_ID").type(ColumnType.STRING).nullable(false),
                        new MetadataMatcher().name("TX_ID").type(ColumnType.STRING).nullable(true),
                        new MetadataMatcher().name("OBJECT_ID").type(ColumnType.STRING).nullable(true),
                        new MetadataMatcher().name("MODE").type(ColumnType.STRING).nullable(true)
                )
                // RO tx doesn't take locks.
                .returnNothing()
                .check();
    }

    @Test
    public void testData() {
        Ignite node = CLUSTER.aliveNode();

        sql("CREATE TABLE test (id INT PRIMARY KEY, val INT)");
        sql("INSERT INTO test VALUES (0, 0)");

        Transaction tx = node.transactions().begin();

        try {
            sql(tx, "UPDATE test SET val = 1 WHERE id = 0");

            IgniteImpl igniteImpl = unwrapIgniteImpl(node);
            LockManager lockMgr = igniteImpl.txManager().lockManager();

            List<List<Object>> rows = sql("SELECT * FROM SYSTEM.LOCKS");

            assertThat(rows, not(empty()));

            for (List<Object> row : rows) {
                System.out.println(row);
            }

            lockMgr.locks(((InternalTransaction) tx).id());
        } finally {
            tx.rollback();
        }
    }
}
