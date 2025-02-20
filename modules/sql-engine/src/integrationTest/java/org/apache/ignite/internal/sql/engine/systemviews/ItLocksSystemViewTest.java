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

package org.apache.ignite.internal.sql.engine.systemviews;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.internal.sql.engine.util.MetadataMatcher;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.LockMode;
import org.apache.ignite.sql.ColumnType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * End-to-end tests to verify {@code LOCKS} system view.
 */
public class ItLocksSystemViewTest extends BaseSqlIntegrationTest {
    private Set<String> nodeNames;

    private Set<String> lockModes;

    @Override
    protected int initialNodes() {
        return 2;
    }

    @BeforeAll
    void beforeAll() {
        await(systemViewManager().completeRegistration());

        nodeNames = CLUSTER.runningNodes()
                .map(Ignite::name)
                .collect(Collectors.toSet());

        lockModes = Arrays.stream(LockMode.values())
                .map(Enum::name)
                .collect(Collectors.toSet());
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

        String[] operations = {
                "INSERT INTO test VALUES (1, 1)",
                "UPDATE test SET val = 1 WHERE id = 0",
                "DELETE FROM test WHERE id = 2"
        };

        for (String op : operations) {
            sql("DELETE FROM test");
            sql("INSERT INTO test VALUES (0, 0), (2, 2)");

            InternalTransaction tx = (InternalTransaction) node.transactions().begin();

            try {
                sql(tx, op);

                List<List<Object>> rows = sql("SELECT * FROM SYSTEM.LOCKS WHERE TX_ID=?", tx.id().toString());

                assertThat(rows, is(not(empty())));

                for (List<Object> row : rows) {
                    verifyLockInfo(row, tx.id().toString());
                }
            } finally {
                tx.rollback();
            }
        }
    }

    private void verifyLockInfo(List<Object> row, String expectedTxId) {
        int idx = 0;

        String owningNode = (String) row.get(idx++);
        String txId = (String) row.get(idx++);
        String objectId = (String) row.get(idx++);
        String mode = (String) row.get(idx);

        assertThat(nodeNames, hasItem(owningNode));
        assertThat(txId, equalTo(expectedTxId));
        assertThat(objectId, not(emptyString()));
        assertThat(lockModes, hasItem(mode));
    }
}
