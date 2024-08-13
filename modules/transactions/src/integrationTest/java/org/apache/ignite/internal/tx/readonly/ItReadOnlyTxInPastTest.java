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

package org.apache.ignite.internal.tx.readonly;

import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.executeUpdate;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.tx.TransactionOptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests about read-only transactions in the past.
 */
@SuppressWarnings("resource")
class ItReadOnlyTxInPastTest extends ClusterPerTestIntegrationTest {
    private static final String TABLE_NAME = "test";

    @Override
    protected int initialNodes() {
        return 0;
    }

    @BeforeEach
    void prepareCluster() {
        // Setting idleSafeTimePropagationDuration to 1 second so that an RO tx has a potential to look before a table was created.
        cluster.startAndInit(1, builder -> builder.clusterConfiguration("replication.idleSafeTimePropagationDuration: 1000"));

        cluster.doInSession(0, session -> {
            executeUpdate("CREATE TABLE " + TABLE_NAME + " (id int PRIMARY KEY, val varchar)", session);
        });
    }

    /**
     * Make sure that an explicit RO transaction does not look too far in the past (where the corresponding
     * table did not yet exist) even when the 'look in the past' optimization is enabled.
     */
    @Test
    void explicitReadOnlyTxDoesNotLookBeforeTableCreation() {
        IgniteImpl node = cluster.node(0);

        long count = node.transactions().runInTransaction(tx -> {
            return cluster.doInSession(0, session -> {
                try (ResultSet<SqlRow> resultSet = session.execute(tx, "SELECT COUNT(*) FROM " + TABLE_NAME)) {
                    return resultSet.next().longValue(0);
                }
            });
        }, new TransactionOptions().readOnly(true));

        assertThat(count, is(0L));
    }

    /**
     * Make sure that an implicit RO transaction does not look too far in the past (where the corresponding
     * table did not yet exist) even when the 'look in the past' optimization is enabled.
     */
    @Test
    void implicitReadOnlyTxDoesNotLookBeforeTableCreation() {
        long count = cluster.query(0, "SELECT COUNT(*) FROM " + TABLE_NAME, rs -> rs.next().longValue(0));

        assertThat(count, is(0L));
    }
}
