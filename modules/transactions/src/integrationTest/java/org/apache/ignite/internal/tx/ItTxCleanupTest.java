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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.tx.message.WriteIntentSwitchReplicaRequest;
import org.apache.ignite.table.KeyValueView;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ItTxCleanupTest extends ClusterPerTestIntegrationTest {
    public static final String TABLE_NAME = "TEST_TABLE";
    private IgniteImpl ignite;

    @Override
    protected int initialNodes() {
        return 1;
    }

    @BeforeEach
    void init() {
        ignite = igniteImpl(0);
    }

    @ParameterizedTest(name = "readsOnly = {0}")
    @ValueSource(booleans = {true, false})
    void writeIntentSwitchHappensOncePerTx(boolean readsOnly) throws Exception {
        ignite.sql().executeScript(
                "CREATE ZONE TEST_ZONE (partitions 1, replicas 1) storage profiles ['" + CatalogService.DEFAULT_STORAGE_PROFILE + "']; "
                        + "CREATE TABLE " + TABLE_NAME + " (ID INT PRIMARY KEY, VAL VARCHAR(20)) ZONE TEST_ZONE"
        );

        AtomicInteger writeIntentSwitchRequestCount = new AtomicInteger();

        ignite.dropMessages((recipientId, message) -> {
            if (message instanceof WriteIntentSwitchReplicaRequest) {
                writeIntentSwitchRequestCount.incrementAndGet();
            }

            return false;
        });

        ignite.transactions().runInTransaction(tx -> {
            KeyValueView<Integer, String> view = ignite.tables().table(TABLE_NAME).keyValueView(Integer.class, String.class);

            if (readsOnly) {
                view.get(tx, 1);
            } else {
                view.put(tx, 1, "one");
            }
        });

        if (readsOnly) {
            // We shouldn't see write intent switching from RW-R a transaction.
            assertFalse(waitForCondition(() -> writeIntentSwitchRequestCount.get() > 0, SECONDS.toMillis(1)));
            return;
        }

        // We should see one WI switch...
        assertTrue(waitForCondition(() -> writeIntentSwitchRequestCount.get() > 0, SECONDS.toMillis(10)));

        // ... but not more than one.
        waitForCondition(() -> writeIntentSwitchRequestCount.get() > 1, SECONDS.toMillis(1));
        assertThat(writeIntentSwitchRequestCount.get(), is(1));
    }
}
