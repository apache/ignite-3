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

import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_TEST_PROFILE_NAME;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.TestWrappers.unwrapTableImpl;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.setZoneAutoAdjustScaleUpToImmediate;
import static org.apache.ignite.internal.lang.IgniteSystemProperties.colocationEnabled;

import org.apache.ignite.Ignite;
import org.apache.ignite.InitParametersBuilder;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.testframework.TestIgnitionManager;
import org.apache.ignite.internal.wrapper.Wrappers;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Set of tests to make sure thin client uses suitable {@link HybridTimestampTracker} for sql. */
@SuppressWarnings("ConcatenationWithEmptyString")
public class ItClientObservableTimeTest extends ClusterPerClassIntegrationTest {
    private static final String ZONE_NAME = "TEST_ZONE_Z";

    private static final String TABLE_NAME = "TEST_TABLE_Z";

    @Override
    protected int initialNodes() {
        return 1; //2;
    }

    @BeforeAll
    void setUp() {
        CatalogManager catalogManager = unwrapIgniteImpl(node(0)).catalogManager();
        String defaultZoneName = catalogManager.catalog(catalogManager.latestCatalogVersion()).defaultZone().name();

        if (colocationEnabled()) {
            // Generally it's required to await default zone dataNodesAutoAdjustScaleUp timeout in order to treat zone as ready one.
            // In order to eliminate awaiting interval, default zone scaleUp is altered to be immediate.
            setZoneAutoAdjustScaleUpToImmediate(catalogManager, defaultZoneName);
        }
    }

    @AfterEach
    void cleanUp() {
        dropAllTables();
    }

    @Override
    protected void configureInitParameters(InitParametersBuilder builder) {
        builder.clusterConfiguration(TestIgnitionManager.PRODUCTION_CLUSTER_CONFIG_STRING);
    }

    //@Test
    void testAAA() throws Exception {
        //"CREATE TABLE IF NOT EXISTS {} (id INT PRIMARY KEY, name VARCHAR, salary DOUBLE) ZONE \"{}\"",

        TableImpl table = unwrapTableImpl(createZoneAndTable(ZONE_NAME, TABLE_NAME, 2, 1, DEFAULT_TEST_PROFILE_NAME));

        Thread.sleep(5_000);

        for (int i = 0; i < initialNodes(); ++i) {
            log.warn(">>>>> node ids: " + i + ", " + node(i).cluster().localNode().id());
        }
        Ignite ignite = node(1);
        KeyValueView<Tuple, Tuple> kv = ignite.tables().table(TABLE_NAME).keyValueView();

        Tuple key = Tuple.create().set("id", 12);
        Tuple value = Tuple.create().set("name", "test-name").set("salary", 1.0);

        log.warn(">>>>> implicit rw tx");
        kv.put(null, key, value);

        log.warn(">>>>> explicit rw tx");
        Transaction tx = ignite.transactions().begin();
        kv.put(tx, key, value);
        tx.commit();

        log.warn(">>>>> empty explicit rw tx");
        tx = ignite.transactions().begin();
        tx.commit();

        log.warn(">>>>> run in tx");
        ignite.transactions().runInTransaction(txx -> {
            kv.put(txx, key, value);
        });

        log.warn(">>>>> explicit ro tx");
        tx = ignite.transactions().begin(new TransactionOptions().readOnly(true));
        kv.get(tx, key);
        tx.commit();

        log.warn(">>>>> empty explicit ro tx");
        tx = ignite.transactions().begin(new TransactionOptions().readOnly(true));
        tx.commit();

        log.warn(">>>>> implicit ro tx");
        kv.get(null, key);

        log.warn(">>>>> rollback explicit ro tx");
        tx = ignite.transactions().begin(new TransactionOptions().readOnly(true));
        kv.get(tx, key);
        tx.rollback();

        try (IgniteClient client1 = newClient(1)) {
            KeyValueView<Tuple, Tuple> kv2 = client1.tables().table(TABLE_NAME).keyValueView();

            log.warn(">>>>> client explicit rw tx");
            Transaction txclient = client1.transactions().begin();
            kv2.put(txclient, key, value);
            txclient.commit();

            log.warn(">>>>> client explicit ro tx");
            txclient = client1.transactions().begin(new TransactionOptions().readOnly(true));
            kv2.get(txclient, key);
            txclient.commit();
        }
        Thread.sleep(30_000);
    }

    @Test
    void observableTimeAdjustmentWorksForQueryStartedFromThinClient() {
        try (
                IgniteClient client1 = newClient();
                IgniteClient client2 = newClient();
                ResultSet<?> ignored = client1.sql().execute(null, "CREATE TABLE my_table (id INT PRIMARY KEY, val INT)")
        ) {
            // Should not throw
            try (ResultSet<?> rs = client2.sql().execute(null, "SELECT * FROM my_table")) {
                while (rs.hasNext()) {
                    rs.next();
                }
            }
        }
    }

    @Test
    void startOfRoTxInScriptAccountForPreviouslyExecutedStatements() {
        try (IgniteClient client = newClient()) {
            // Should not throw
            client.sql().executeScript(""
                    + "CREATE TABLE my_table (id INT PRIMARY KEY, val INT);"
                    + "START TRANSACTION READ ONLY;"
                    + "SELECT COUNT(val) FROM my_table;"
                    + "COMMIT;"
            );
        }
    }

    private static IgniteClient newClient() {
        return IgniteClient.builder()
                .addresses("localhost:" + Wrappers.unwrap(CLUSTER.aliveNode(), IgniteImpl.class).clientAddress().port())
                .build();
    }
    private static IgniteClient newClient(int i) {
        return IgniteClient.builder()
                .addresses("localhost:" + Wrappers.unwrap(CLUSTER.node(i), IgniteImpl.class).clientAddress().port())
                .build();
    }
}
