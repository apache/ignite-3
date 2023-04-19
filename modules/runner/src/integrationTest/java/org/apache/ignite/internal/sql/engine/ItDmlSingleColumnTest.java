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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.function.Consumer;
import org.apache.ignite.Ignite;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

public class ItDmlSingleColumnTest extends ClusterPerClassIntegrationTest {
    private static final String TABLE_NAME = "test";

    @Override
    protected int nodes() {
        return 1;
    }

    /**
     * Clear tables after each test.
     *
     * @param testInfo Test information object.
     * @throws Exception If failed.
     */
    @AfterEach
    @Override
    public void tearDown(TestInfo testInfo) throws Exception {
        sql("delete from " + TABLE_NAME);
        tearDownBase(testInfo);
    }

    @BeforeAll
    public void beforeAll() {
        sql("create zone test_zone with partitions=1, replicas=3;");
        sql("create table test (ID int primary key) with primary_zone='TEST_ZONE'");
    }

    @ParameterizedTest
    @EnumSource(TransactionType.class)
    public void testRecordView(TransactionType txType) {
        Table tab = CLUSTER_NODES.get(0).tables().table(TABLE_NAME);
        RecordView<Tuple> view = tab.recordView();

        runInTx(txType, tx -> {
            Tuple key = Tuple.create().set("id", 0);
            Transaction rwTx = tx != null && !tx.isReadOnly() ? tx : null;

            view.upsert(rwTx, key);
            view.upsert(rwTx, key);

            assertNotNull(view.get(tx, key));
            assertEquals(1, view.getAll(tx, Collections.singleton(key)).size());
        });
    }

    @ParameterizedTest
    @EnumSource(TransactionType.class)
    public void testKVView(TransactionType txType) {
        Table tab = CLUSTER_NODES.get(0).tables().table(TABLE_NAME);

        runInTx(txType, tx -> {
            KeyValueView<Integer, Void> view = tab.keyValueView(Integer.class, Void.class);
            Transaction rwTx = tx != null && !tx.isReadOnly() ? tx : null;

            view.put(rwTx, 0, null);
            view.put(rwTx, 0, null);

            assertNotNull(view.getNullable(tx, 0));
            assertTrue(view.contains(tx, 0));
            assertEquals(1, view.getAll(tx, Collections.singleton(0)).size());
        });
    }

    @ParameterizedTest
    @EnumSource(TransactionType.class)
    public void testSqlApi(TransactionType txType) {
        runInTx(txType, tx -> {
            sql("insert into test values (0), (1)");

            assertQuery("select * from test").returns(2);
        });
    }

    private enum TransactionType {
        NO_TX,
        RO_TX,
        RW_TX
    }

    private void runInTx(TransactionType txType, Consumer<Transaction> call) {
        Ignite ignite = CLUSTER_NODES.get(0);

        switch (txType) {
            case NO_TX:
                call.accept(null);

                return;

            case RO_TX:
                ignite.transactions().runInTransaction(call, new TransactionOptions().readOnly(true));

                return;

            case RW_TX:
                ignite.transactions().runInTransaction(call, new TransactionOptions().readOnly(false));
        }
    }

    private void ensureRowsCount(long expected) {
        assertQuery("select * from test").returns(expected);
    }

    @Test
    @Disabled
    public void testSinglePkTableScan() {

        // with primary_zone='TEST_ZONE'
        String tabName1 = "myTbl";

        String tabName2 = "myTbl2Cols";

        sql("CREATE TABLE " + tabName1 + " (id INT PRIMARY KEY)");
//        sql("INSERT INTO " + tabName1 + " VALUES (0), (0), (1)");

//        if (true)
//            return;

        sql("CREATE TABLE " + tabName2 + " (id INT PRIMARY KEY, val INT)");
//        sql("INSERT INTO " + tabName2 + " VALUES (0, 0), (1, 1), (1, 2)");

        Ignite ignite = CLUSTER_NODES.get(0);

//        ignite.transactions().runInTransaction(tx -> {
//            List<List<Object>> rows = sql(tx, "SELECT count(*) from " + tabName1);
//            assertEquals(2L, rows.get(0).get(0));
//        });

        Table tab = ignite.tables().table(tabName1);
        Table tab2 = ignite.tables().table(tabName2);

//        tab.recordView().upsert(null, Tuple.create().set("id", 1));
//        tab.recordView().upsert(null, Tuple.create().set("id", 1));
        tab.keyValueView(Integer.class, Void.class).put(null, 1, null);
        tab.keyValueView(Integer.class, Void.class).put(null, 1, null);


//        tab2.recordView().upsert(null, Tuple.create().set("id", 1).set("val", 1));
//        tab2.recordView().upsert(null, Tuple.create().set("id", 1).set("val", 2));
        tab2.keyValueView(Integer.class, Integer.class).put(null, 1, 1);
        tab2.keyValueView(Integer.class, Integer.class).put(null, 1, 2);

//        tab2.recordView().upsert(null, Tuple.create().set("id", 1));
//        tab2.recordView().upsert(null, Tuple.create().set("id", 1));
//        tab2.keyValueView(Integer.class, Void.class).put(null, 1, null);

        System.out.println(">xxx> RECORD VIEW SINGLE");
        checkOp(ignite, tx -> {
            Object obj = tab.recordView().get(tx, Tuple.create().set("id", 1));

            if (obj != null)
                System.out.println(">xxx> cls=" + obj.getClass().getSimpleName() + " " + obj);
        });

        System.out.println(">xxx> KV VIEW SINGLE");

        checkOp(ignite, tx -> {
            Object obj = tab.keyValueView().get(tx, Tuple.create().set("id", 1));

            if (obj != null)
                System.out.println(">xxx> cls=" + obj.getClass().getSimpleName() + " " + obj);
        });

        System.out.println(">xxx> RECORD VIEW DOUBLE");
        checkOp(ignite, tx -> {
            Object obj = tab2.recordView().get(tx, Tuple.create().set("id", 1));

            if (obj != null)
                System.out.println(">xxx> cls=" + obj.getClass().getSimpleName() + " " + obj);
        });

        System.out.println(">xxx> KV VIEW DOUBLE");

        checkOp(ignite, tx -> {
            Object obj = tab2.keyValueView().get(tx, Tuple.create().set("id", 1));

            if (obj != null)
                System.out.println(">xxx> cls=" + obj.getClass().getSimpleName() + " " + obj);
        });

        System.out.println(">xxx> tab1 " + sql("SELECT * from " + tabName1));
        System.out.println(">xxx> tab2 " + sql("SELECT * from " + tabName2));

//        ignite.transactions().runInTransaction(tx -> {
//            List<List<Object>> rows = sql(tx, "SELECT * from " + tabName1);
//
//            assertEquals(10, rows.size(), rows.toString());
//        });
    }

    private void checkOp(Ignite ignite, Consumer<Transaction> op) {
        System.out.println(">xxx> no transaction");

        op.accept(null);;

        System.out.println(">xxx> RW transaction");

        ignite.transactions().runInTransaction(op);

        System.out.println(">xxx> RO transaction");

        ignite.transactions().runInTransaction(op, new TransactionOptions().readOnly(true));
    }
}
