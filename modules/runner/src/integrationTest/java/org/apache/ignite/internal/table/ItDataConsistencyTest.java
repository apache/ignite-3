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

import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.executeUpdate;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.Transaction;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test data consistency in mixed read-write load.
 */
public class ItDataConsistencyTest extends ClusterPerClassIntegrationTest {
    private static final String ZONE_NAME = "test_zone";
    private static final String TABLE_NAME = "accounts";
    private static final int WRITE_PARALLELISM = Runtime.getRuntime().availableProcessors();
    private static final int READ_PARALLELISM = 1;
    private static final int ACCOUNTS_COUNT = WRITE_PARALLELISM * 10;
    private static final double INITIAL = 1000;
    private static final double TOTAL = ACCOUNTS_COUNT * INITIAL;
    private static final int DURATION_MILLIS = 10000;

    private CyclicBarrier startBar = new CyclicBarrier(WRITE_PARALLELISM + READ_PARALLELISM, () -> log.info("Before test"));
    private LongAdder ops = new LongAdder();
    private LongAdder fails = new LongAdder();
    private LongAdder readOps = new LongAdder();
    private LongAdder readFails = new LongAdder();
    private AtomicBoolean stop = new AtomicBoolean();
    private Random rng = new Random();
    private AtomicReference<Throwable> firstErr = new AtomicReference<>();

    @BeforeAll
    public void createTables() {
        String zoneSql = "create zone " + ZONE_NAME + " (partitions 10, replicas 1) storage profiles ['" + DEFAULT_STORAGE_PROFILE + "']";
        String sql = "create table " + TABLE_NAME + " (accountNumber BIGINT PRIMARY KEY, balance DOUBLE)" + " zone " + ZONE_NAME;

        CLUSTER.doInSession(0, session -> {
            executeUpdate(zoneSql, session);
            executeUpdate(sql, session);
        });
    }

    @BeforeEach
    public void clearTables() {
        for (Table t : CLUSTER.aliveNode().tables().tables()) {
            sql("DELETE FROM " + t.name());
        }

        initAccounts();
    }

    @AfterAll
    public void dropTables() {
        dropAllTables();
    }

    @Test
    public void testDataConsistency() throws InterruptedException {
        Thread[] threads = new Thread[WRITE_PARALLELISM];

        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(createWriter(i));

            threads[i].setName("Write-Worker-" + i);
            threads[i].setUncaughtExceptionHandler((t, e) -> firstErr.compareAndExchange(null, e));
            threads[i].start();
        }

        Thread[] readThreads = new Thread[READ_PARALLELISM];

        for (int i = 0; i < readThreads.length; i++) {
            readThreads[i] = new Thread(createReader(i));

            readThreads[i].setName("Read-Worker-" + i);
            readThreads[i].setUncaughtExceptionHandler((t, e) -> firstErr.compareAndExchange(null, e));
            readThreads[i].start();
        }

        long cur = System.currentTimeMillis();

        while (cur + DURATION_MILLIS > System.currentTimeMillis()) {
            Thread.sleep(1000);

            log.info("Waiting...");

            if (firstErr.get() != null) {
                throw new IgniteException(INTERNAL_ERR, firstErr.get());
            }
        }

        stop.set(true);

        for (Thread thread : threads) {
            thread.join(3_000);
        }
        for (Thread readThread : readThreads) {
            readThread.join(3_000);
        }

        validate();
    }

    private void initAccounts() {
        Ignite node = node(0); // Intentional.
        Table accounts = node.tables().table("accounts");

        for (int i = 0; i < ACCOUNTS_COUNT; i++) {
            accounts.recordView().upsert(null, makeValue(i, 1000));
        }

        double total0 = 0;

        for (long i = 0; i < ACCOUNTS_COUNT; i++) {
            double balance = accounts.recordView().get(null, makeKey(i)).doubleValue("balance");

            total0 += balance;
        }

        assertEquals(TOTAL, total0, "Total amount invariant is not preserved");
    }

    private void validate() {
        if (firstErr.get() != null) {
            throw new IgniteException(INTERNAL_ERR, firstErr.get());
        }

        Ignite node = node(0);
        Table accounts = node.tables().table("accounts");

        log.info("After test ops={} fails={} readOps={} readFails={}", ops.sum(), fails.sum(), readOps.sum(), readFails.sum());

        double total0 = 0;

        for (long i = 0; i < ACCOUNTS_COUNT; i++) {
            double balance = accounts.recordView().get(null, makeKey(i)).doubleValue("balance");

            total0 += balance;
        }

        assertEquals(TOTAL, total0, "Total amount invariant is not preserved");
    }

    private Runnable createWriter(int workerId) {
        return () -> {
            try {
                startBar.await();
            } catch (Exception e) {
                fail();
            }

            logger().info("Starting writer: workerId=" + workerId);

            while (!stop.get() && firstErr.get() == null) {
                Ignite node = assignNodeForIteration(workerId);
                Transaction tx = node.transactions().begin();

                var view = node.tables().table("accounts").recordView();

                try {
                    long acc1 = rng.nextInt(ACCOUNTS_COUNT);

                    double amount = 100 + rng.nextInt(500);

                    double val0 = view.get(tx, makeKey(acc1)).doubleValue("balance");

                    long acc2 = acc1;

                    while (acc1 == acc2) {
                        acc2 = rng.nextInt(ACCOUNTS_COUNT);
                    }

                    double val1 = view.get(tx, makeKey(acc2)).doubleValue("balance");

                    view.upsert(tx, makeValue(acc1, val0 - amount));

                    view.upsert(tx, makeValue(acc2, val1 + amount));

                    tx.commit();

                    ops.increment();
                } catch (IgniteException e) {
                    assertTrue(e.getMessage().contains("Failed to acquire a lock"), e.getMessage());

                    // Don't need to rollback manually if got IgniteException.
                    fails.increment();
                }
            }
        };
    }

    private Runnable createReader(int workerId) {
        return () -> {
            try {
                startBar.await();
            } catch (Exception e) {
                fail();
            }

            List<Tuple> keys = new ArrayList<>();

            for (int i = 0; i < ACCOUNTS_COUNT; i++) {
                keys.add(makeKey(i));
            }

            logger().info("Starting reader: workerId=" + workerId);

            while (!stop.get() && firstErr.get() == null) {
                Ignite node = assignNodeForIteration(workerId);

                var view = node.tables().table("accounts").recordView();

                try {
                    List<Tuple> vals = view.getAll(null, keys);

                    double sum = 0;

                    for (int i = 0; i < vals.size(); i++) {
                        Tuple key = keys.get(i);
                        Tuple val = vals.get(i);

                        assertNotNull(val, "Value is null for key=" + key);
                        sum += val.doubleValue("balance");
                    }

                    assertEquals(TOTAL, sum);

                    readOps.increment();
                } catch (Exception e) {
                    assertTrue(e.getMessage().contains("Failed to acquire a lock"), e.getMessage());

                    readFails.increment();
                }
            }
        };
    }

    protected Ignite assignNodeForIteration(int workerId) {
        return CLUSTER.node(workerId % initialNodes());
    }

    protected Tuple makeKey(long id) {
        return Tuple.create().set("accountNumber", id);
    }

    protected Tuple makeValue(long id, double balance) {
        return Tuple.create().set("accountNumber", id).set("balance", balance);
    }
}
