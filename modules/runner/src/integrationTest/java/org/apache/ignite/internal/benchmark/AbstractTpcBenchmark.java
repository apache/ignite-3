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

package org.apache.ignite.internal.benchmark;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.sql.engine.SqlQueryProcessor;
import org.apache.ignite.internal.sql.engine.statistic.SqlStatisticManagerImpl;
import org.apache.ignite.internal.sql.engine.util.TpcTable;
import org.apache.ignite.sql.IgniteSql;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

/**
 * Abstract benchmark class that initializes schema and fills up tables for TPC suite.
 */
@State(Scope.Benchmark)
@SuppressWarnings({"WeakerAccess", "unused"})
public abstract class AbstractTpcBenchmark extends AbstractMultiNodeBenchmark {
    private static final String DATASET_READY_MARK_FILE_NAME = "ready.txt";

    protected IgniteSql sql;

    abstract TpcTable[] tablesToInit();

    abstract Path pathToDataset();

    /** Initializes a schema and fills tables with data. */
    @Setup
    public void initSchema() throws Throwable {
        try {
            sql = publicIgnite.sql();

            if (!Files.exists(workDir().resolve(DATASET_READY_MARK_FILE_NAME))) {
                Path pathToDataset = pathToDataset();

                if (pathToDataset == null) {
                    throw new IllegalStateException("Path do dataset is not provided. Please read the comment"
                            + " in the beginning of " + this.getClass().getSimpleName() + ".class");
                }

                System.out.println("Going to create schema...");

                for (TpcTable table : tablesToInit()) {
                    System.out.println("Going to create table \"" + table.tableName() + "\"...");
                    sql.executeScript(table.ddlScript());
                    System.out.println("Done");

                    fillTable(table, pathToDataset);
                }

                Files.createFile(workDir().resolve(DATASET_READY_MARK_FILE_NAME));
            }

            SqlStatisticManagerImpl statisticManager = (SqlStatisticManagerImpl) ((SqlQueryProcessor) igniteImpl.queryEngine())
                    .sqlStatisticManager();

            statisticManager.forceUpdateAll();
            statisticManager.lastUpdateStatisticFuture().get(10, TimeUnit.SECONDS);
        } catch (Throwable e) {
            nodeTearDown();

            throw e;
        }
    }

    private void fillTable(TpcTable table, Path pathToDataset) throws Throwable {
        System.out.println("Going to fill table \"" + table.tableName() + "\"...");
        long start = System.nanoTime();
        Iterable<Object[]> dataProvider = () -> {
            try {
                return table.dataProvider(pathToDataset);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };

        Semaphore semaphore = new Semaphore(1024); // 1024 was chosen empirically
        AtomicReference<Throwable> exceptionHolder = new AtomicReference<>(null);
        AtomicInteger inserted = new AtomicInteger();
        int expectedCount = 0;
        for (Object[] params : dataProvider) {
            semaphore.acquire();
            sql.executeAsync(null, table.insertPrepareStatement(), params)
                    .whenComplete((ignored, ex) -> {
                        semaphore.release();

                        int val = inserted.incrementAndGet();
                        if (val % 10_000 == 0) {
                            System.out.println(val + " rows uploaded to \"" + table.tableName() + "\"");
                        }

                        if (ex != null) {
                            exceptionHolder.compareAndSet(null, ex);
                        }
                    });

            if (exceptionHolder.get() != null) {
                throw exceptionHolder.get();
            }

            expectedCount++;
        }

        while (expectedCount != inserted.intValue()) {
            if (exceptionHolder.get() != null) {
                throw exceptionHolder.get();
            }

            Thread.sleep(100);
        }

        System.out.println("Table \"" + table.tableName() + "\" filled in " + Duration.ofNanos(System.nanoTime() - start));
    }

    @Override
    protected void createTable(String tableName) {
        // NO-OP
    }
}
