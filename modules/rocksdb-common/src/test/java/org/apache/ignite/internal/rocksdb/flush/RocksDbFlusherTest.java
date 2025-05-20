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

package org.apache.ignite.internal.rocksdb.flush;

import static java.util.concurrent.CompletableFuture.allOf;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.IntStream;
import org.apache.ignite.internal.components.NoOpLogSyncer;
import org.apache.ignite.internal.testframework.ExecutorServiceExtension;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.testframework.InjectExecutorService;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.filter.AbstractFilter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

@ExtendWith(ExecutorServiceExtension.class)
class RocksDbFlusherTest extends IgniteAbstractTest {
    private RocksDbFlusher flusher;

    private Options dbOptions;

    private RocksDB db;

    private final CompletableFuture<Throwable> failureProcessorError = new CompletableFuture<>();

    @BeforeEach
    void setUp(
            @InjectExecutorService ScheduledExecutorService scheduledExecutor,
            @InjectExecutorService ExecutorService executor
    ) throws RocksDBException {
        flusher = new RocksDbFlusher(
                "RocksDbFlusherTest",
                new IgniteSpinBusyLock(),
                scheduledExecutor,
                executor,
                () -> 0,
                new NoOpLogSyncer(),
                failureCtx -> failureProcessorError.completeExceptionally(failureCtx.error()),
                () -> {}
        );

        dbOptions = new Options()
                .setCreateIfMissing(true)
                .setListeners(List.of(flusher.listener()));

        db = RocksDB.open(dbOptions, workDir.toString());

        flusher.init(db, List.of(db.getDefaultColumnFamily()));

        setUpLogFilter();
    }

    /**
     * Sets a filter that removes warning messages produced by the flusher. This is needed, because the CI server has been configured to
     * fail the build if these warnings are found in the logs. In this test we intentionally create a situation where such warnings will
     * be produced.
     */
    private static void setUpLogFilter() {
        var filter = new AbstractFilter() {
            @Override
            public Result filter(LogEvent event) {
                if (event.getMessage().getFormattedMessage().contains("Unable to perform explicit flush, will try again.")) {
                    return Result.DENY;
                }

                return Result.NEUTRAL;
            }
        };

        var context = (LoggerContext) LogManager.getContext(false);

        LoggerConfig loggerConfig = context.getConfiguration().getLoggerConfig(RocksDbFlusher.class.getName());

        loggerConfig.addFilter(filter);

        context.updateLoggers();
    }

    @AfterEach
    void tearDown() throws Exception {
        closeAll(flusher::stop, db, dbOptions);
    }

    @Test
    void testFlushRetryOnWriteThrottling() {
        CompletableFuture<?>[] flushFutures = IntStream.range(0, 200)
                .mapToObj(ByteUtils::intToBytes)
                .map(bytes -> {
                    try {
                        // Wait a little bit, because otherwise batching still works and less flushes will be issued.
                        Thread.sleep(1);

                        db.put(bytes, bytes);
                    } catch (RocksDBException | InterruptedException e) {
                        throw new AssertionError(e);
                    }

                    return flusher.awaitFlush(true);
                })
                .toArray(CompletableFuture[]::new);

        assertThat(allOf(flushFutures), willCompleteSuccessfully());

        if (failureProcessorError.isCompletedExceptionally()) {
            failureProcessorError.join();
        }
    }
}
