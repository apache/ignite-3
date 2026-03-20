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

package org.apache.ignite.internal.metastorage.impl;

import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.util.ExceptionUtils.hasCause;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.ConfigOverride;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.testframework.failure.FailureManagerExtension;
import org.apache.ignite.internal.testframework.failure.MuteFailureManagerLogging;
import org.apache.ignite.internal.testframework.log4j2.LogInspector;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(FailureManagerExtension.class)
@MuteFailureManagerLogging
@ConfigOverride(name = "ignite.failureHandler.handler.type", value = "noop")
class ItBrokenWatchLoggingTest extends ClusterPerTestIntegrationTest {
    @Override
    protected int initialNodes() {
        return 1;
    }

    @Test
    void brokenWatchChainLogsOriginalExceptionJustOnce() throws Exception {
        MetaStorageManager metaStorageManager = unwrapIgniteImpl(cluster.node(0)).metaStorageManager();

        registerWatchBreakingNotifications(metaStorageManager);

        AtomicInteger logCount = new AtomicInteger();

        LogInspector logInspector = new LogInspector(
                LogInspector.ALL_LOGGERS,
                evt -> hasCause(evt.getThrown(), BreakingException.class),
                logCount::incrementAndGet
        );
        logInspector.start();

        try {
            await("Must observe at least one logging event")
                    .until(logCount::get, greaterThan(0));

            //noinspection deprecation
            assertFalse(
                    waitForCondition(() -> logCount.get() > 1, SECONDS.toMillis(2)),
                    () -> "Must not observe more than one logging event, but observed " + logCount.get()
            );
        } finally {
            logInspector.stop();
        }
    }

    private static void registerWatchBreakingNotifications(MetaStorageManager metaStorageManager) {
        BreakingException exception = new BreakingException("Oops " + randomUUID());
        metaStorageManager.registerPrefixWatch(new ByteArray(""), event -> failedFuture(exception));
    }

    private static class BreakingException extends RuntimeException {
        private static final long serialVersionUID = -8004315686224616385L;

        private BreakingException(String message) {
            super(message);
        }
    }
}
