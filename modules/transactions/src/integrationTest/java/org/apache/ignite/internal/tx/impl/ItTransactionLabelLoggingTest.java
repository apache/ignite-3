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

package org.apache.ignite.internal.tx.impl;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.testframework.log4j2.Log4jUtils;
import org.apache.ignite.internal.testframework.log4j2.LogInspector;
import org.apache.ignite.tx.TransactionOptions;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.junit.jupiter.api.Test;

/**
 * Test to verify that transaction labels appear in logs for various negative scenarios.
 */
public class ItTransactionLabelLoggingTest extends ClusterPerTestIntegrationTest {
    @Override
    protected int initialNodes() {
        return 1;
    }

    @Test
    void testTransactionLabelInTimeoutLogs() {
        AutoCloseable debugLogging = enableDebugLogging(TransactionExpirationRegistry.class);
        LogInspector timeoutLogInspector = LogInspector.create(TransactionExpirationRegistry.class);
        AtomicInteger timeoutLabelCount = new AtomicInteger(0);
        String label = "TIMEOUT-TEST";
        timeoutLogInspector.addHandler(
                logEvent -> {
                    String message = logEvent.getMessage().getFormattedMessage();
                    return message.contains("Transaction has aborted due to timeout")
                            && message.contains("txLabel=" + label);
                },
                timeoutLabelCount::incrementAndGet
        );
        timeoutLogInspector.start();

        try {
            Ignite ignite = cluster.aliveNode();

            // Create a transaction with a custom label and very short timeout.
            ignite.transactions().begin(
                    new TransactionOptions()
                            .label(label)
                            .timeoutMillis(1) // 1ms timeout to trigger expiration quickly.
            );

            await("Expected to find transaction label in timeout log message")
                    .atMost(Duration.ofSeconds(10L)).untilAtomic(timeoutLabelCount, is(greaterThan(0)));
        } finally {
            timeoutLogInspector.stop();
            closeQuietly(debugLogging);
        }
    }

    private static AutoCloseable enableDebugLogging(Class<?> loggerClass) {
        Log4jUtils.waitTillConfigured();

        LoggerContext context = (LoggerContext) LogManager.getContext(false);
        Configuration config = context.getConfiguration();
        String loggerName = loggerClass.getName();

        LoggerConfig existing = config.getLoggerConfig(loggerName);
        boolean exactMatch = loggerName.equals(existing.getName());

        if (exactMatch) {
            Level previous = existing.getLevel();
            existing.setLevel(Level.DEBUG);
            context.updateLoggers();
            return () -> {
                existing.setLevel(previous);
                context.updateLoggers();
            };
        }

        LoggerConfig loggerConfig = new LoggerConfig(loggerName, Level.DEBUG, true);
        config.addLogger(loggerName, loggerConfig);
        context.updateLoggers();

        return () -> {
            config.removeLogger(loggerName);
            context.updateLoggers();
        };
    }

    private static void closeQuietly(AutoCloseable closeable) {
        try {
            closeable.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
