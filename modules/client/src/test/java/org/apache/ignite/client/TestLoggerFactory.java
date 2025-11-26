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

package org.apache.ignite.client;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.System.Logger;
import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;
import java.util.function.Supplier;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.lang.LoggerFactory;

/**
 * Logger factory for tests.
 */
public class TestLoggerFactory implements LoggerFactory {
    final ListLogger logger;

    TestLoggerFactory(String factoryName) {
        this.logger = new ListLogger(factoryName);
    }

    @Override
    public Logger forName(String name) {
        return logger;
    }

    void assertLogContains(String msg) {
        assertTrue(logContains(msg), this::log);
    }

    void assertLogDoesNotContain(String msg) {
        assertFalse(logContains(msg), this::log);
    }

    void waitForLogContains(String msg, long timeoutMillis) throws InterruptedException {
        assertTrue(
                IgniteTestUtils.waitForCondition(() -> logContains(msg), timeoutMillis),
                () -> "Log does not contain expected message '" + msg + "': " +  log());
    }

    void waitForLogMatches(String regex, long timeoutMillis) throws InterruptedException {
        assertTrue(
                IgniteTestUtils.waitForCondition(() -> logLineMatches(regex), timeoutMillis),
                () -> "Log does not match expected pattern '" + regex + "': " +  log());
    }

    private boolean logContains(String msg) {
        return logger.entries().stream().anyMatch(x -> x.contains(msg));
    }

    private boolean logLineMatches(String regex) {
        return logger.entries().stream().anyMatch(x -> x.matches(regex));
    }

    private String log() {
        return String.join("\n", logger.entries());
    }

    /** Logger that stores all messages in a list. */
    public static class ListLogger implements System.Logger {
        private final String name;

        private final List<String> logEntries = new ArrayList<>();

        ListLogger(String name) {
            this.name = name;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public boolean isLoggable(Level level) {
            return true;
        }

        @Override
        public void log(Level level, String msg) {
            captureLog(msg);
        }

        @Override
        public void log(Level level, Supplier<String> msgSupplier) {
            throw new AssertionError("Should not be called");
        }

        @Override
        public void log(Level level, Object obj) {
            throw new AssertionError("Should not be called");
        }

        @Override
        public void log(Level level, String msg, Throwable thrown) {
            captureLog(msg);
        }

        @Override
        public void log(Level level, Supplier<String> msgSupplier, Throwable thrown) {
            throw new AssertionError("Should not be called");
        }

        @Override
        public void log(Level level, String format, Object... params) {
            throw new AssertionError("Should not be called");
        }

        @Override
        public void log(Level level, ResourceBundle bundle, String msg, Throwable thrown) {
            throw new AssertionError("Should not be called");
        }

        @Override
        public void log(Level level, ResourceBundle bundle, String format, Object... params) {
            throw new AssertionError("Should not be called");
        }

        public synchronized List<String> entries() {
            return new ArrayList<>(logEntries);
        }

        private synchronized void captureLog(String msg) {
            logEntries.add(name + ":" + msg);
        }
    }
}
