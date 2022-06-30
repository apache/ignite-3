/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.lang.System.Logger;
import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;
import java.util.function.Supplier;
import org.apache.ignite.client.fakes.FakeIgnite;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.LoggerFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Tests verifies an ability to set custom logger to the client.
 */
public class ClientLoggingTest {
    /** Test server. */
    TestServer server;

    /** Test server 2. */
    TestServer server2;

    @AfterEach
    void tearDown() throws Exception {
        IgniteUtils.closeAll(server, server2);
    }

    @Test
    public void loggersSetToDifferentClientsNotInterfereWithEachOther() throws Exception {
        FakeIgnite ignite1 = new FakeIgnite();
        ignite1.tables().createTable("t", c -> {});

        server = startServer(10900, ignite1);

        var loggerFactory1 = new TestLoggerFactory("client1");
        var loggerFactory2 = new TestLoggerFactory("client2");

        var client1 = createClient(loggerFactory1);
        var client2 = createClient(loggerFactory2);

        assertEquals("t", client1.tables().tables().get(0).name());
        assertEquals("t", client2.tables().tables().get(0).name());

        assertThat(loggerFactory1.logger.entries(), empty());
        assertThat(loggerFactory2.logger.entries(), empty());

        server.close();

        FakeIgnite ignite2 = new FakeIgnite();
        ignite2.tables().createTable("t2", c -> {});

        server2 = startServer(10950, ignite2);

        assertEquals("t2", client1.tables().tables().get(0).name());
        assertEquals("t2", client2.tables().tables().get(0).name());

        assertThat(loggerFactory1.logger.entries(), not(empty()));
        assertThat(loggerFactory2.logger.entries(), not(empty()));

        loggerFactory1.logger.entries().forEach(msg -> assertThat(msg, startsWith("client1:")));
        loggerFactory2.logger.entries().forEach(msg -> assertThat(msg, startsWith("client2:")));
    }

    private TestServer startServer(int port, FakeIgnite ignite) {
        return AbstractClientTest.startServer(
                port,
                10,
                0,
                ignite
        );
    }

    private IgniteClient createClient(LoggerFactory loggerFactory) {
        return IgniteClient.builder()
                .addresses("127.0.0.1:10900..10910", "127.0.0.1:10950..10960")
                .retryPolicy(new RetryLimitPolicy().retryLimit(1))
                .loggerFactory(loggerFactory)
                .build();
    }

    private static class TestLoggerFactory implements LoggerFactory {
        private final SimpleCapturingLogger logger;

        public TestLoggerFactory(String factoryName) {
            this.logger = new SimpleCapturingLogger(factoryName);
        }

        @Override
        public Logger forName(String name) {
            return logger;
        }
    }

    private static class SimpleCapturingLogger implements System.Logger {
        private final String name;

        private final List<String> logEntries = new ArrayList<>();

        public SimpleCapturingLogger(String name) {
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

        public List<String> entries() {
            return logEntries;
        }

        private void captureLog(String msg) {
            logEntries.add(name + ":" + msg);
        }
    }
}
