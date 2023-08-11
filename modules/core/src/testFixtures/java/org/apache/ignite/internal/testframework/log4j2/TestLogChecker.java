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

package org.apache.ignite.internal.testframework.log4j2;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.Property;

/**
 * Test log checker.
 */
public class TestLogChecker {
    /** Test log appender that is used to check log events/messages. */
    private final TestLogAppender appender;

    /** Logger configuration. */
    private Configuration config;

    /**
     * Creates a new instance of {@link TestLogChecker} with the given {@code predicate}.
     *
     * @param loggerName Logger name.
     * @param predicate Predicate to check log messages.
     */
    public TestLogChecker(String loggerName, Predicate<String> predicate) {
        this(loggerName, predicate, () -> {});
    }

    /**
     * Creates a new instance of {@link TestLogChecker} with the given {@code predicate} and {@code action}.
     *
     * @param loggerName Logger name.
     * @param predicate Predicate to check log messages.
     * @param action Action to be executed when the {@code predicate} is matched.
     */
    public TestLogChecker(String loggerName, Predicate<String> predicate, Runnable action) {
        Objects.requireNonNull(loggerName);
        Objects.requireNonNull(predicate);
        Objects.requireNonNull(action);

        this.appender = new TestLogAppender(loggerName, predicate, action);
    }

    /**
     * Checks if the {@code predicate} is matched.
     *
     * @return {@code true} if the {@code predicate} is matched, {@code false} otherwise.
     */
    public boolean isMatched() {
        return appender.isMatched();
    }

    /**
     * Starts the test log checker.
     */
    public void start() {
        if (config != null) {
            throw new IllegalStateException("TestLogChecker is already started.");
        }

        Logger logger = (Logger) LogManager.getLogger(appender.getName());

        config = logger.getContext().getConfiguration();

        appender.start();

        addAppender(appender, config);
    }

    /**
     * Stops the test log checker.
     */
    public void stop() {
        if (config == null) {
            throw new IllegalStateException("TestLogChecker is not started.");
        }

        appender.stop();

        removeAppender(appender, config);
    }

    private static synchronized void addAppender(Appender appender, Configuration config) {
        for (final LoggerConfig loggerConfig : config.getLoggers().values()) {
            loggerConfig.addAppender(appender, null, null);
        }
        config.getRootLogger().addAppender(appender, null, null);
    }

    private static synchronized void removeAppender(Appender appender, Configuration config) {
        for (final LoggerConfig loggerConfig : config.getLoggers().values()) {
            loggerConfig.removeAppender(appender.getName());
        }
        config.getRootLogger().removeAppender(appender.getName());
    }

    private static class TestLogAppender extends AbstractAppender {
        private final AtomicBoolean isMatched = new AtomicBoolean();
        private final Predicate<String> predicate;
        private final Runnable action;

        public TestLogAppender(final String name, Predicate<String> predicate, Runnable action) {
            super(name, null, null, true, Property.EMPTY_ARRAY);
            this.predicate = predicate;
            this.action = action;
        }

        public boolean isMatched() {
            return isMatched.get();
        }

        @Override
        public void append(final LogEvent event) {
            if (predicate.test(event.getMessage().getFormattedMessage())) {
                isMatched.set(true);
                action.run();
            }
        }
    }
}
