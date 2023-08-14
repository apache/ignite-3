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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
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
    /** Logger name. */
    private final String loggerName;

    /** List of handlers. */
    private final List<Handler> handlers = new ArrayList<>(1);

    /** Lock that is used to synchronize the collection of handlers. */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /** Test log appender that is used to check log events/messages. */
    private final TestLogAppender appender;

    /** Logger configuration. */
    private Configuration config;

    /**
     * Creates a new instance of {@link TestLogChecker} for the given {@code loggerName}.
     *
     * @return New instance of {@link TestLogChecker}.
     **/
    public static TestLogChecker create(String loggerName) {
        return create(loggerName, false);
    }

    /**
     * Creates a new instance of {@link TestLogChecker} for the given {@code loggerName}.
     *
     * @param started Whether the checker should be started.
     * @return New instance of {@link TestLogChecker}.
     **/
    public static TestLogChecker create(String loggerName, boolean started) {
        TestLogChecker checker = new TestLogChecker(loggerName);

        if (started) {
            checker.start();
        }

        return checker;
    }

    /**
     * Creates a new instance of {@link TestLogChecker} for the given {@code clazz}.
     *
     * @return New instance of {@link TestLogChecker}.
     **/
    public static TestLogChecker create(Class<?> clazz) {
        return create(clazz, false);
    }

    /**
     * Creates a new instance of {@link TestLogChecker} for the given {@code clazz}.
     *
     * @param started Whether the checker should be started.
     * @return New instance of {@link TestLogChecker}.
     **/
    public static TestLogChecker create(Class<?> clazz, boolean started) {
        return create(clazz.getName(), started);
    }

    /**
     * Creates a new instance of {@link TestLogChecker}.
     *
     * @param loggerName Logger name.
     */
    public TestLogChecker(String loggerName) {
        Objects.requireNonNull(loggerName);

        this.loggerName = loggerName;
        this.appender = new TestLogAppender(loggerName);
    }

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
        this(loggerName, new Handler(predicate, action));
    }

    /**
     * Creates a new instance of {@link TestLogChecker} with the given {@code predicate} and {@code action}.
     *
     * @param loggerName Logger name.
     * @param handlers List of handlers to be added.
     */
    public TestLogChecker(String loggerName, Handler... handlers) {
        this(loggerName);

        lock.writeLock().lock();
        try {
            for (Handler handler : handlers) {
                this.handlers.add(handler);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Adds a new handler with the given {@code predicate} and {@code action}.
     *
     * @param predicate Predicate to check log messages.
     * @param action Action to be executed when the {@code predicate} is matched.
     * @return New instance of {@link Handler}.
     */
    public Handler addHandler(Predicate<String> predicate, Runnable action) {
        Handler handler = new Handler(predicate, action);

        lock.writeLock().lock();
        try {
            handlers.add(handler);
        } finally {
            lock.writeLock().unlock();
        }

        return handler;
    }

    /**
     * Adds a new handler with the given {@code predicate} and {@code action}.
     *
     * @param handler Handler to be added.
     */
    public void addHandler(Handler handler) {
        lock.writeLock().lock();
        try {
            handlers.add(handler);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Removes the given {@code handler}.
     *
     * @param handler Handler to be removed.
     */
    public void removeHandler(Handler handler) {
        Objects.requireNonNull(handler);

        lock.writeLock().lock();
        try {
            handlers.remove(handler);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Checks if any of the predicates is matched.
     *
     * @return {@code true} if one of the predicates is matched, {@code false} otherwise.
     */
    public boolean isMatched() {
        lock.readLock().lock();

        try {
            return handlers.stream().anyMatch(handler -> handler.isMatched.get());
        } finally {
            lock.readLock().unlock();
        }
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

    /**
     * Log event handler.
     */
    public static class Handler {
        /** Predicate that is used to check log messages. */
        private final Predicate<String> predicate;

        /** Action to be executed when the {@code predicate} is matched. */
        private final Runnable action;

        /** Flag indicating whether the predicate is matched. */
        private final AtomicBoolean isMatched = new AtomicBoolean();

        /**
         * Creates a new instance of {@link Handler}.
         *
         * @param predicate Predicate to check log messages.
         * @param action Action to be executed when the {@code predicate} is matched.
         */
        public Handler(Predicate<String> predicate, Runnable action) {
            Objects.requireNonNull(predicate);
            Objects.requireNonNull(action);

            this.predicate = predicate;
            this.action = action;
        }

        /**
         * Checks if the predicate is matched.
         *
         * @return {@code true} if the predicate is matched, {@code false} otherwise.
         */
        public boolean isMatched() {
            return isMatched.get();
        }
    }

    private class TestLogAppender extends AbstractAppender {
        public TestLogAppender(final String name) {
            super(name, null, null, true, Property.EMPTY_ARRAY);
        }

        @Override
        public void append(final LogEvent event) {
            if (!loggerName.equals(event.getLoggerName())) {
                return;
            }

            lock.readLock().lock();
            try {
                handlers.forEach(handler -> {
                    if (handler.predicate.test(event.getMessage().getFormattedMessage())) {
                        handler.isMatched.set(true);
                        handler.action.run();
                    }
                });
            } finally {
                lock.readLock().unlock();
            }
        }
    }
}
