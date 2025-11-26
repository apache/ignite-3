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
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.Property;

/**
 * This class is used to check log events/messages.
 *
 * <p>
 * When it is needed to check log events/messages, the following steps should be performed:
 * </p>
 * <pre><code>
 *     // Create a new instance of LogInspector for the given logger name and specified predicate.
 *     LogInspector logInspector = new LogInspector(
 *             CustomClass.class.getName(),
 *             evt -> evt.getMessage().getFormattedMessage().matches(pattern) && evt.getLevel() == Level.ERROR);
 *
 *     logInspector.start();
 *     try {
 *         // do something
 *     } finally {
 *         logInspector.stop();
 *     }
 *
 *     assertThat(logInspector.isMatched(), is(true));
 * </code></pre>
 *
 * <p>
 * When it is needed to check log events/messages and perform some action, just add a predicate and an action:
 * </p>
 * <pre><code>
 *     AtomicInteger messageCounter = new AtomicInteger();
 *     LogInspector checker = new LogInspector(
 *             CustomClass.class.getName(),
 *             evt -> evt.getMessage().getFormattedMessage().matches(pattern) && evt.getLevel() == Level.ERROR,
 *             // This action will be executed when the predicate is matched.
 *             () -> {
 *                 messageCounter.incrementAndGet();
 *             });
 *
 *     logInspector.start();
 *     try {
 *         // do something
 *     } finally {
 *         logInspector.stop();
 *     }
 *
 *     assertThat(messageCounter.get(), is(42));
 * </code></pre>
 *
 * <p>
 * It is possible to add a new pair of predicate and action using {@link #addHandler(Predicate, Runnable)},
 * {@link #addHandler(Handler)} methods at any time.
 * </p>
 */
public class LogInspector {
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

    static {
        Log4jUtils.waitTillConfigured();
    }

    /**
     * Creates a new instance of {@link LogInspector} for the given {@code loggerName}.
     *
     * @return New instance of {@link LogInspector}.
     **/
    public static LogInspector create(String loggerName) {
        return create(loggerName, false);
    }

    /**
     * Creates a new instance of {@link LogInspector} for the given {@code loggerName}.
     *
     * @param started Whether the checker should be started.
     * @return New instance of {@link LogInspector}.
     **/
    public static LogInspector create(String loggerName, boolean started) {
        LogInspector checker = new LogInspector(loggerName);

        if (started) {
            checker.start();
        }

        return checker;
    }

    /**
     * Creates a new instance of {@link LogInspector} for the given {@code clazz}.
     *
     * @return New instance of {@link LogInspector}.
     **/
    public static LogInspector create(Class<?> clazz) {
        return create(clazz, false);
    }

    /**
     * Creates a new instance of {@link LogInspector} for the given {@code clazz}.
     *
     * @param started Whether the checker should be started.
     * @return New instance of {@link LogInspector}.
     **/
    public static LogInspector create(Class<?> clazz, boolean started) {
        return create(clazz.getName(), started);
    }

    /**
     * Creates a new instance of {@link LogInspector}.
     *
     * @param loggerName Logger name.
     */
    public LogInspector(String loggerName) {
        Objects.requireNonNull(loggerName);

        this.loggerName = loggerName;
        this.appender = new TestLogAppender(loggerName);
    }

    /**
     * Creates a new instance of {@link LogInspector} with the given {@code predicate}.
     * In order to check that the required log event was logged, use {@link #isMatched()} method.
     *
     * @param loggerName Logger name.
     * @param predicate Predicate to check log messages.
     */
    public LogInspector(String loggerName, Predicate<LogEvent> predicate) {
        this(loggerName, predicate, () -> {});
    }

    /**
     * Creates a new instance of {@link LogInspector} with the given {@code predicate} and {@code action}.
     *
     * @param loggerName Logger name.
     * @param predicate Predicate to check log messages.
     * @param action Action to be executed when the {@code predicate} is matched.
     */
    public LogInspector(String loggerName, Predicate<LogEvent> predicate, Runnable action) {
        this(loggerName, new Handler(predicate, event -> action.run()));
    }

    /**
     * Creates a new instance of {@link LogInspector} with the given list of {@code handlers}.
     *
     * @param loggerName Logger name.
     * @param handlers List of handlers to be added.
     */
    public LogInspector(String loggerName, Handler... handlers) {
        this(loggerName);

        lock.writeLock().lock();
        try {
            Collections.addAll(this.handlers, handlers);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Logger name.
     *
     * @return Logger name.
     */
    public String loggerName() {
        return loggerName;
    }

    /**
     * Adds a new handler with the given {@code predicate} and {@code action}.
     *
     * @param predicate Predicate to check log messages.
     * @param action Action to be executed when the {@code predicate} is matched.
     * @return New instance of {@link Handler}.
     */
    public Handler addHandler(Predicate<LogEvent> predicate, Runnable action) {
        Handler handler = new Handler(predicate, event -> action.run());

        addHandler(handler);

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
            return handlers.stream().anyMatch(Handler::isMatched);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Returns integer stream of {@link Handler#timesMatched()} for all handlers.
     *
     * @return Returns integer stream of {@link Handler#timesMatched()} for all handlers.
     */
    public IntStream timesMatched() {
        lock.readLock().lock();

        try {
            return handlers.stream().mapToInt(Handler::timesMatched);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Starts the test log checker.
     *
     * @throws IllegalStateException If the checker is already started.
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
     *
     * @throws IllegalStateException If the checker is not started.
     */
    public void stop() {
        if (config == null) {
            throw new IllegalStateException("TestLogChecker is not started.");
        }

        appender.stop();

        removeAppender(appender, config);

        config = null;
    }

    private static synchronized void addAppender(Appender appender, Configuration config) {
        for (LoggerConfig loggerConfig : config.getLoggers().values()) {
            // Only add appender to the non-additive logger to prevent event duplication.
            if (!loggerConfig.isAdditive()) {
                loggerConfig.addAppender(appender, null, null);
            }
        }
        config.getRootLogger().addAppender(appender, null, null);
    }

    private static synchronized void removeAppender(Appender appender, Configuration config) {
        for (LoggerConfig loggerConfig : config.getLoggers().values()) {
            loggerConfig.removeAppender(appender.getName());
        }
        config.getRootLogger().removeAppender(appender.getName());
    }

    /**
     * Log event handler.
     */
    public static class Handler {
        /** Predicate that is used to check log messages. */
        private final Predicate<LogEvent> predicate;

        /** Consumer to be supplied with event when the {@code predicate} is matched. */
        private final Consumer<LogEvent> consumer;

        /** Counter that indicates how many times the predicate has matched. */
        private final AtomicInteger timesMatched = new AtomicInteger();

        /**
         * Creates a new instance of {@link Handler}.
         *
         * @param predicate Predicate to check log messages.
         * @param consumer Consumer to be supplied with the event when the {@code predicate} is matched.
         */
        public Handler(Predicate<LogEvent> predicate, Consumer<LogEvent> consumer) {
            Objects.requireNonNull(predicate);
            Objects.requireNonNull(consumer);

            this.predicate = predicate;
            this.consumer = consumer;
        }

        /**
         * Checks if the predicate is matched.
         *
         * @return {@code true} if the predicate is matched, {@code false} otherwise.
         */
        public boolean isMatched() {
            return timesMatched.get() > 0;
        }

        /**
         * Indicates how many times the predicate has matched.
         *
         * @return How many times the predicate has matched.
         */
        public int timesMatched() {
            return timesMatched.get();
        }
    }

    private class TestLogAppender extends AbstractAppender {
        private TestLogAppender(String name) {
            super(name, null, null, true, Property.EMPTY_ARRAY);
        }

        @Override
        public void append(LogEvent event) {
            if (!loggerName.equals(event.getLoggerName())) {
                return;
            }

            lock.readLock().lock();
            try {
                handlers.forEach(handler -> {
                    if (handler.predicate.test(event)) {
                        handler.timesMatched.incrementAndGet();
                        handler.consumer.accept(event);
                    }
                });
            } finally {
                lock.readLock().unlock();
            }
        }
    }
}
