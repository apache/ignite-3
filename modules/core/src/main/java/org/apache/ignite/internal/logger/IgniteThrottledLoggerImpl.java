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

package org.apache.ignite.internal.logger;

import com.github.benmanes.caffeine.cache.Caffeine;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.lang.IgniteSystemProperties;
import org.apache.ignite.internal.util.FastTimestamps;
import org.jetbrains.annotations.Nullable;

class IgniteThrottledLoggerImpl implements IgniteThrottledLogger {
    /** Throttle interval in milliseconds (value is 5 min). */
    private final long throttleIntervalMs = IgniteSystemProperties.getLong(LOG_THROTTLE_INTERVAL_MS, DEFAULT_LOG_THROTTLE_INTERVAL_MS);

    /** Logger delegate. */
    private final System.Logger delegate;

    /** Log messages. */
    private final Map<LogThrottleKey, Long> messagesMap;

    IgniteThrottledLoggerImpl(Logger delegate) {
        this(delegate, Runnable::run);
    }

    IgniteThrottledLoggerImpl(Logger delegate, Executor executor) {
        this.delegate = delegate;

        messagesMap = Caffeine.newBuilder()
                .executor(executor)
                .expireAfterWrite(throttleIntervalMs, TimeUnit.MILLISECONDS)
                .<LogThrottleKey, Long>build()
                .asMap();
    }

    @Override
    public void info(String msg, Object... params) {
        logInternal(null, () -> IgniteStringFormatter.format(msg, params), null, Level.INFO);
    }

    @Override
    public void info(String msg, @Nullable Throwable th, Object... params) {
        logInternal(null, () -> IgniteStringFormatter.format(msg, params), th, Level.INFO);
    }

    @Override
    public void info(Supplier<String> msgSupplier, @Nullable Throwable th) {
        logInternal(null, msgSupplier, th, Level.INFO);
    }

    @Override
    public void info(String msg, @Nullable Throwable th) {
        logInternal(null, () -> msg, th, Level.INFO);
    }

    @Override
    public void info(String throttleKey, String msg, Object... params) {
        logInternal(throttleKey, () -> IgniteStringFormatter.format(msg, params), null, Level.INFO);
    }

    @Override
    public void info(String throttleKey, String msg, @Nullable Throwable th, Object... params) {
        logInternal(throttleKey, () -> IgniteStringFormatter.format(msg, params), th, Level.INFO);
    }

    @Override
    public void info(String throttleKey, Supplier<String> msgSupplier, @Nullable Throwable th) {
        logInternal(throttleKey, msgSupplier, th, Level.INFO);
    }

    @Override
    public void info(String throttleKey, String msg, @Nullable Throwable th) {
        logInternal(throttleKey, () -> msg, th, Level.INFO);
    }

    @Override
    public void debug(String msg, Object... params) {
        logInternal(null, () -> IgniteStringFormatter.format(msg, params), null, Level.DEBUG);
    }

    @Override
    public void debug(String msg, @Nullable Throwable th, Object... params) {
        logInternal(null, () -> IgniteStringFormatter.format(msg, params), th, Level.DEBUG);
    }

    @Override
    public void debug(Supplier<String> msgSupplier, @Nullable Throwable th) {
        logInternal(null, msgSupplier, th, Level.DEBUG);
    }

    @Override
    public void debug(String msg, @Nullable Throwable th) {
        logInternal(null, () -> msg, th, Level.DEBUG);
    }

    @Override
    public void debug(String throttleKey, String msg, Object... params) {
        logInternal(throttleKey, () -> IgniteStringFormatter.format(msg, params), null, Level.DEBUG);
    }

    @Override
    public void debug(String throttleKey, String msg, @Nullable Throwable th, Object... params) {
        logInternal(throttleKey, () -> IgniteStringFormatter.format(msg, params), th, Level.DEBUG);
    }

    @Override
    public void debug(String throttleKey, Supplier<String> msgSupplier, @Nullable Throwable th) {
        logInternal(throttleKey, msgSupplier, th, Level.DEBUG);
    }

    @Override
    public void debug(String throttleKey, String msg, @Nullable Throwable th) {
        logInternal(throttleKey, () -> msg, th, Level.DEBUG);
    }

    @Override
    public void warn(String msg, Object... params) {
        logInternal(null, () -> IgniteStringFormatter.format(msg, params), null, Level.WARNING);
    }

    @Override
    public void warn(String msg, @Nullable Throwable th, Object... params) {
        logInternal(null, () -> IgniteStringFormatter.format(msg, params), th, Level.WARNING);
    }

    @Override
    public void warn(Supplier<String> msgSupplier, @Nullable Throwable th) {
        logInternal(null, msgSupplier, th, Level.WARNING);
    }

    @Override
    public void warn(String msg, @Nullable Throwable th) {
        logInternal(null, () -> msg, th, Level.WARNING);
    }

    @Override
    public void warn(String throttleKey, String msg, Object... params) {
        logInternal(throttleKey, () -> IgniteStringFormatter.format(msg, params), null, Level.WARNING);
    }

    @Override
    public void warn(String throttleKey, String msg, @Nullable Throwable th, Object... params) {
        logInternal(throttleKey, () -> IgniteStringFormatter.format(msg, params), th, Level.WARNING);
    }

    @Override
    public void warn(String throttleKey, Supplier<String> msgSupplier, @Nullable Throwable th) {
        logInternal(throttleKey, msgSupplier, th, Level.WARNING);
    }

    @Override
    public void warn(String throttleKey, String msg, @Nullable Throwable th) {
        logInternal(throttleKey, () -> msg, th, Level.WARNING);
    }

    @Override
    public void error(String msg, Object... params) {
        logInternal(null, () -> IgniteStringFormatter.format(msg, params), null, Level.ERROR);
    }

    @Override
    public void error(String msg, @Nullable Throwable th, Object... params) {
        logInternal(null, () -> IgniteStringFormatter.format(msg, params), th, Level.ERROR);
    }

    @Override
    public void error(Supplier<String> msgSupplier, @Nullable Throwable th) {
        logInternal(null, msgSupplier, th, Level.ERROR);
    }

    @Override
    public void error(String msg, @Nullable Throwable th) {
        logInternal(null, () -> msg, th, Level.ERROR);
    }

    @Override
    public void error(String throttleKey, String msg, Object... params) {
        logInternal(throttleKey, () -> IgniteStringFormatter.format(msg, params), null, Level.ERROR);
    }

    @Override
    public void error(String throttleKey, String msg, @Nullable Throwable th, Object... params) {
        logInternal(throttleKey, () -> IgniteStringFormatter.format(msg, params), th, Level.ERROR);
    }

    @Override
    public void error(String throttleKey, Supplier<String> msgSupplier, @Nullable Throwable th) {
        logInternal(throttleKey, msgSupplier, th, Level.ERROR);
    }

    @Override
    public void error(String throttleKey, String msg, @Nullable Throwable th) {
        logInternal(throttleKey, () -> msg, th, Level.ERROR);
    }

    @Override
    public void trace(String msg, Object... params) {
        logInternal(null, () -> IgniteStringFormatter.format(msg, params), null, Level.TRACE);
    }

    @Override
    public void trace(String msg, @Nullable Throwable th, Object... params) {
        logInternal(null, () -> IgniteStringFormatter.format(msg, params), th, Level.TRACE);
    }

    @Override
    public void trace(Supplier<String> msgSupplier, @Nullable Throwable th) {
        logInternal(null, msgSupplier, th, Level.TRACE);
    }

    @Override
    public void trace(String msg, @Nullable Throwable th) {
        logInternal(null, () -> msg, th, Level.TRACE);
    }

    @Override
    public void trace(String throttleKey, String msg, Object... params) {
        logInternal(throttleKey, () -> IgniteStringFormatter.format(msg, params), null, Level.TRACE);
    }

    @Override
    public void trace(String throttleKey, String msg, @Nullable Throwable th, Object... params) {
        logInternal(throttleKey, () -> IgniteStringFormatter.format(msg, params), th, Level.TRACE);
    }

    @Override
    public void trace(String throttleKey, Supplier<String> msgSupplier, @Nullable Throwable th) {
        logInternal(throttleKey, msgSupplier, th, Level.TRACE);
    }

    @Override
    public void trace(String throttleKey, String msg, @Nullable Throwable th) {
        logInternal(throttleKey, () -> msg, th, Level.TRACE);
    }

    @Override
    public boolean isTraceEnabled() {
        return delegate.isLoggable(Level.TRACE);
    }

    @Override
    public boolean isDebugEnabled() {
        return delegate.isLoggable(Level.DEBUG);
    }

    @Override
    public boolean isInfoEnabled() {
        return delegate.isLoggable(Level.INFO);
    }

    @Override
    public boolean isWarnEnabled() {
        return delegate.isLoggable(Level.WARNING);
    }

    private void logInternal(
            @Nullable String throttleKey,
            Supplier<String> messageSupplier,
            @Nullable Throwable throwable,
            Level level
    ) {
        if (!delegate.isLoggable(level)) {
            return;
        }

        String message = messageSupplier.get();

        var msgKey = new LogThrottleKey(throwable, throttleKey == null ? message : throttleKey);

        while (true) {
            Long loggedTs = messagesMap.get(msgKey);

            long curTs = FastTimestamps.coarseCurrentTimeMillis();

            if (loggedTs == null || curTs - loggedTs >= throttleIntervalMs) {
                if (replace(msgKey, loggedTs, curTs)) {
                    if (throwable == null) {
                        delegate.log(level, message);
                    } else {
                        delegate.log(level, message, throwable);
                    }

                    break;
                }
            } else {
                // Ignore.
                break;
            }
        }
    }

    private boolean replace(LogThrottleKey key, @Nullable Long oldTs, Long newTs) {
        if (oldTs == null) {
            Long old = messagesMap.putIfAbsent(key, newTs);

            return old == null;
        }

        return messagesMap.replace(key, oldTs, newTs);
    }

    private static class LogThrottleKey {
        @Nullable
        final Class<? extends Throwable> errorClass;

        final String errorMessage;

        LogThrottleKey(@Nullable Throwable error, String errorMessage) {
            this.errorClass = error == null ? null : error.getClass();
            this.errorMessage = errorMessage;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            LogThrottleKey that = (LogThrottleKey) o;
            return Objects.equals(errorClass, that.errorClass) && errorMessage.equals(that.errorMessage);
        }

        @Override
        public int hashCode() {
            int result = errorClass != null ? errorClass.hashCode() : 0;
            result = 31 * result + errorMessage.hashCode();
            return result;
        }
    }
}
