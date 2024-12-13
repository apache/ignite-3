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

package org.apache.ignite.internal.tostring;

import static org.apache.ignite.internal.util.ArrayUtils.OBJECT_EMPTY_ARRAY;
import static org.apache.ignite.internal.util.ArrayUtils.nullOrEmpty;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.util.FastTimestamps;
import org.jetbrains.annotations.Nullable;

/**
 * Log throttle.
 *
 * <p>Errors are logged only if they were not logged for the last {@link #THROTTLE_TIMEOUT_MILLIS} milliseconds. Note that not only error
 * messages are checked for duplicates, but also exception classes.</p>
 */
public class IgniteLogThrottle {
    /** Throttle timeout in milliseconds (value is 5 min). */
    private static final long THROTTLE_TIMEOUT_MILLIS = TimeUnit.MINUTES.toMillis(5);

    /** Log messages. */
    private static final ConcurrentMap<LogThrottleKey, Long> MESSAGES_MAP = new ConcurrentHashMap<>(128, 0.75f);

    /**
     * Logs error if needed.
     *
     * @param log Logger.
     * @param msg Message.
     */
    public static void error(IgniteLogger log, String msg) {
        error(log, msg, (Throwable) null, OBJECT_EMPTY_ARRAY);
    }

    /**
     * Logs error if needed.
     *
     * @param log Logger.
     * @param msg Message.
     * @param e Error (optional).
     */
    public static void error(IgniteLogger log, String msg, @Nullable Throwable e) {
        error(log, msg, e, OBJECT_EMPTY_ARRAY);
    }

    /**
     * Logs error if needed.
     *
     * @param log Logger.
     * @param msg Message pattern.
     * @param params List of arguments to be substituted in place of formatting anchors.
     */
    public static void error(IgniteLogger log, String msg, Object... params) {
        error(log, msg, (Throwable) null, params);
    }

    /**
     * Logs error if needed.
     *
     * @param log Logger.
     * @param msg Message pattern.
     * @param e Error (optional).
     * @param params List of arguments to be substituted in place of formatting anchors.
     */
    public static void error(IgniteLogger log, String msg, @Nullable Throwable e, Object... params) {
        log(log, e, null, msg, LogLevel.ERROR, params);
    }

    /**
     * Logs error if needed.
     *
     * @param log Logger.
     * @param throttleKey Messages with the same key will be throttled.
     * @param msg Message.
     */
    public static void error(IgniteLogger log, String throttleKey, String msg) {
        error(log, throttleKey, msg, null, OBJECT_EMPTY_ARRAY);
    }

    /**
     * Logs error if needed.
     *
     * @param log Logger.
     * @param throttleKey Messages with the same key will be throttled.
     * @param msg Message.
     * @param e Error (optional).
     */
    public static void error(IgniteLogger log, String throttleKey, String msg, @Nullable Throwable e) {
        error(log, throttleKey, msg, e, OBJECT_EMPTY_ARRAY);
    }

    /**
     * Logs error if needed.
     *
     * @param log Logger.
     * @param throttleKey Messages with the same key will be throttled.
     * @param msg Message pattern.
     * @param params List of arguments to be substituted in place of formatting anchors.
     */
    public static void error(IgniteLogger log, String throttleKey, String msg, Object... params) {
        error(log, throttleKey, msg, null, params);
    }

    /**
     * Logs error if needed.
     *
     * @param log Logger.
     * @param throttleKey Messages with the same key will be throttled.
     * @param msg Message pattern.
     * @param e Error (optional).
     * @param params List of arguments to be substituted in place of formatting anchors.
     */
    public static void error(IgniteLogger log, String throttleKey, String msg, @Nullable Throwable e, Object... params) {
        log(log, e, throttleKey, msg, LogLevel.ERROR, params);
    }

    /**
     * Logs warn if needed.
     *
     * @param log Logger.
     * @param msg Message.
     */
    public static void warn(IgniteLogger log, String msg) {
        warn(log, msg, (Throwable) null, OBJECT_EMPTY_ARRAY);
    }

    /**
     * Logs warn if needed.
     *
     * @param log Logger.
     * @param msg Message.
     * @param e Error (optional).
     */
    public static void warn(IgniteLogger log, String msg, @Nullable Throwable e) {
        warn(log, msg, e, OBJECT_EMPTY_ARRAY);
    }

    /**
     * Logs warn if needed.
     *
     * @param log Logger.
     * @param msg Message pattern.
     * @param params List of arguments to be substituted in place of formatting anchors.
     */
    public static void warn(IgniteLogger log, String msg, Object... params) {
        warn(log, msg, (Throwable) null, params);
    }

    /**
     * Logs warn if needed.
     *
     * @param log Logger.
     * @param msg Message pattern.
     * @param e Error (optional).
     * @param params List of arguments to be substituted in place of formatting anchors.
     */
    public static void warn(IgniteLogger log, String msg, @Nullable Throwable e, Object... params) {
        log(log, e, null, msg, LogLevel.WARN, params);
    }

    /**
     * Logs warn if needed.
     *
     * @param log Logger.
     * @param throttleKey Messages with the same key will be throttled.
     * @param msg Message.
     */
    public static void warn(IgniteLogger log, String throttleKey, String msg) {
        warn(log, throttleKey, msg, null, OBJECT_EMPTY_ARRAY);
    }

    /**
     * Logs warn if needed.
     *
     * @param log Logger.
     * @param throttleKey Messages with the same key will be throttled.
     * @param msg Message.
     * @param e Error (optional).
     */
    public static void warn(IgniteLogger log, String throttleKey, String msg, @Nullable Throwable e) {
        warn(log, throttleKey, msg, e, OBJECT_EMPTY_ARRAY);
    }

    /**
     * Logs warn if needed.
     *
     * @param log Logger.
     * @param throttleKey Messages with the same key will be throttled.
     * @param msg Message pattern.
     * @param params List of arguments to be substituted in place of formatting anchors.
     */
    public static void warn(IgniteLogger log, String throttleKey, String msg, Object... params) {
        warn(log, throttleKey, msg, null, params);
    }

    /**
     * Logs warn if needed.
     *
     * @param log Logger.
     * @param throttleKey Messages with the same key will be throttled.
     * @param msg Message pattern.
     * @param e Error (optional).
     * @param params List of arguments to be substituted in place of formatting anchors.
     */
    public static void warn(IgniteLogger log, String throttleKey, String msg, @Nullable Throwable e, Object... params) {
        log(log, e, throttleKey, msg, LogLevel.WARN, params);
    }

    /**
     * Logs info if needed.
     *
     * @param log Logger.
     * @param msg Message.
     */
    public static void info(IgniteLogger log, String msg) {
        info(log, msg, (Throwable) null, OBJECT_EMPTY_ARRAY);
    }

    /**
     * Logs info if needed.
     *
     * @param log Logger.
     * @param msg Message.
     * @param e Error (optional).
     */
    public static void info(IgniteLogger log, String msg, @Nullable Throwable e) {
        info(log, msg, e, OBJECT_EMPTY_ARRAY);
    }

    /**
     * Logs info if needed.
     *
     * @param log Logger.
     * @param msg Message pattern.
     * @param params List of arguments to be substituted in place of formatting anchors.
     */
    public static void info(IgniteLogger log, String msg, Object... params) {
        info(log, msg, (Throwable) null, params);
    }

    /**
     * Logs info if needed.
     *
     * @param log Logger.
     * @param e Error (optional).
     * @param msg Message pattern.
     * @param params List of arguments to be substituted in place of formatting anchors.
     */
    public static void info(IgniteLogger log, String msg, @Nullable Throwable e, Object... params) {
        log(log, e, null, msg, LogLevel.INFO, params);
    }

    /**
     * Logs info if needed.
     *
     * @param log Logger.
     * @param throttleKey Messages with the same key will be throttled.
     * @param msg Message.
     */
    public static void info(IgniteLogger log, String throttleKey, String msg) {
        info(log, throttleKey, msg, null, OBJECT_EMPTY_ARRAY);
    }

    /**
     * Logs info if needed.
     *
     * @param log Logger.
     * @param throttleKey Messages with the same key will be throttled.
     * @param e Error (optional).
     * @param msg Message.
     */
    public static void info(IgniteLogger log, String throttleKey, String msg, @Nullable Throwable e) {
        info(log, throttleKey, msg, e, OBJECT_EMPTY_ARRAY);
    }

    /**
     * Logs info if needed.
     *
     * @param log Logger.
     * @param throttleKey Messages with the same key will be throttled.
     * @param msg Message pattern.
     * @param params List of arguments to be substituted in place of formatting anchors.
     */
    public static void info(IgniteLogger log, String throttleKey, String msg, Object... params) {
        info(log, throttleKey, msg, null, params);
    }

    /**
     * Logs info if needed.
     *
     * @param log Logger.
     * @param throttleKey Messages with the same key will be throttled.
     * @param msg Message pattern.
     * @param e Error (optional).
     * @param params List of arguments to be substituted in place of formatting anchors.
     */
    public static void info(IgniteLogger log, String throttleKey, String msg, @Nullable Throwable e, Object... params) {
        log(log, e, throttleKey, msg, LogLevel.INFO, params);
    }

    /**
     * Logs debug if needed.
     *
     * @param log Logger.
     * @param msg Message.
     */
    public static void debug(IgniteLogger log, String msg) {
        debug(log, msg, (Throwable) null, OBJECT_EMPTY_ARRAY);
    }

    /**
     * Logs debug if needed.
     *
     * @param log Logger.
     * @param msg Message.
     * @param e Error (optional).
     */
    public static void debug(IgniteLogger log, String msg, @Nullable Throwable e) {
        debug(log, msg, e, OBJECT_EMPTY_ARRAY);
    }

    /**
     * Logs debug if needed.
     *
     * @param log Logger.
     * @param msg Message pattern.
     * @param params List of arguments to be substituted in place of formatting anchors.
     */
    public static void debug(IgniteLogger log, String msg, Object... params) {
        debug(log, msg, (Throwable) null, params);
    }

    /**
     * Logs debug if needed.
     *
     * @param log Logger.
     * @param e Error (optional).
     * @param msg Message pattern.
     * @param params List of arguments to be substituted in place of formatting anchors.
     */
    public static void debug(IgniteLogger log, String msg, @Nullable Throwable e, Object... params) {
        log(log, e, null, msg, LogLevel.DEBUG, params);
    }

    /**
     * Logs debug if needed.
     *
     * @param log Logger.
     * @param throttleKey Messages with the same key will be throttled.
     * @param msg Message.
     */
    public static void debug(IgniteLogger log, String throttleKey, String msg) {
        debug(log, throttleKey, msg, null, OBJECT_EMPTY_ARRAY);
    }

    /**
     * Logs debug if needed.
     *
     * @param log Logger.
     * @param throttleKey Messages with the same key will be throttled.
     * @param e Error (optional).
     * @param msg Message.
     */
    public static void debug(IgniteLogger log, String throttleKey, String msg, @Nullable Throwable e) {
        debug(log, throttleKey, msg, e, OBJECT_EMPTY_ARRAY);
    }

    /**
     * Logs debug if needed.
     *
     * @param log Logger.
     * @param throttleKey Messages with the same key will be throttled.
     * @param msg Message pattern.
     * @param params List of arguments to be substituted in place of formatting anchors.
     */
    public static void debug(IgniteLogger log, String throttleKey, String msg, Object... params) {
        debug(log, throttleKey, msg, null, params);
    }

    /**
     * Logs debug if needed.
     *
     * @param log Logger.
     * @param throttleKey Messages with the same key will be throttled.
     * @param msg Message pattern.
     * @param e Error (optional).
     * @param params List of arguments to be substituted in place of formatting anchors.
     */
    public static void debug(IgniteLogger log, String throttleKey, String msg, @Nullable Throwable e, Object... params) {
        log(log, e, throttleKey, msg, LogLevel.DEBUG, params);
    }

    /**
     * Logs message if needed using desired level.
     *
     * @param log Logger.
     * @param e Error (optional).
     * @param longMsg Long message/message pattern.
     * @param throttleKey Throttle key (optional).
     * @param level Level where messages should appear.
     * @param params List of arguments to be substituted in place of formatting anchors.
     */
    private static void log(
            IgniteLogger log,
            @Nullable Throwable e,
            @Nullable String throttleKey,
            String longMsg,
            LogLevel level,
            Object... params
    ) {
        String errorMessage = nullOrEmpty(params) ? longMsg : IgniteStringFormatter.format(longMsg, params);

        var msgKey = new LogThrottleKey(e, throttleKey == null ? errorMessage : throttleKey);

        while (true) {
            Long loggedTs = MESSAGES_MAP.get(msgKey);

            long curTs = FastTimestamps.coarseCurrentTimeMillis();

            if (loggedTs == null || loggedTs < curTs - THROTTLE_TIMEOUT_MILLIS) {
                if (replace(msgKey, loggedTs, curTs)) {
                    level.doLog(log, errorMessage, e);

                    break;
                }
            } else {
                // Ignore.
                break;
            }
        }
    }

    private static boolean replace(LogThrottleKey key, @Nullable Long oldTs, Long newTs) {
        if (oldTs == null) {
            Long old = MESSAGES_MAP.putIfAbsent(key, newTs);

            return old == null;
        }

        return MESSAGES_MAP.replace(key, oldTs, newTs);
    }

    private enum LogLevel {
        ERROR {
            @Override
            public void doLog(IgniteLogger log, String msg, @Nullable Throwable e) {
                log.error(msg, e);
            }
        },

        WARN {
            @Override
            public void doLog(IgniteLogger log, String msg, @Nullable Throwable e) {
                log.warn(msg, e);
            }
        },

        INFO {
            @Override
            public void doLog(IgniteLogger log, String msg, @Nullable Throwable e) {
                log.info(msg, e, msg);
            }
        },

        DEBUG {
            @Override
            public void doLog(IgniteLogger log, String msg, @Nullable Throwable e) {
                log.debug(msg, e);
            }
        };

        public abstract void doLog(IgniteLogger log, String msg, @Nullable Throwable e);
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
