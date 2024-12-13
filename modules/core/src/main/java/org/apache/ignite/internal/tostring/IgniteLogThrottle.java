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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
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
        error(log, (Throwable) null, msg, OBJECT_EMPTY_ARRAY);
    }

    /**
     * Logs error if needed.
     *
     * @param log Logger.
     * @param e Error (optional).
     * @param msg Message.
     */
    public static void error(IgniteLogger log, @Nullable Throwable e, String msg) {
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
        error(log, (Throwable) null, msg, params);
    }

    /**
     * Logs error if needed.
     *
     * @param log Logger.
     * @param e Error (optional).
     * @param msg Message pattern
     * @param params List of arguments to be substituted in place of formatting anchors.
     */
    public static void error(IgniteLogger log, @Nullable Throwable e, String msg, Object... params) {
        log(log, e, msg, msg, LogLevel.ERROR, false, params);
    }

    /**
     * Logs error if needed.
     *
     * @param log Logger.
     * @param throttleKey Messages with the same key will be throttled.
     * @param msg Message.
     */
    public static void error(IgniteLogger log, String throttleKey, String msg) {
        error(log, throttleKey, null, msg, OBJECT_EMPTY_ARRAY);
    }

    /**
     * Logs error if needed.
     *
     * @param log Logger.
     * @param throttleKey Messages with the same key will be throttled.
     * @param e Error (optional).
     * @param msg Message.
     */
    public static void error(IgniteLogger log, String throttleKey, @Nullable Throwable e, String msg) {
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
        error(log, throttleKey, null, msg, params);
    }

    /**
     * Logs error if needed.
     *
     * @param log Logger.
     * @param throttleKey Messages with the same key will be throttled.
     * @param e Error (optional).
     * @param msg Message pattern.
     * @param params List of arguments to be substituted in place of formatting anchors.
     */
    public static void error(IgniteLogger log, String throttleKey, @Nullable Throwable e, String msg, Object... params) {
        log(log, e, throttleKey, msg, LogLevel.ERROR, false, params);
    }

    /**
     * Logs warn if needed.
     *
     * @param log Logger.
     * @param msg Message.
     */
    public static void warn(IgniteLogger log, String msg) {
        warn(log, (Throwable) null, msg, OBJECT_EMPTY_ARRAY);
    }

    /**
     * Logs warn if needed.
     *
     * @param log Logger.
     * @param e Error (optional).
     * @param msg Message.
     */
    public static void warn(IgniteLogger log, @Nullable Throwable e, String msg) {
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
        warn(log, (Throwable) null, msg, params);
    }

    /**
     * Logs warn if needed.
     *
     * @param log Logger.
     * @param e Error (optional).
     * @param msg Message pattern
     * @param params List of arguments to be substituted in place of formatting anchors.
     */
    public static void warn(IgniteLogger log, @Nullable Throwable e, String msg, Object... params) {
        log(log, e, msg, msg, LogLevel.WARN, false, params);
    }

    /**
     * Logs warn if needed.
     *
     * @param log Logger.
     * @param throttleKey Messages with the same key will be throttled.
     * @param msg Message.
     */
    public static void warn(IgniteLogger log, String throttleKey, String msg) {
        warn(log, throttleKey, null, msg, OBJECT_EMPTY_ARRAY);
    }

    /**
     * Logs warn if needed.
     *
     * @param log Logger.
     * @param throttleKey Messages with the same key will be throttled.
     * @param e Error (optional).
     * @param msg Message.
     */
    public static void warn(IgniteLogger log, String throttleKey, @Nullable Throwable e, String msg) {
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
        warn(log, throttleKey, null, msg, params);
    }

    /**
     * Logs warn if needed.
     *
     * @param log Logger.
     * @param throttleKey Messages with the same key will be throttled.
     * @param e Error (optional).
     * @param msg Message pattern.
     * @param params List of arguments to be substituted in place of formatting anchors.
     */
    public static void warn(IgniteLogger log, String throttleKey, @Nullable Throwable e, String msg, Object... params) {
        log(log, e, throttleKey, msg, LogLevel.WARN, false, params);
    }

    /**
     * Logs info if needed.
     *
     * @param log Logger.
     * @param msg Message.
     */
    public static void info(IgniteLogger log, String msg) {
        info(log, (Throwable) null, msg, OBJECT_EMPTY_ARRAY);
    }

    /**
     * Logs info if needed.
     *
     * @param log Logger.
     * @param e Error (optional).
     * @param msg Message.
     */
    public static void info(IgniteLogger log, @Nullable Throwable e, String msg) {
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
        info(log, (Throwable) null, msg, params);
    }

    /**
     * Logs info if needed.
     *
     * @param log Logger.
     * @param e Error (optional).
     * @param msg Message pattern
     * @param params List of arguments to be substituted in place of formatting anchors.
     */
    public static void info(IgniteLogger log, @Nullable Throwable e, String msg, Object... params) {
        log(log, e, msg, msg, LogLevel.INFO, false, params);
    }

    /**
     * Logs info if needed.
     *
     * @param log Logger.
     * @param throttleKey Messages with the same key will be throttled.
     * @param msg Message.
     */
    public static void info(IgniteLogger log, String throttleKey, String msg) {
        info(log, throttleKey, null, msg, OBJECT_EMPTY_ARRAY);
    }

    /**
     * Logs info if needed.
     *
     * @param log Logger.
     * @param throttleKey Messages with the same key will be throttled.
     * @param e Error (optional).
     * @param msg Message.
     */
    public static void info(IgniteLogger log, String throttleKey, @Nullable Throwable e, String msg) {
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
        info(log, throttleKey, null, msg, params);
    }

    /**
     * Logs info if needed.
     *
     * @param log Logger.
     * @param throttleKey Messages with the same key will be throttled.
     * @param e Error (optional).
     * @param msg Message pattern.
     * @param params List of arguments to be substituted in place of formatting anchors.
     */
    public static void info(IgniteLogger log, String throttleKey, @Nullable Throwable e, String msg, Object... params) {
        log(log, e, throttleKey, msg, LogLevel.INFO, params);
    }

    /**
     * Logs message if needed using desired level.
     *
     * @param log Logger.
     * @param e Error (optional).
     * @param longMsg Long message/message pattern (or just message).
     * @param throttleKey Throttle key.
     * @param level Level where messages should appear.
     * @param params List of arguments to be substituted in place of formatting anchors.
     */
    private static void log(
            IgniteLogger log,
            @Nullable Throwable e,
            String throttleKey,
            String longMsg,
            LogLevel level,
            Object... params
    ) {
        LogThrottleKey msgKey = e != null ? new LogThrottleKey(e.getClass(), throttleKey) : new LogThrottleKey(null, throttleKey);

        while (true) {
            Long loggedTs = MESSAGES_MAP.get(msgKey);

            long curTs = FastTimestamps.coarseCurrentTimeMillis();

            if (loggedTs == null || loggedTs < curTs - THROTTLE_TIMEOUT_MILLIS) {
                if (replace(msgKey, loggedTs, curTs)) {
                    level.doLog(log, longMsg, e, params);

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
            public void doLog(IgniteLogger log, String msg, @Nullable Throwable e, Object... params) {
                if (nullOrEmpty(params)) {
                    log.error(msg, e, msg, params);
                } else {
                    log.error(msg, e);
                }
            }
        },

        WARN {
            @Override
            public void doLog(IgniteLogger log, String msg, @Nullable Throwable e, Object... params) {
                if (nullOrEmpty(params)) {
                    log.warn(msg, e, msg, params);
                } else {
                    log.warn(msg, e);
                }
            }
        },

        INFO {
            @Override
            public void doLog(IgniteLogger log, String msg, @Nullable Throwable e, Object... params) {
                if (nullOrEmpty(params)) {
                    log.info(msg, e, msg, params);
                } else {
                    log.info(msg, e);
                }
            }
        };

        public abstract void doLog(IgniteLogger log, String msg, @Nullable Throwable e, Object... params);
    }

    private static class LogThrottleKey {
        @Nullable
        final Class<? extends Throwable> errorClass;

        final String errorMessage;

        LogThrottleKey(@Nullable Class<? extends Throwable> errorClass, String errorMessage) {
            this.errorClass = errorClass;
            this.errorMessage = errorMessage;
        }
    }
}
