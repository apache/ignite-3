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

import java.lang.System.Logger.Level;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.jetbrains.annotations.Nullable;

/**
 * {@link IgniteLogger} throttle.
 *
 * <p>Messages are logged only if they were not logged recently. The interval of message appears is
 * {@link this#DEFAULT_LOG_THROTTLE_INTERVAL_MS} by default or can be configured through the JVM property
 * {@link this#LOG_THROTTLE_INTERVAL_MS}. Note that not only error messages are checked for duplicates, but also exception classes if
 * present.</p>
 */
public interface IgniteThrottledLogger extends IgniteLogger {
    /** JVM property to configure a throttle interval. */
    String LOG_THROTTLE_INTERVAL_MS = "IGNITE_LOG_THROTTLE_INTERVAL_MS";

    /** Default log throttle interval in milliseconds (value is 5 min). */
    long DEFAULT_LOG_THROTTLE_INTERVAL_MS = TimeUnit.MINUTES.toMillis(5);

    /**
     * Logs a message on {@link Level#INFO} level composed from args with given format.
     *
     * @param throttleKey Messages with the same key will be throttled.
     * @param msg The message pattern which will be formatted and passed to the {@link System.Logger}.
     * @param params The list of arguments to be substituted in place of formatting anchors.
     */
    void info(String throttleKey, String msg, Object... params);

    /**
     * Logs a message on {@link Level#INFO} level composed from args with given format and with associated throwable {@code th}.
     *
     * @param throttleKey Messages with the same key will be throttled.
     * @param msg The message pattern which will be formatted and passed to the {@link System.Logger}.
     * @param th The {@code Throwable} associated with log message; can be {@code null}.
     * @param params The list of arguments to be substituted in place of formatting anchors.
     */
    void info(String throttleKey, String msg, @Nullable Throwable th, Object... params);

    /**
     * Logs a message which produces in {@code msgSupplier}, on {@link Level#INFO} level with associated throwable {@code th}.
     *
     * @param throttleKey Messages with the same key will be throttled.
     * @param msgSupplier A supplier function that produces a message.
     * @param th The {@code Throwable} associated with log message; can be {@code null}.
     */
    void info(String throttleKey, Supplier<String> msgSupplier, @Nullable Throwable th);

    /**
     * Logs a message on {@link Level#INFO} level with associated throwable {@code th}.
     *
     * @param throttleKey Messages with the same key will be throttled.
     * @param msg The message pattern which will be passed to the {@link System.Logger}.
     * @param th The {@code Throwable} associated with the log message.
     */
    void info(String throttleKey, String msg, @Nullable Throwable th);

    /**
     * Logs a message on {@link Level#DEBUG} level composed from args with given format.
     *
     * @param throttleKey Messages with the same key will be throttled.
     * @param msg The message pattern which will be formatted and passed to the {@link System.Logger}.
     * @param params The list of arguments to be substituted in place of formatting anchors.
     */
    void debug(String throttleKey, String msg, Object... params);

    /**
     * Logs a message on {@link Level#DEBUG} level composed from args with given format and with associated throwable {@code th}.
     *
     * @param throttleKey Messages with the same key will be throttled.
     * @param msg The message pattern which will be formatted and passed to the {@link System.Logger}.
     * @param th The {@code Throwable} associated with log message; can be {@code null}.
     * @param params The list of arguments to be substituted in place of formatting anchors.
     */
    void debug(String throttleKey, String msg, @Nullable Throwable th, Object... params);

    /**
     * Logs a message which produces in {@code msgSupplier}, on {@link Level#DEBUG} level with associated throwable {@code th}.
     *
     * @param throttleKey Messages with the same key will be throttled.
     * @param msgSupplier A supplier function that produces a message.
     * @param th The {@code Throwable} associated with log message; can be {@code null}.
     */
    void debug(String throttleKey, Supplier<String> msgSupplier, @Nullable Throwable th);

    /**
     * Logs a message on {@link Level#DEBUG} level with associated throwable {@code th}.
     *
     * @param throttleKey Messages with the same key will be throttled.
     * @param msg The message pattern which will be passed to the {@link System.Logger}.
     * @param th The {@code Throwable} associated with the log message;
     */
    void debug(String throttleKey, String msg, @Nullable Throwable th);

    /**
     * Logs a message on {@link Level#WARNING} level composed from args with given format.
     *
     * @param throttleKey Messages with the same key will be throttled.
     * @param msg The message pattern which will be formatted and passed to the {@link System.Logger}.
     * @param params The list of arguments to be substituted in place of formatting anchors.
     */
    void warn(String throttleKey, String msg, Object... params);

    /**
     * Logs a message on {@link Level#WARNING} level composed from args with given format and with associated throwable {@code th}.
     *
     * @param throttleKey Messages with the same key will be throttled.
     * @param msg The message pattern which will be formatted and passed to the {@link System.Logger}.
     * @param th The {@code Throwable} associated with log message; can be {@code null}.
     * @param params The list of arguments to be substituted in place of formatting anchors.
     */
    void warn(String throttleKey, String msg, @Nullable Throwable th, Object... params);

    /**
     * Logs a message which produces in {@code msgSupplier}, on {@link Level#WARNING} level with associated throwable {@code th}.
     *
     * @param throttleKey Messages with the same key will be throttled.
     * @param msgSupplier A supplier function that produces a message.
     * @param th The {@code Throwable} associated with log message; can be {@code null}.
     */
    void warn(String throttleKey, Supplier<String> msgSupplier, @Nullable Throwable th);

    /**
     * Logs a message on {@link Level#WARNING} level with associated throwable {@code th}.
     *
     * @param throttleKey Messages with the same key will be throttled.
     * @param msg The message pattern which will be passed to the {@link System.Logger}.
     * @param th The {@code Throwable} associated with the log message.
     */
    void warn(String throttleKey, String msg, @Nullable Throwable th);

    /**
     * Logs a message on {@link Level#ERROR} level composed from args with given format.
     *
     * @param throttleKey Messages with the same key will be throttled.
     * @param msg The message pattern which will be formatted and passed to the {@link System.Logger}.
     * @param params The list of arguments to be substituted in place of formatting anchors.
     */
    void error(String throttleKey, String msg, Object... params);

    /**
     * Logs a message on {@link Level#ERROR} level composed from args with given format and with associated throwable {@code th}.
     *
     * @param throttleKey Messages with the same key will be throttled.
     * @param msg The message pattern which will be formatted and passed to the {@link System.Logger}.
     * @param th The {@code Throwable} associated with log message; can be {@code null}.
     * @param params The list of arguments to be substituted in place of formatting anchors.
     */
    void error(String throttleKey, String msg, @Nullable Throwable th, Object... params);

    /**
     * Logs a message which produces in {@code msgSupplier}, on {@link Level#ERROR} level with associated throwable {@code th}.
     *
     * @param throttleKey Messages with the same key will be throttled.
     * @param msgSupplier A supplier function that produces a message.
     * @param th The {@code Throwable} associated with log message; can be {@code null}.
     */
    void error(String throttleKey, Supplier<String> msgSupplier, @Nullable Throwable th);

    /**
     * Logs a message on {@link Level#ERROR} level with associated throwable {@code th}.
     *
     * @param throttleKey Messages with the same key will be throttled.
     * @param msg The message pattern which will be passed to the {@link System.Logger}.
     * @param th The {@code Throwable} associated with the log message.
     */
    void error(String throttleKey, String msg, @Nullable Throwable th);

    /**
     * Logs a message on {@link Level#TRACE} level composed from args with given format.
     *
     * @param throttleKey Messages with the same key will be throttled.
     * @param msg The message pattern which will be formatted and passed to the {@link System.Logger}.
     * @param params The list of arguments to be substituted in place of formatting anchors.
     */
    void trace(String throttleKey, String msg, Object... params);

    /**
     * Logs a message on {@link Level#TRACE} level composed from args with given format and with associated throwable {@code th}.
     *
     * @param throttleKey Messages with the same key will be throttled.
     * @param msg The message pattern which will be formatted and passed to the {@link System.Logger}.
     * @param th The {@code Throwable} associated with log message; can be {@code null}.
     * @param params The list of arguments to be substituted in place of formatting anchors.
     */
    void trace(String throttleKey, String msg, @Nullable Throwable th, Object... params);

    /**
     * Logs a message which produces in {@code msgSupplier}, on {@link Level#TRACE} level with associated throwable {@code th}.
     *
     * @param throttleKey Messages with the same key will be throttled.
     * @param msgSupplier A supplier function that produces a message.
     * @param th The {@code Throwable} associated with log message; can be {@code null}.
     */
    void trace(String throttleKey, Supplier<String> msgSupplier, @Nullable Throwable th);

    /**
     * Logs a message on {@link Level#TRACE} level with associated throwable {@code th}.
     *
     * @param throttleKey Messages with the same key will be throttled.
     * @param msg The message pattern which will be passed to the {@link System.Logger}.
     * @param th A {@code Throwable} associated with the log message.
     */
    void trace(String throttleKey, String msg, @Nullable Throwable th);
}
