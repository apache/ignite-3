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
import java.util.function.Supplier;
import org.jetbrains.annotations.Nullable;

/**
 * Ignite logger wraps system logger for more convenient access.
 */
public interface IgniteLogger {
    /**
     * Logs a message on {@link Level#INFO} level composed from args with given format.
     *
     * @param msg The message pattern which will be formatted and passed to the {@link System.Logger}.
     * @param params The list of arguments to be substituted in place of formatting anchors.
     */
    void info(String msg, Object... params);

    /**
     * Logs a message on {@link Level#INFO} level composed from args with given format and with associated throwable {@code th}.
     *
     * @param msg The message pattern which will be formatted and passed to the {@link System.Logger}.
     * @param th The {@code Throwable} associated with log message; can be {@code null}.
     * @param params The list of arguments to be substituted in place of formatting anchors.
     */
    void info(String msg, @Nullable Throwable th, Object... params);

    /**
     * Logs a message which produces in {@code msgSupplier}, on {@link Level#INFO} level with associated throwable {@code th}.
     *
     * @param msgSupplier A supplier function that produces a message.
     * @param th The {@code Throwable} associated with log message; can be {@code null}.
     */
    void info(Supplier<String> msgSupplier, @Nullable Throwable th);

    /**
     * Logs a message on {@link Level#INFO} level with associated throwable {@code th}.
     *
     * @param msg The message pattern which will be passed to the {@link System.Logger}.
     * @param th The {@code Throwable} associated with the log message.
     */
    void info(String msg, @Nullable Throwable th);

    /**
     * Logs a message on {@link Level#DEBUG} level composed from args with given format.
     *
     * @param msg The message pattern which will be formatted and passed to the {@link System.Logger}.
     * @param params The list of arguments to be substituted in place of formatting anchors.
     */
    void debug(String msg, Object... params);

    /**
     * Logs a message on {@link Level#DEBUG} level composed from args with given format and with associated throwable {@code th}.
     *
     * @param msg The message pattern which will be formatted and passed to the {@link System.Logger}.
     * @param th The {@code Throwable} associated with log message; can be {@code null}.
     * @param params The list of arguments to be substituted in place of formatting anchors.
     */
    void debug(String msg, @Nullable Throwable th, Object... params);

    /**
     * Logs a message which produces in {@code msgSupplier}, on {@link Level#DEBUG} level with associated throwable {@code th}.
     *
     * @param msgSupplier A supplier function that produces a message.
     * @param th The {@code Throwable} associated with log message; can be {@code null}.
     */
    void debug(Supplier<String> msgSupplier, @Nullable Throwable th);

    /**
     * Logs a message on {@link Level#DEBUG} level with associated throwable {@code th}.
     *
     * @param msg The message pattern which will be passed to the {@link System.Logger}.
     * @param th The {@code Throwable} associated with the log message;
     */
    void debug(String msg, @Nullable Throwable th);

    /**
     * Logs a message on {@link Level#WARNING} level composed from args with given format.
     *
     * @param msg The message pattern which will be formatted and passed to the {@link System.Logger}.
     * @param params The list of arguments to be substituted in place of formatting anchors.
     */
    void warn(String msg, Object... params);

    /**
     * Logs a message on {@link Level#WARNING} level composed from args with given format and with associated throwable {@code th}.
     *
     * @param msg The message pattern which will be formatted and passed to the {@link System.Logger}.
     * @param th The {@code Throwable} associated with log message; can be {@code null}.
     * @param params The list of arguments to be substituted in place of formatting anchors.
     */
    void warn(String msg, @Nullable Throwable th, Object... params);

    /**
     * Logs a message which produces in {@code msgSupplier}, on {@link Level#WARNING} level with associated throwable {@code th}.
     *
     * @param msgSupplier A supplier function that produces a message.
     * @param th The {@code Throwable} associated with log message; can be {@code null}.
     */
    void warn(Supplier<String> msgSupplier, @Nullable Throwable th);

    /**
     * Logs a message on {@link Level#WARNING} level with associated throwable {@code th}.
     *
     * @param msg The message pattern which will be passed to the {@link System.Logger}.
     * @param th The {@code Throwable} associated with the log message.
     */
    void warn(String msg, @Nullable Throwable th);

    /**
     * Logs a message on {@link Level#ERROR} level composed from args with given format.
     *
     * @param msg The message pattern which will be formatted and passed to the {@link System.Logger}.
     * @param params The list of arguments to be substituted in place of formatting anchors.
     */
    void error(String msg, Object... params);

    /**
     * Logs a message on {@link Level#ERROR} level composed from args with given format and with associated throwable {@code th}.
     *
     * @param msg The message pattern which will be formatted and passed to the {@link System.Logger}.
     * @param th The {@code Throwable} associated with log message; can be {@code null}.
     * @param params The list of arguments to be substituted in place of formatting anchors.
     */
    void error(String msg, @Nullable Throwable th, Object... params);

    /**
     * Logs a message which produces in {@code msgSupplier}, on {@link Level#ERROR} level with associated throwable {@code th}.
     *
     * @param msgSupplier A supplier function that produces a message.
     * @param th The {@code Throwable} associated with log message; can be {@code null}.
     */
    void error(Supplier<String> msgSupplier, @Nullable Throwable th);

    /**
     * Logs a message on {@link Level#ERROR} level with associated throwable {@code th}.
     *
     * @param msg The message pattern which will be passed to the {@link System.Logger}.
     * @param th The {@code Throwable} associated with the log message.
     */
    void error(String msg, @Nullable Throwable th);

    /**
     * Logs a message on {@link Level#TRACE} level composed from args with given format.
     *
     * @param msg The message pattern which will be formatted and passed to the {@link System.Logger}.
     * @param params The list of arguments to be substituted in place of formatting anchors.
     */
    void trace(String msg, Object... params);

    /**
     * Logs a message on {@link Level#TRACE} level composed from args with given format and with associated throwable {@code th}.
     *
     * @param msg The message pattern which will be formatted and passed to the {@link System.Logger}.
     * @param th The {@code Throwable} associated with log message; can be {@code null}.
     * @param params The list of arguments to be substituted in place of formatting anchors.
     */
    void trace(String msg, @Nullable Throwable th, Object... params);

    /**
     * Logs a message which produces in {@code msgSupplier}, on {@link Level#TRACE} level with associated throwable {@code th}.
     *
     * @param msgSupplier A supplier function that produces a message.
     * @param th The {@code Throwable} associated with log message; can be {@code null}.
     */
    void trace(Supplier<String> msgSupplier, @Nullable Throwable th);

    /**
     * Logs a message on {@link Level#TRACE} level with associated throwable {@code th}.
     *
     * @param msg The message pattern which will be passed to the {@link System.Logger}.
     * @param th A {@code Throwable} associated with the log message.
     */
    void trace(String msg, @Nullable Throwable th);

    /**
     * Checks if a message of the {@link Level#TRACE} level would be logged by this logger.
     *
     * @return {@code true} if the message level is currently being logged, {@code false} otherwise.
     */
    boolean isTraceEnabled();

    /**
     * Checks if a message of the {@link Level#DEBUG} level would be logged by this logger.
     *
     * @return {@code true} if the message level is currently being logged, {@code false} otherwise.
     */
    boolean isDebugEnabled();

    /**
     * Checks if a message of the {@link Level#INFO} level would be logged by this logger.
     *
     * @return {@code true} if the message level is currently being logged, {@code false} otherwise.
     */
    boolean isInfoEnabled();

    /**
     * Checks if a message of the {@link Level#WARNING} level would be logged by this logger.
     *
     * @return {@code true} if the message level is currently being logged, {@code false} otherwise.
     */
    boolean isWarnEnabled();
}
