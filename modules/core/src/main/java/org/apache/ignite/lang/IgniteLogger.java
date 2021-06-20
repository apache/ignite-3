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

package org.apache.ignite.lang;

import java.lang.System.Logger.Level;
import java.util.Objects;
import java.util.function.Supplier;
import org.jetbrains.annotations.NotNull;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.ERROR;
import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.TRACE;
import static java.lang.System.Logger.Level.WARNING;

/**
 * Ignite logger wraps system logger for more convenient access.
 */
public class IgniteLogger {
    /**
     * Creates logger for class.
     *
     * @param cls The class for a logger.
     * @return Ignite logger.
     */
    public static IgniteLogger forClass(Class<?> cls) {
        return new IgniteLogger(cls);
    }

    /** Logger delegate. */
    private final System.Logger log;

    /**
     * @param cls The class for a logger.
     */
    protected IgniteLogger(@NotNull Class<?> cls) {
        log = System.getLogger(Objects.requireNonNull(cls).getName());
    }

    /**
     * @param msg The message pattern which will be formatted and passed to the {@link System.Logger}.
     * @param params A list of arguments to be substituted in place of formatting anchors.
     */
    public void info(String msg, Object... params) {
        logInternal(INFO, msg, params);
    }

    /**
     * Logs a message which produces in {@code msgSupplier},
     * on {@link INFO} level with associated exception {@code thrown}.
     *
     * @param msgSupplier A supplier function that produces a message.
     * @param thrown A {@code Throwable} associated with log message; can be {@code null}.
     */
    public void info(Supplier<String> msgSupplier, Throwable thrown) {
        logInternalExceptional(INFO, msgSupplier, thrown);
    }

    /**
     * @param msg The message pattern which will be passed to the {@link System.Logger}.
     * @param th A {@code Throwable} associated with the log message.
     */
    public void info(String msg, Throwable th) {
        log.log(INFO, msg, th);
    }

    /**
     * @param msg The message pattern which will be formatted and passed to the {@link System.Logger}.
     * @param params A list of arguments to be substituted in place of formatting anchors.
     */
    public void debug(String msg, Object... params) {
        logInternal(DEBUG, msg, params);
    }

    /**
     * Logs a message which produces in {@code msgSupplier},
     * on {@link DEBUG} level with associated exception {@code thrown}.
     *
     * @param msgSupplier A supplier function that produces a message.
     * @param thrown A {@code Throwable} associated with log message; can be {@code null}.
     */
    public void debug(Supplier<String> msgSupplier, Throwable thrown) {
        logInternalExceptional(DEBUG, msgSupplier, thrown);
    }

    /**
     * @param msg The message pattern which will be passed to the {@link System.Logger}.
     * @param th A {@code Throwable} associated with the log message;
     */
    public void debug(String msg, Throwable th) {
        log.log(DEBUG, msg, th);
    }

    /**
     * @param msg The message pattern which will be formatted and passed to the {@link System.Logger}.
     * @param params A list of arguments to be substituted in place of formatting anchors.
     */
    public void warn(String msg, Object... params) {
        logInternal(WARNING, msg, params);
    }

    /**
     * Logs a message which produces in {@code msgSupplier},
     * on {@link WARNING} level with associated exception {@code thrown}.
     *
     * @param msgSupplier A supplier function that produces a message.
     * @param thrown A {@code Throwable} associated with log message; can be {@code null}.
     */
    public void warn(Supplier<String> msgSupplier, Throwable thrown) {
        logInternalExceptional(WARNING, msgSupplier, thrown);
    }

    /**
     * @param msg The message pattern which will be passed to the {@link System.Logger}.
     * @param th A {@code Throwable} associated with the log message.
     */
    public void warn(String msg, Throwable th) {
        log.log(WARNING, msg, th);
    }

    /**
     * @param msg The message pattern which will be formatted and passed to the {@link System.Logger}.
     * @param params A list of arguments to be substituted in place of formatting anchors.
     */
    public void error(String msg, Object... params) {
        logInternal(ERROR, msg, params);
    }

    /**
     * Logs a message which produces in {@code msgSupplier},
     * on {@link ERROR} level with associated exception {@code thrown}.
     *
     * @param msgSupplier A supplier function that produces a message.
     * @param thrown A {@code Throwable} associated with log message; can be {@code null}.
     */
    public void error(Supplier<String> msgSupplier, Throwable thrown) {
        logInternalExceptional(ERROR, msgSupplier, thrown);
    }

    /**
     * @param msg The message pattern which will be passed to the {@link System.Logger}.
     * @param th A {@code Throwable} associated with the log message.
     */
    public void error(String msg, Throwable th) {
        log.log(ERROR, msg, th);
    }

    /**
     * @param msg The message pattern which will be formatted and passed to the {@link System.Logger}.
     * @param params A list of arguments to be substituted in place of formatting anchors.
     */
    public void trace(String msg, Object... params) {
        logInternal(TRACE, msg, params);
    }

    /**
     * Logs a message which produces in {@code msgSupplier},
     * on {@link TRACE} level with associated exception {@code thrown}.
     *
     * @param msgSupplier A supplier function that produces a message.
     * @param thrown A {@code Throwable} associated with log message; can be {@code null}.
     */
    public void trace(Supplier<String> msgSupplier, Throwable thrown) {
        logInternalExceptional(TRACE, msgSupplier, thrown);
    }

    /**
     * @param msg The message pattern which will be passed to the {@link System.Logger}.
     * @param th A {@code Throwable} associated with the log message.
     */
    public void trace(String msg, Throwable th) {
        log.log(TRACE, msg, th);
    }

    /**
     * Logs a message with an optional list of parameters.
     *
     * @param level One of the log message level identifiers.
     * @param msg The string message format in {@link LoggerMessageHelper} format.
     * @param params An optional list of parameters to the message (may be none).
     * @throws NullPointerException If {@code level} is {@code null}.
     */
    private void logInternal(Level level, String msg, Object... params) {
        Objects.requireNonNull(level);

        if (!log.isLoggable(level))
            return;

        log.log(level, LoggerMessageHelper.arrayFormat(msg, params));
    }

    /**
     * Logs a lazily supplied message associated with a given throwable.
     *
     * @param level One of the log message level identifiers.
     * @param msgSupplier A supplier function that produces a message.
     * @param thrown A {@code Throwable} associated with log message; can be {@code null}.
     * @throws NullPointerException If {@code level} is {@code null}, or {@code msgSupplier} is {@code null}.
     */
    private void logInternalExceptional(Level level, Supplier<String> msgSupplier, Throwable thrown) {
        Objects.requireNonNull(level);
        Objects.requireNonNull(msgSupplier);

        if (!log.isLoggable(level))
            return;

        log.log(level, msgSupplier.get(), thrown);
    }

    /**
     * @return {@code true} if the {@link TRACE} log message level is currently being logged.
     */
    public boolean isTraceEnabled() {
        return log.isLoggable(TRACE);
    }

    /**
     * @return {@code true} if the {@link DEBUG} log message level is currently being logged.
     */
    public boolean isDebugEnabled() {
        return log.isLoggable(DEBUG);
    }

    /**
     * @return {@code true} if the {@link INFO} log message level is currently being logged.
     */
    public boolean isInfoEnabled() {
        return log.isLoggable(INFO);
    }

    /**
     * @return {@code true} if the {@link WARNING} log message level is currently being logged.
     */
    public boolean isWarnEnabled() {
        return log.isLoggable(WARNING);
    }
}
