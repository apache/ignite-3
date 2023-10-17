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
import java.util.Objects;
import java.util.function.Supplier;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.jetbrains.annotations.Nullable;

/**
 * Ignite logger wraps system logger for more convenient access.
 */
public class IgniteLogger {
    /** Logger delegate. */
    private final System.Logger delegate;

    /**
     * Creates logger facade for a given delegate.
     *
     * @param delegate The delegate to create facade for.
     */
    IgniteLogger(System.Logger delegate) {
        this.delegate = delegate;
    }

    /**
     * Logs a message on {@link Level#INFO} level composed from args with given format.
     *
     * @param msg    The message pattern which will be formatted and passed to the {@link System.Logger}.
     * @param params The list of arguments to be substituted in place of formatting anchors.
     */
    public void info(String msg, Object... params) {
        logInternal(Level.INFO, msg, null, params);
    }

    /**
     * Logs a message on {@link Level#INFO} level composed from args with given format and with associated throwable {@code th}.
     *
     * @param msg    The message pattern which will be formatted and passed to the {@link System.Logger}.
     * @param th     The {@code Throwable} associated with log message; can be {@code null}.
     * @param params The list of arguments to be substituted in place of formatting anchors.
     */
    public void info(String msg, @Nullable Throwable th, Object... params) {
        logInternal(Level.INFO, msg, th, params);
    }

    /**
     * Logs a message which produces in {@code msgSupplier}, on {@link Level#INFO} level with associated throwable {@code th}.
     *
     * @param msgSupplier A supplier function that produces a message.
     * @param th          The {@code Throwable} associated with log message; can be {@code null}.
     */
    public void info(Supplier<String> msgSupplier, @Nullable Throwable th) {
        logInternalExceptional(Level.INFO, msgSupplier, th);
    }

    /**
     * Logs a message on {@link Level#INFO} level with associated throwable {@code th}.
     *
     * @param msg The message pattern which will be passed to the {@link System.Logger}.
     * @param th  The {@code Throwable} associated with the log message.
     */
    public void info(String msg, @Nullable Throwable th) {
        delegate.log(Level.INFO, msg, th);
    }

    /**
     * Logs a message on {@link Level#DEBUG} level composed from args with given format.
     *
     * @param msg    The message pattern which will be formatted and passed to the {@link System.Logger}.
     * @param params The list of arguments to be substituted in place of formatting anchors.
     */
    public void debug(String msg, Object... params) {
        logInternal(Level.DEBUG, msg, null, params);
    }

    /**
     * Logs a message on {@link Level#DEBUG} level composed from args with given format and with associated throwable {@code th}.
     *
     * @param msg    The message pattern which will be formatted and passed to the {@link System.Logger}.
     * @param th     The {@code Throwable} associated with log message; can be {@code null}.
     * @param params The list of arguments to be substituted in place of formatting anchors.
     */
    public void debug(String msg, @Nullable Throwable th, Object... params) {
        logInternal(Level.DEBUG, msg, th, params);
    }

    /**
     * Logs a message which produces in {@code msgSupplier}, on {@link Level#DEBUG} level with associated throwable {@code th}.
     *
     * @param msgSupplier A supplier function that produces a message.
     * @param th          The {@code Throwable} associated with log message; can be {@code null}.
     */
    public void debug(Supplier<String> msgSupplier, @Nullable Throwable th) {
        logInternalExceptional(Level.DEBUG, msgSupplier, th);
    }

    /**
     * Logs a message on {@link Level#DEBUG} level with associated throwable {@code th}.
     *
     * @param msg The message pattern which will be passed to the {@link System.Logger}.
     * @param th  The {@code Throwable} associated with the log message;
     */
    public void debug(String msg, @Nullable Throwable th) {
        delegate.log(Level.DEBUG, msg, th);
    }

    /**
     * Logs a message on {@link Level#WARNING} level composed from args with given format.
     *
     * @param msg    The message pattern which will be formatted and passed to the {@link System.Logger}.
     * @param params The list of arguments to be substituted in place of formatting anchors.
     */
    public void warn(String msg, Object... params) {
        logInternal(Level.WARNING, msg, null, params);
    }

    /**
     * Logs a message on {@link Level#WARNING} level composed from args with given format and with associated throwable {@code th}.
     *
     * @param msg    The message pattern which will be formatted and passed to the {@link System.Logger}.
     * @param th     The {@code Throwable} associated with log message; can be {@code null}.
     * @param params The list of arguments to be substituted in place of formatting anchors.
     */
    public void warn(String msg, @Nullable Throwable th, Object... params) {
        logInternal(Level.WARNING, msg, th, params);
    }

    /**
     * Logs a message which produces in {@code msgSupplier}, on {@link Level#WARNING} level with associated throwable {@code th}.
     *
     * @param msgSupplier A supplier function that produces a message.
     * @param th          The {@code Throwable} associated with log message; can be {@code null}.
     */
    public void warn(Supplier<String> msgSupplier, @Nullable Throwable th) {
        logInternalExceptional(Level.WARNING, msgSupplier, th);
    }

    /**
     * Logs a message on {@link Level#WARNING} level with associated throwable {@code th}.
     *
     * @param msg The message pattern which will be passed to the {@link System.Logger}.
     * @param th  The {@code Throwable} associated with the log message.
     */
    public void warn(String msg, @Nullable Throwable th) {
        delegate.log(Level.WARNING, msg, th);
    }

    /**
     * Logs a message on {@link Level#ERROR} level composed from args with given format.
     *
     * @param msg    The message pattern which will be formatted and passed to the {@link System.Logger}.
     * @param params The list of arguments to be substituted in place of formatting anchors.
     */
    public void error(String msg, Object... params) {
        logInternal(Level.ERROR, msg, null, params);
    }

    /**
     * Logs a message on {@link Level#ERROR} level composed from args with given format and with associated throwable {@code th}.
     *
     * @param msg    The message pattern which will be formatted and passed to the {@link System.Logger}.
     * @param th     The {@code Throwable} associated with log message; can be {@code null}.
     * @param params The list of arguments to be substituted in place of formatting anchors.
     */
    public void error(String msg, @Nullable Throwable th, Object... params) {
        logInternal(Level.ERROR, msg, th, params);
    }

    /**
     * Logs a message which produces in {@code msgSupplier}, on {@link Level#ERROR} level with associated throwable {@code th}.
     *
     * @param msgSupplier A supplier function that produces a message.
     * @param th          The {@code Throwable} associated with log message; can be {@code null}.
     */
    public void error(Supplier<String> msgSupplier, @Nullable Throwable th) {
        logInternalExceptional(Level.ERROR, msgSupplier, th);
    }

    /**
     * Logs a message on {@link Level#ERROR} level with associated throwable {@code th}.
     *
     * @param msg The message pattern which will be passed to the {@link System.Logger}.
     * @param th  The {@code Throwable} associated with the log message.
     */
    public void error(String msg, @Nullable Throwable th) {
        delegate.log(Level.ERROR, msg, th);
    }

    /**
     * Logs a message on {@link Level#TRACE} level composed from args with given format.
     *
     * @param msg    The message pattern which will be formatted and passed to the {@link System.Logger}.
     * @param params The list of arguments to be substituted in place of formatting anchors.
     */
    public void trace(String msg, Object... params) {
        logInternal(Level.TRACE, msg, null, params);
    }

    /**
     * Logs a message on {@link Level#TRACE} level composed from args with given format and with associated throwable {@code th}.
     *
     * @param msg    The message pattern which will be formatted and passed to the {@link System.Logger}.
     * @param th     The {@code Throwable} associated with log message; can be {@code null}.
     * @param params The list of arguments to be substituted in place of formatting anchors.
     */
    public void trace(String msg, @Nullable Throwable th, Object... params) {
        logInternal(Level.TRACE, msg, th, params);
    }

    /**
     * Logs a message which produces in {@code msgSupplier}, on {@link Level#TRACE} level with associated throwable {@code th}.
     *
     * @param msgSupplier A supplier function that produces a message.
     * @param th          The {@code Throwable} associated with log message; can be {@code null}.
     */
    public void trace(Supplier<String> msgSupplier, @Nullable Throwable th) {
        logInternalExceptional(Level.TRACE, msgSupplier, th);
    }

    /**
     * Logs a message on {@link Level#TRACE} level with associated throwable {@code th}.
     *
     * @param msg The message pattern which will be passed to the {@link System.Logger}.
     * @param th  A {@code Throwable} associated with the log message.
     */
    public void trace(String msg, @Nullable Throwable th) {
        delegate.log(Level.TRACE, msg, th);
    }

    /**
     * Logs a message with an optional list of parameters.
     *
     * @param level  One of the log message level identifiers.
     * @param msg    The string message format in {@link IgniteStringFormatter} format.
     * @param th     The {@code Throwable} associated with the log message.
     * @param params An optional list of parameters to the message (may be none).
     * @throws NullPointerException If {@code level} is {@code null}.
     */
    private void logInternal(Level level, String msg, @Nullable Throwable th, Object... params) {
        Objects.requireNonNull(level);

        if (!delegate.isLoggable(level)) {
            return;
        }

        if (th != null) {
            delegate.log(level, IgniteStringFormatter.format(msg, params), th);
        } else {
            delegate.log(level, IgniteStringFormatter.format(msg, params));
        }
    }

    /**
     * Logs a lazily supplied message associated with a given throwable.
     *
     * @param level       One of the log message level identifiers.
     * @param msgSupplier The supplier function that produces a message.
     * @param th          The {@code Throwable} associated with log message; can be {@code null}.
     * @throws NullPointerException If {@code level} is {@code null}, or {@code msgSupplier} is {@code null}.
     */
    private void logInternalExceptional(Level level, Supplier<String> msgSupplier, @Nullable Throwable th) {
        Objects.requireNonNull(level);
        Objects.requireNonNull(msgSupplier);

        if (!delegate.isLoggable(level)) {
            return;
        }

        delegate.log(level, msgSupplier.get(), th);
    }

    /**
     * Checks if a message of the {@link Level#TRACE} level would be logged by this logger.
     *
     * @return {@code true} if the message level is currently being logged, {@code false} otherwise.
     */
    public boolean isTraceEnabled() {
        return delegate.isLoggable(Level.TRACE);
    }

    /**
     * Checks if a message of the {@link Level#DEBUG} level would be logged by this logger.
     *
     * @return {@code true} if the message level is currently being logged, {@code false} otherwise.
     */
    public boolean isDebugEnabled() {
        return delegate.isLoggable(Level.DEBUG);
    }

    /**
     * Checks if a message of the {@link Level#INFO} level would be logged by this logger.
     *
     * @return {@code true} if the message level is currently being logged, {@code false} otherwise.
     */
    public boolean isInfoEnabled() {
        return delegate.isLoggable(Level.INFO);
    }

    /**
     * Checks if a message of the {@link Level#WARNING} level would be logged by this logger.
     *
     * @return {@code true} if the message level is currently being logged, {@code false} otherwise.
     */
    public boolean isWarnEnabled() {
        return delegate.isLoggable(Level.WARNING);
    }
}
