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

class IgniteLoggerImpl implements IgniteLogger {
    /** Logger delegate. */
    final System.Logger delegate;

    /**
     * Creates logger facade for a given delegate.
     *
     * @param delegate The delegate to create facade for.
     */
    IgniteLoggerImpl(System.Logger delegate) {
        this.delegate = delegate;
    }

    @Override
    public void info(String msg, Object... params) {
        logInternal(Level.INFO, msg, null, params);
    }

    @Override
    public void info(String msg, @Nullable Throwable th, Object... params) {
        logInternal(Level.INFO, msg, th, params);
    }

    @Override
    public void info(Supplier<String> msgSupplier, @Nullable Throwable th) {
        logInternalExceptional(Level.INFO, msgSupplier, th);
    }

    @Override
    public void info(String msg, @Nullable Throwable th) {
        delegate.log(Level.INFO, msg, th);
    }

    @Override
    public void debug(String msg, Object... params) {
        logInternal(Level.DEBUG, msg, null, params);
    }

    @Override
    public void debug(String msg, @Nullable Throwable th, Object... params) {
        logInternal(Level.DEBUG, msg, th, params);
    }

    @Override
    public void debug(Supplier<String> msgSupplier, @Nullable Throwable th) {
        logInternalExceptional(Level.DEBUG, msgSupplier, th);
    }

    @Override
    public void debug(String msg, @Nullable Throwable th) {
        delegate.log(Level.DEBUG, msg, th);
    }

    @Override
    public void warn(String msg, Object... params) {
        logInternal(Level.WARNING, msg, null, params);
    }

    @Override
    public void warn(String msg, @Nullable Throwable th, Object... params) {
        logInternal(Level.WARNING, msg, th, params);
    }

    @Override
    public void warn(Supplier<String> msgSupplier, @Nullable Throwable th) {
        logInternalExceptional(Level.WARNING, msgSupplier, th);
    }

    @Override
    public void warn(String msg, @Nullable Throwable th) {
        delegate.log(Level.WARNING, msg, th);
    }

    @Override
    public void error(String msg, Object... params) {
        logInternal(Level.ERROR, msg, null, params);
    }

    @Override
    public void error(String msg, @Nullable Throwable th, Object... params) {
        logInternal(Level.ERROR, msg, th, params);
    }

    @Override
    public void error(Supplier<String> msgSupplier, @Nullable Throwable th) {
        logInternalExceptional(Level.ERROR, msgSupplier, th);
    }

    @Override
    public void error(String msg, @Nullable Throwable th) {
        delegate.log(Level.ERROR, msg, th);
    }

    @Override
    public void trace(String msg, Object... params) {
        logInternal(Level.TRACE, msg, null, params);
    }

    @Override
    public void trace(String msg, @Nullable Throwable th, Object... params) {
        logInternal(Level.TRACE, msg, th, params);
    }

    @Override
    public void trace(Supplier<String> msgSupplier, @Nullable Throwable th) {
        logInternalExceptional(Level.TRACE, msgSupplier, th);
    }

    @Override
    public void trace(String msg, @Nullable Throwable th) {
        delegate.log(Level.TRACE, msg, th);
    }

    /**
     * Logs a message with an optional list of parameters.
     *
     * @param level One of the log message level identifiers.
     * @param msg The string message format in {@link IgniteStringFormatter} format.
     * @param th The {@code Throwable} associated with the log message.
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
     * @param level One of the log message level identifiers.
     * @param msgSupplier The supplier function that produces a message.
     * @param th The {@code Throwable} associated with log message; can be {@code null}.
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
}
