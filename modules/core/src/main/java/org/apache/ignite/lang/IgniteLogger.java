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

import java.util.Objects;
import org.jetbrains.annotations.NotNull;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.ERROR;
import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.TRACE;
import static java.lang.System.Logger.Level.WARNING;
import static org.apache.ignite.lang.LoggerMessageHelper.format;

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
     * A last one of the parameter list may be {@code Throwable}, that cause of the message appeared.
     */
    public void info(String msg, Object... params) {
        if (log.isLoggable(INFO)) {
            IgniteBiTuple<String, Throwable> readyParams = format(msg, params);

            log.log(INFO, readyParams.get1(), readyParams.get1());
        }
    }

    /**
     * @param msg The message pattern which will be formatted and passed to the {@link System.Logger}.
     * @param params A list of arguments to be substituted in place of formatting anchors.
     * A last one of the parameter list may be {@code Throwable}, that cause of the message appeared.
     */
    public void debug(String msg, Object... params) {
        if (log.isLoggable(DEBUG)) {
            IgniteBiTuple<String, Throwable> readyParams = format(msg, params);

            log.log(DEBUG, readyParams.get1(), readyParams.get2());
        }
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
     * A last one of the parameter list may be {@code Throwable}, that cause of the message appeared.
     */
    public void warn(String msg, Object... params) {
        if (log.isLoggable(WARNING)) {
            IgniteBiTuple<String, Throwable> readyParams = format(msg, params);

            log.log(WARNING, readyParams.get1(), readyParams.get2());
        }
    }

    /**
     * @param msg The message pattern which will be formatted and passed to the {@link System.Logger}.
     * @param params A list of arguments to be substituted in place of formatting anchors.
     * A last one of the parameter list may be {@code Throwable}, that cause of the message appeared.
     */
    public void error(String msg, Object... params) {
        if (log.isLoggable(ERROR)) {
            IgniteBiTuple<String, Throwable> readyParams = format(msg, params);

            log.log(ERROR, readyParams.get1(), readyParams.get2());
        }
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
     * A last one of the parameter list may be {@code Throwable}, that cause of the message appeared.
     */
    public void trace(String msg, Object... params) {
        if (log.isLoggable(TRACE)) {
            IgniteBiTuple<String, Throwable> readyParams = format(msg, params);

            log.log(TRACE, readyParams.get1(), readyParams.get2());
        }
    }

    /**
     * @return {@code true} if the TRACE log message level is currently being logged.
     */
    public boolean isTraceEnabled() {
        return log.isLoggable(TRACE);
    }

    /**
     * @return {@code true} if the DEBUG log message level is currently being logged.
     */
    public boolean isDebugEnabled() {
        return log.isLoggable(DEBUG);
    }

    /**
     * @return {@code true} if the INFO log message level is currently being logged.
     */
    public boolean isInfoEnabled() {
        return log.isLoggable(INFO);
    }
}
