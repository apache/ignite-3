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

package org.apache.ignite.migrationtools.persistence;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.ignite.IgniteLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class LoggerBridge implements IgniteLogger {

    private final Logger logger;

    public LoggerBridge() {
        this(LoggerFactory.getLogger(""));
    }

    public LoggerBridge(Logger logger) {
        this.logger = logger;
    }

    @Override
    public IgniteLogger getLogger(Object o) {
        return o == null
                ? new LoggerBridge()
                : new LoggerBridge(LoggerFactory.getLogger(o.getClass()));
    }

    @Override
    public void trace(String s) {
        logger.trace(s);
    }

    @Override
    public void debug(String s) {
        logger.debug(s);
    }

    @Override
    public void info(String s) {
        logger.info(s);
    }

    @Override
    public void warning(String s, Throwable throwable) {
        logger.warn(s, throwable);
    }

    @Override
    public void error(String s, Throwable throwable) {
        logger.error(s, throwable);
    }

    @Override
    public boolean isTraceEnabled() {
        return logger.isTraceEnabled();
    }

    @Override
    public boolean isDebugEnabled() {
        return logger.isDebugEnabled();
    }

    @Override
    public boolean isInfoEnabled() {
        return logger.isInfoEnabled();
    }

    @Override
    public boolean isQuiet() {
        return !logger.isErrorEnabled();
    }

    @Override
    public String fileName() {
        throw new NotImplementedException();
    }
}
