/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
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
