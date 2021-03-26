package org.apache.ignite.lang;

import java.util.Objects;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.ERROR;
import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.WARNING;

/**
 * Wraps system logger for more convenient access.
 */
public class LogWrapper {
    /** Logger delegate. */
    private final System.Logger log;

    /**
     * @param cls The class for a logger.
     */
    public LogWrapper(Class<?> cls) {
        this.log = System.getLogger(Objects.requireNonNull(cls).getName());
    }

    /**
     * @param msg The message.
     * @param params Parameters.
     */
    public void info(String msg, Object... params) {
        log.log(INFO, msg, params);
    }

    /**
     * @param msg The message.
     * @param params Parameters.
     */
    public void debug(String msg, Object... params) {
        log.log(DEBUG, msg, params);
    }

    /**
     * @param msg The message.
     * @param params Parameters.
     */
    public void warn(String msg, Object... params) {
        log.log(WARNING, msg, params);
    }

    /**
     * @param msg The message.
     * @param params Parameters.
     */
    public void error(String msg, Object... params) {
        log.log(ERROR, msg, params);
    }

    /**
     * @param msg The message.
     * @param e The exception.
     */
    public void error(String msg, Exception e) {
        log.log(ERROR, msg, e);
    }
}
