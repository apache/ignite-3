package org.apache.ignite.cli.core.exception;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * General interface of exception handler.
 *
 * @param <T> exception type.
 */
public interface ExceptionHandler<T extends Throwable> {
    Logger logger = LoggerFactory.getLogger("DefaultExceptionHandler");

    ExceptionHandler<Throwable> DEFAULT = new ExceptionHandler<>() {
        @Override
        public void handle(ExceptionWriter err, Throwable e) {
            logger.error("Unhandled exception ", e);
            err.write("Internal error!");
        }

        @Override
        public Class<Throwable> applicableException() {
            return Throwable.class;
        }
    };

    /**
     * Handler method.
     *
     * @param err writer instance for any error messages.
     * @param e exception instance.
     */
    void handle(ExceptionWriter err, T e);

    /**
     * Exception class getter.
     *
     * @return class of applicable exception for the handler.
     */
    Class<T> applicableException();
}
