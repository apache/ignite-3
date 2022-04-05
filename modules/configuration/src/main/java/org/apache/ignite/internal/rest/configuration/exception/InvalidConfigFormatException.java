package org.apache.ignite.internal.rest.configuration.exception;

/**
 * Exception that is thrown when the wrong configuration is given.
 */
public class InvalidConfigFormatException extends RuntimeException {
    public InvalidConfigFormatException(Throwable cause) {
        super(cause);
    }
}
