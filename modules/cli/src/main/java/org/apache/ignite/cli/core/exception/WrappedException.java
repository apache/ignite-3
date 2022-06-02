package org.apache.ignite.cli.core.exception;

/**
 * Wrapper for checked exception.
 * This exception will be handled as cause type.
 */
public class WrappedException extends RuntimeException {
    /**
     * Constructor.
     *
     * @param cause cause exception.
     */
    public WrappedException(Throwable cause) {
        super(cause);
    }
}