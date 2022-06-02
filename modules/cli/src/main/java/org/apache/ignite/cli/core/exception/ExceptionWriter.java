package org.apache.ignite.cli.core.exception;

import java.io.PrintWriter;

/**
 * Writer for exception error messages.
 */
public interface ExceptionWriter {
    /**
     * Write provided exception message.
     *
     * @param errMessage error message.
     */
    void write(String errMessage);

    /**
     * Helper mapper.
     *
     * @param pw {@link PrintWriter} instance.
     * @return {@link ExceptionWriter} instance based on {@param pw}.
     */
    static ExceptionWriter fromPrintWriter(PrintWriter pw) {
        return pw::println;
    }
}
