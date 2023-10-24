package org.apache.ignite.internal.lang;

public class SafeTimeReorderException extends IgniteInternalException {
    /**
     * Constructor with error message.
     *
     * @param msg Message.
     */
    public SafeTimeReorderException(String msg) {
        super(msg);
    }

    /**
     * Constructor with error message and cause.
     *
     * @param msg   Message.
     * @param cause Cause.
     */
    public SafeTimeReorderException(String msg, Throwable cause) {
        super(msg, cause);
    }

    /**
     * Creates a new exception with the given error code and detail message.
     *
     * @param code Full error code.
     * @param message Detail message.
     */
    public SafeTimeReorderException(int code, String message) {
        super(code, message);
    }
}

