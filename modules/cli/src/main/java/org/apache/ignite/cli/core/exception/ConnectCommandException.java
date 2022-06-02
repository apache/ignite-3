package org.apache.ignite.cli.core.exception;

/**
 * Connect command exception.
 */
public class ConnectCommandException extends RuntimeException {
    private final String reason;

    public ConnectCommandException(String reason) {
        this.reason = reason;
    }

    /**
     * Exception reason getter.
     *
     * @return exception reason.
     */
    public String getReason() {
        return reason;
    }

    @Override
    public String toString() {
        return super.toString();
    }
}
