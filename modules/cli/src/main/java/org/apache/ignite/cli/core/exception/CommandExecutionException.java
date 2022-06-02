package org.apache.ignite.cli.core.exception;

/**
 * Command execution exception.
 */
public class CommandExecutionException extends RuntimeException {
    private final String commandId;
    private final String reason;

    public CommandExecutionException(String commandId, String reason) {
        this.commandId = commandId;
        this.reason = reason;
    }

    /**
     * Command identifier getter.
     *
     * @return command identifier.
     */
    public String getCommandId() {
        return commandId;
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
