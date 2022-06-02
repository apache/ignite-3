package org.apache.ignite.cli.commands.decorators.core;

/**
 * Interface for terminal output representation.
 */
public interface TerminalOutput {
    /**
     * Terminal output transformation.
     *
     * @return String representation of some
     */
    String toTerminalString();
}
