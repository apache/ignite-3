package org.apache.ignite.cli.commands.decorators.core;

/**
 * Interface for transformation command output to terminal output.
 *
 * @param <CommandDataT> type of command output.
 * @param <TerminalDataT> type of terminal output.
 */
public interface Decorator<CommandDataT, TerminalDataT extends TerminalOutput> {
    /**
     * Interface for transforming command output to terminal output.
     *
     * @param data incoming data object.
     * @return Decorated object with type {@link TerminalDataT}.
     */
    TerminalDataT decorate(CommandDataT data);
}
