package org.apache.ignite.cli.commands.decorators.core;

public interface Decorator<CommandDateType extends CommandOutput, TerminalDataType extends TerminalOutput> {
    TerminalDataType decorate(CommandDateType date);
}
