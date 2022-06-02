package org.apache.ignite.cli.commands.decorators;

import org.apache.ignite.cli.call.status.Status;
import org.apache.ignite.cli.commands.decorators.core.Decorator;
import org.apache.ignite.cli.commands.decorators.core.TerminalOutput;
import picocli.CommandLine.Help.Ansi;

/**
 * Decorator for {@link Status}.
 */
public class StatusDecorator implements Decorator<Status, TerminalOutput> {

    @Override
    public TerminalOutput decorate(Status data) {
        if (!data.isConnected()) {
            return () -> "You are not connected to any Ignite 3 node. Try to run 'connect' command.";
        }

        return () -> Ansi.AUTO.string("Connected to " + data.getConnectedNodeUrl() + System.lineSeparator()
                + "[name: " + data.getName() + ", nodes: " + data.getNodeCount() + ", status: "
                + (data.isInitialized() ? "@|fg(10) active|@" : "@|fg(9) not initialized|@") + "]");
    }
}
