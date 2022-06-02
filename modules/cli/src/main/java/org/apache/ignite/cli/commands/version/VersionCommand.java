package org.apache.ignite.cli.commands.version;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.ignite.cli.VersionProvider;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

/**
 * Command that prints CLI version.
 */
@Command(name = "version", description = "Prints CLI version.")
@Singleton
public class VersionCommand implements Runnable {

    @Spec
    private CommandSpec commandSpec;

    @Inject
    private VersionProvider versionProvider;

    /** {@inheritDoc} */
    @Override
    public void run() {
        commandSpec.commandLine().getOut().println(versionProvider.getVersion()[0]);
    }
}
