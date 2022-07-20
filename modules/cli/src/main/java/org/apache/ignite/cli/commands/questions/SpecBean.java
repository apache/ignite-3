package org.apache.ignite.cli.commands.questions;

import jakarta.inject.Singleton;
import picocli.CommandLine;
import picocli.CommandLine.Model.CommandSpec;

/**
 * {@inheritDoc}
 */
@Singleton
@CommandLine.Command()
public class SpecBean {
    @CommandLine.Spec
    private CommandSpec spec;


    public CommandSpec getSpec() {
        return spec;
    }
}
