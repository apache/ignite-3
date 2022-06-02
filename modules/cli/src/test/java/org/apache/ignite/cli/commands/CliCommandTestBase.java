package org.apache.ignite.cli.commands;

import io.micronaut.configuration.picocli.MicronautFactory;
import io.micronaut.context.ApplicationContext;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.io.PrintWriter;
import java.io.StringWriter;
import picocli.CommandLine;

/**
 * Base class for testing CLI commands.
 */
@MicronautTest
public class CliCommandTestBase {
    @Inject
    private ApplicationContext context;

    private CommandLine commandLine;
    protected StringWriter err;
    protected StringWriter out;
    private int exitCode = Integer.MIN_VALUE;

    protected void setUp(Class<?> commandClass) {
        err = new StringWriter();
        out = new StringWriter();
        commandLine = new CommandLine(commandClass, new MicronautFactory(context));
        commandLine.setErr(new PrintWriter(err));
        commandLine.setOut(new PrintWriter(out));
    }

    protected void execute(String... args) {
        exitCode = commandLine.execute(args);
    }
}
