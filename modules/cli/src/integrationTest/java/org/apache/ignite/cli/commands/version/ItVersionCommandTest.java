package org.apache.ignite.cli.commands.version;

import static org.junit.jupiter.api.Assertions.assertAll;

import java.util.concurrent.Callable;
import org.apache.ignite.cli.commands.CliCommandTestIntegrationBase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;
import picocli.CommandLine.Command;

class ItVersionCommandTest extends CliCommandTestIntegrationBase {

    public static void main(String[] args) {
        CommandLine commandLine = new CommandLine(new CallableCommand());
        String[] argv = {"sub"};
        commandLine.execute(argv);
        System.out.println(
                commandLine.getSubcommands().get("sub").getExecutionResult().toString()); // NPE: commandLine.getExecutionResult() is null
    }

    @Test
    @DisplayName("Should print cli version that is got from pom.xml")
    void printVersion() {
        // When
        execute("--version");

        // Then
        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("Apache Ignite CLI ver")
        );
    }

    @Command(subcommands = CallableSubCommand.class)
    public static class CallableCommand implements Callable<String> { // top level command
        @Override
        public String call() {
            return "Result";
        }
    }

    @Command(name = "sub")
    public static class CallableSubCommand implements Callable<String> { // subcommand that I'd like to test
        @Override
        public String call() {
            System.out.println("I am executed!"); // this will be printed in the console
            return "Result subcommand"; // but this result is unreachable after subcommand is executed
        }
    }
}