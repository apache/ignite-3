package org.apache.ignite.internal.cli.commands.cluster;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.ignite.internal.cli.call.cluster.status.ClusterRenameCall;
import org.apache.ignite.internal.cli.call.cluster.status.ClusterRenameCallInput;
import org.apache.ignite.internal.cli.commands.CliCommandTestBase;
import org.apache.ignite.internal.cli.commands.TopLevelCliCommand;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class RenameTest extends CliCommandTestBase {
    @Override
    protected Class<?> getCommandClass() {
        return TopLevelCliCommand.class;
    }

    private static Stream<Arguments> calls() {
        return Stream.of(
                arguments(
                        "cluster rename",
                        ClusterRenameCall.class,
                        ClusterRenameCallInput.class,
                        (Function<ClusterRenameCallInput, String>) ClusterRenameCallInput::getName
                )
        );
    }

    @ParameterizedTest
    @MethodSource("calls")
    void noParameter(String command) {
        // When executed without arguments
        execute(command);

        assertAll(
                () -> assertExitCodeIs(2),
                this::assertOutputIsEmpty,
                () -> assertErrOutputContains("Failed to parse name.")
        );
    }
}
