package org.apache.ignite.cli.commands.connect;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

import jakarta.inject.Inject;
import org.apache.ignite.cli.commands.CliCommandTestIntegrationBase;
import org.apache.ignite.cli.commands.TopLevelCliReplCommand;
import org.apache.ignite.cli.core.repl.prompt.PromptProvider;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import picocli.CommandLine.Help.Ansi;

class ItConnectCommandTest extends CliCommandTestIntegrationBase {
    @Inject
    PromptProvider promptProvider;

    @Override
    protected @NotNull Class<?> getCommandClass() {
        return TopLevelCliReplCommand.class;
    }

    @Test
    @DisplayName("Should connect to cluster with default url")
    void connectWithDefaultUrl() {
        // Given prompt before connect
        String promptBefore = Ansi.OFF.string(promptProvider.getPrompt());
        assertThat(promptBefore).isEqualTo("[disconnected]> ");

        // When connect without parameters
        execute("connect");

        // Then
        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("Connected to http://localhost:10300")
        );
        // And prompt is changed to connect
        String promptAfter = Ansi.OFF.string(promptProvider.getPrompt());
        assertThat(promptAfter).isEqualTo("[http://localhost:10300]> ");
    }

    @Test
    @DisplayName("Should connect to cluster with given url")
    void connectWithGivenUrl() {
        // When connect with given url
        execute("connect", "http://localhost:10301");

        // Then
        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("Connected to http://localhost:10301")
        );
    }

    @Test
    @DisplayName("Should not connect to cluster with wrong url")
    void connectWithWrongUrl() {
        // When connect with wrong url
        execute("connect", "http://localhost:11111");

        // Then
        assertAll(
                () -> assertErrOutputIs("Can not connect to http://localhost:11111" + System.lineSeparator())
        );
        // And prompt is
        String prompt = Ansi.OFF.string(promptProvider.getPrompt());
        assertThat(prompt).isEqualTo("[disconnected]> ");
    }

    @Test
    @DisplayName("Should disconnect after connect")
    void disconnect() {
        // Given connected to cluster
        execute("connect");
        // And prompt is
        String promptBefore = Ansi.OFF.string(promptProvider.getPrompt());
        assertThat(promptBefore).isEqualTo("[http://localhost:10300]> ");

        // When disconnect
        execute("disconnect");
        // Then
        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("Disconnected from http://localhost:10300")
        );
        // And prompt is changed
        String promptAfter = Ansi.OFF.string(promptProvider.getPrompt());
        assertThat(promptAfter).isEqualTo("[disconnected]> ");
    }
}
