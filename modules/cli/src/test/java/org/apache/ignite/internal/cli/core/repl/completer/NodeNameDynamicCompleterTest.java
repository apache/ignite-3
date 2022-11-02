package org.apache.ignite.internal.cli.core.repl.completer;

import static org.apache.ignite.internal.cli.commands.OptionsConstants.NODE_NAME_OPTION;
import static org.apache.ignite.internal.cli.commands.OptionsConstants.NODE_NAME_OPTION_SHORT;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class NodeNameDynamicCompleterTest {

    private static final Set<String> prefixes = Set.of(NODE_NAME_OPTION, NODE_NAME_OPTION_SHORT);
    private static final List<String> nodeNames = List.of("node1", "node2", "remoteNode");
    private final NodeNameDynamicCompleter completer = new NodeNameDynamicCompleter(prefixes, nodeNames);


    private static Stream<Arguments> words() {
        return Stream.of(
                Arguments.of(new String[]{"-n"}, nodeNames),
                Arguments.of(new String[]{"--node-name"}, nodeNames),
                Arguments.of(new String[]{"-n","node"}, List.of("node1", "node2")),
                Arguments.of(new String[]{"node", "config", "show"}, Collections.emptyList())
        );
    }

    @ParameterizedTest
    @MethodSource("words")
    void complete(String[] words, List<String> expectedCompletions) {
        Assertions.assertEquals(expectedCompletions, completer.complete(words));
    }
}
