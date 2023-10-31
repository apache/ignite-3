/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.cli.core.repl.completer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.internal.cli.commands.Options;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DynamicCompleterRegistryTest {

    DynamicCompleterRegistry registry;

    DynamicCompleter completer1;

    DynamicCompleter completer2;

    DynamicCompleter completer3;

    /** Makes reading easier. */
    private static String[] words(String... words) {
        return words;
    }

    @BeforeEach
    void setUp() {
        registry = new DynamicCompleterRegistry();
        completer1 = words -> null;
        completer2 = words -> null;
        completer3 = words -> null;
    }

    @Test
    void findsCompleterRegisteredWithMultyCommand() {
        // Given
        registry.register(
                CompleterConf.builder()
                        .command("command1")
                        .command("command2").build(),
                words -> completer1
        );

        // Then
        assertThat(registry.findCompleters(words("command1")), containsInAnyOrder(completer1));
        // And
        assertThat(registry.findCompleters(words("command2")), containsInAnyOrder(completer1));
    }

    @Test
    void findsCompletersRegisteredWithStaticCommand() {
        // Given
        registry.register(CompleterConf.forCommand("command1", "subcommand1"), words -> completer1);
        registry.register(CompleterConf.forCommand("command1", "subcommand1"), words -> completer2);
        registry.register(CompleterConf.forCommand("command2"), words -> completer3);

        // When find completers for "command1"
        List<DynamicCompleter> completers = registry.findCompleters(words("command1", "subcommand1"));

        // Then
        assertThat(completers, containsInAnyOrder(completer1, completer2));
    }

    @Test
    void returnsEmptyCollectionIfThereIsNoSuitableCompleter() {
        // Given
        registry.register(CompleterConf.forCommand("command1", "subcommand1"), words -> completer1);
        registry.register(CompleterConf.forCommand("command1", "subcommand1"), words -> completer2);
        registry.register(CompleterConf.forCommand("command2"), words -> completer3);

        // Then
        assertThat(registry.findCompleters(words("command1")), is(empty()));
        assertThat(registry.findCompleters(words("command3")), is(empty()));
        // But if command start with one of the registered prefixes
        assertThat(
                registry.findCompleters(words("command1", "subcommand1", "subsubcommand1")),
                containsInAnyOrder(completer1, completer2)
        );
    }

    @Test
    void doesntReturnCompleterWithDisableOptions() {
        // Given
        registry.register(
                CompleterConf.builder().command("command1", "subcommand1").disableOptions("--stopWord").build(),
                words -> completer1
        );

        // Then
        assertThat(
                registry.findCompleters(words("command1", "subcommand1", "subsub1")),
                containsInAnyOrder(completer1)
        );
        assertThat(
                registry.findCompleters(words("command1", "subcommand1", "subsub1", "")),
                containsInAnyOrder(completer1)
        );
        assertThat(
                registry.findCompleters(words("command1", "subcommand1", "", "")),
                containsInAnyOrder(completer1)
        );

        // But if command ends with a stop word
        assertThat(
                registry.findCompleters(words("command1", "subcommand1", "--stopWord")),
                is(empty())
        );
        assertThat(
                registry.findCompleters(words("command1", "subcommand1", "--stopWord", "")),
                is(empty())
        );
        assertThat(
                registry.findCompleters(words("command1", "subcommand1", "--stopWord", "do-not-complete-me")),
                is(empty())
        );
        assertThat(
                registry.findCompleters(words("command1", "subcommand1", "--stopWord", "do-not-complete-me", "but-complete-me")),
                containsInAnyOrder(completer1)
        );
    }

    @Test
    @DisplayName("If enable options are provided than they are taken into account")
    void enableOptions() {
        // Given completer with enable option
        registry.register(
                CompleterConf.builder()
                        .command("command1")
                        .enableOptions("--complete-after-me", "-com")
                        .build(),
                words -> completer1
        );

        // Then for words without those options there are no completers
        assertThat(registry.findCompleters(words("command1")), is(empty()));
        // And enable option should not be completed itself
        assertThat(registry.findCompleters(words("command1", "-co")), is(empty()));

        // But if any of the enable options are provided then completer is found
        assertThat(registry.findCompleters(words("command1", "some", "--complete-after-me", "")), containsInAnyOrder(completer1));
        assertThat(registry.findCompleters(words("command1", "-com", "")), containsInAnyOrder(completer1));
        assertThat(registry.findCompleters(words("command1", "-com", " ")), containsInAnyOrder(completer1));
        assertThat(registry.findCompleters(words("command1", "-com", "autocompleteme")), containsInAnyOrder(completer1));
    }

    @Test
    @DisplayName("Combination of completers with the same enable/disable options")
    void combinationOfCompleters() {
        // Given first completer with disable option
        registry.register(
                CompleterConf.builder()
                        .command("node", "config", "show")
                        .disableOptions("-n").build(),
                words -> completer1
        );
        // And the second completer with the same enable option
        registry.register(
                CompleterConf.builder().enableOptions("-n").build(),
                (words -> completer2)
        );

        // Then they do not intersect
        assertThat(
                registry.findCompleters(words("node", "config", "show", "-n")), // "-n" is competed by completer2
                both(hasSize(1)).and(containsInAnyOrder(completer2))
        );
        assertThat(
                registry.findCompleters(words("node", "config", "show", "-n", "nodeName")),
                both(hasSize(1)).and(containsInAnyOrder(completer2))
        );
        assertThat(
                registry.findCompleters(words("node", "config", "show", "-n", "nodeName", " ")),
                both(hasSize(1)).and(containsInAnyOrder(completer1))
        );
        assertThat(
                registry.findCompleters(words("node", "config", "show", "-n", "nodeName", "completeMe")),
                both(hasSize(1)).and(containsInAnyOrder(completer1))
        );
    }

    @Test
    @DisplayName("If some completer has exclusive enable option set then all other completers have this option as disable option")
    void exclusiveEnableOption() {
        // Given completer with exclusive enable option -n
        registry.register(
                CompleterConf.builder()
                        .command("node", "config", "show")
                        .enableOptions(Options.NODE_NAME)
                        .exclusiveEnableOptions().build(),
                words -> completer1
        );
        // And completer for the same command
        registry.register(
                CompleterConf.forCommand("node", "config", "show"),
                words -> completer2
        );
        // And common completer for other option
        registry.register(
                CompleterConf.builder().enableOptions("-l").build(),
                words -> completer3
        );

        // Then exclusive option is on the priority and no other completer is used for -n
        assertThat(
                registry.findCompleters(words("node", "config", "show", "-n")),
                both(hasSize(1)).and(containsInAnyOrder(completer1))
        );
        assertThat(
                registry.findCompleters(words("node", "config", "show", "-n", "")),
                both(hasSize(1)).and(containsInAnyOrder(completer1))
        );
        assertThat(
                registry.findCompleters(words("node", "config", "show", "-n", "nod")),
                both(hasSize(1)).and(containsInAnyOrder(completer1))
        );
        // But other cases work well
        assertThat(
                registry.findCompleters(words("node", "config", "show", "-l", "nod")),
                both(hasSize(2)).and(containsInAnyOrder(completer2, completer3))
        );
        assertThat(
                registry.findCompleters(words("node", "config", "show", "-n", "node", " ")),
                both(hasSize(1)).and(containsInAnyOrder(completer2))
        );
    }

    @Test
    @DisplayName(
            "If some completer has exclusive enable option set then all other completers have this option as disable option, "
                    + "registration order changed"
    )
    void exclusiveEnableOptionRegisterInAnotherOrder() {
        // Given completers without exclusive enable option registered first
        registry.register(
                CompleterConf.forCommand("node", "config", "show"),
                words -> completer2
        );
        registry.register(
                CompleterConf.builder().enableOptions("-l").build(),
                words -> completer3
        );
        // And exclusiveEnableOption is registered last
        registry.register(
                CompleterConf.builder()
                        .enableOptions("-n")
                        .exclusiveEnableOptions().build(),
                words -> completer1
        );

        // Then exclusive option is on the priority and no other completer is used for -n
        assertThat(
                registry.findCompleters(words("node", "config", "show", "-n", "nod")),
                both(hasSize(1)).and(containsInAnyOrder(completer1))
        );
        assertThat(
                registry.findCompleters(words("node", "config", "show", "-n", "")),
                both(hasSize(1)).and(containsInAnyOrder(completer1))
        );
        assertThat(
                registry.findCompleters(words("node", "config", "show", "-n")),
                both(hasSize(1)).and(containsInAnyOrder(completer1))
        );
        // But other cases work well
        assertThat(
                registry.findCompleters(words("node", "config", "show", "-l", "nod")),
                both(hasSize(2)).and(containsInAnyOrder(completer2, completer3))
        );
        assertThat(
                registry.findCompleters(words("node", "config", "show", "-n", "node", "")),
                both(hasSize(1)).and(containsInAnyOrder(completer2))
        );
    }

    @Test
    void doesNotReturnFilteredCandidates() {
        // Given
        registry.register(CompleterConf.builder()
                .command("command1", "subcommand1")
                .filter((words, candidates) -> {
                    return Arrays.stream(candidates)
                            .filter(it -> !"candidate2".equals(it))
                            .toArray(String[]::new);
                }).build(), words -> ignored -> List.of("candidate1", "candidate2"));

        // Then
        List<DynamicCompleter> completers = registry.findCompleters(words("command1", "subcommand1"));
        assertThat(completers, not(empty()));

        List<String> candidates = completers.stream()
                .flatMap(it -> it.complete(words("word")).stream())
                .collect(Collectors.toList());
        assertThat(candidates, contains("candidate1"));
        assertThat(candidates, not(contains("candidate2")));
    }

    @Test
    void positionalParameter() {
        // Given completer for single positional parameter
        registry.register(
                CompleterConf.builder()
                        .command("cluster", "config", "show") // should be real commands
                        .singlePositionalParameter()
                        .build(),
                words -> completer1
        );

        // When there is no positional argument typed
        List<DynamicCompleter> completers = registry.findCompleters(words("cluster", "config", "show"));

        // Then completer is returned
        assertThat(completers, contains(completer1));

        // When there is one positional argument typed
        List<DynamicCompleter> completersWithPositional = registry.findCompleters(words("cluster", "config", "show", "arg1", ""));

        // Then completer is not returned
        assertThat(completersWithPositional, is(empty()));
    }
}
