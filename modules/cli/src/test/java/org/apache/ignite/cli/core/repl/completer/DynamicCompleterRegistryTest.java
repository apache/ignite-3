/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.cli.core.repl.completer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DynamicCompleterRegistryTest {

    DynamicCompleterRegistry registry;

    DynamicCompleter completer1;

    DynamicCompleter completer2;

    DynamicCompleter completer3;

    @BeforeEach
    void setUp() {
        registry = new DynamicCompleterRegistry();
        completer1 = words -> null;
        completer2 = words -> null;
        completer3 = words -> null;
    }

    @Test
    void findsCompletersRegisteredWithPredicate() {
        // Given
        registry.register(words -> words[0].equals("command1"), completer1);
        registry.register(words -> words[0].equals("command2"), completer2);
        registry.register(words -> words[0].equals("command2"), completer3);

        // When find completers for "command1"
        List<DynamicCompleter> completers = registry.findCompleters(new String[]{"command1"});

        // Then
        assertThat(completers, containsInAnyOrder(completer1));
    }

    @Test
    void findsCompletersRegisteredWithStaticCommand() {
        // Given
        registry.register(new String[]{"command1", "subcommand1"}, completer1);
        registry.register(new String[]{"command1", "subcommand1"}, completer2);
        registry.register(new String[]{"command2"}, completer3);

        // When find completers for "command1"
        List<DynamicCompleter> completers = registry.findCompleters(new String[]{"command1", "subcommand1"});

        // Then
        assertThat(completers, containsInAnyOrder(completer1, completer2));
    }

    @Test
    void returnsEmptyCollectionIfThereIsNoSuitableCompleter() {
        // Given
        registry.register(new String[]{"command1", "subcommand1"}, completer1);
        registry.register(new String[]{"command1", "subcommand1"}, completer2);
        registry.register(new String[]{"command2"}, completer3);

        // Then
        assertThat(registry.findCompleters(new String[]{"command1"}), is(empty()));
        assertThat(registry.findCompleters(new String[]{"command3"}), is(empty()));
        // But if command start with one of the registered prefixes
        assertThat(
                registry.findCompleters(new String[]{"command1", "subcommand1", "subsubcommand1"}),
                containsInAnyOrder(completer1, completer2)
        );
    }
}
