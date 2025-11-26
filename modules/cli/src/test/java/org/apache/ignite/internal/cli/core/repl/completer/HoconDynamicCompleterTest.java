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
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;

import com.typesafe.config.ConfigFactory;
import java.util.List;
import org.apache.ignite.internal.cli.core.repl.completer.hocon.HoconDynamicCompleter;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

class HoconDynamicCompleterTest {
    HoconDynamicCompleter completer;

    private static HoconDynamicCompleter completerFrom(String configString) {
        return new HoconDynamicCompleter(ConfigFactory.parseString(configString));
    }

    @Test
    void completesSingleRoot() {
        // Given
        completer = completerFrom("root: { subRoot: { subSubRoot: value } }");
        // And
        String[] typedWords = {"cluster", "config", "show", ""};

        // When
        List<String> completions = completer.complete(typedWords);

        // Then
        assertThat(completions, containsInAnyOrder("root"));
    }

    @Test
    void completesMultiRoots() {
        // Given
        completer = completerFrom(
                "root1: { subRoot1: { subSubRoot1: value1 } }, "
                        + "root2: { subRoot2: value2 }, "
                        + "root3: value3");
        // And
        String[] typedWords = {"cluster", "config", "show", ""};

        // When
        List<String> completions = completer.complete(typedWords);

        // Then
        assertThat(completions, containsInAnyOrder("root1", "root2", "root3"));
    }

    @Test
    void completesMultiRootsWhenLastWordPartiallyTyped() {
        // Given
        completer = completerFrom(
                "root1: { subRoot1: { subSubRoot1: value1 } }, "
                        + "root2: { subRoot2: value2 }, "
                        + "root3: value3");
        // And last word is "roo"
        String[] typedWords = {"cluster", "config", "show", "roo"};

        // When
        List<String> completions = completer.complete(typedWords);

        // Then
        assertThat(completions, containsInAnyOrder("root1", "root2", "root3"));
    }

    @Test
    void completePartialPath() {
        // Given
        completer = completerFrom("root: { subRoot: { subSubRoot: value } }");
        // And typed a part of the path
        String[] typedWords = {"cluster", "config", "show", "root."};

        // When
        List<String> completions = completer.complete(typedWords);

        // Then
        assertThat(completions, containsInAnyOrder("root.subRoot"));
    }

    @Test
    void completeWholePath() {
        // Given
        completer = completerFrom("root: { subRoot: { subSubRoot1: value1, subSubRoot2: value2 } }");
        // And typed a part of the path but with "." at the end
        String[] typedWords = {"cluster", "config", "show", "root.subRoot."};

        // When
        List<String> completions = completer.complete(typedWords);

        // Then
        assertThat(completions, containsInAnyOrder("root.subRoot.subSubRoot1", "root.subRoot.subSubRoot2"));
    }

    @Test
    void completesEmptyConfig() {
        // Given
        completer = completerFrom("");
        // And
        String[] typedWords = {"cluster", "config", "show"};

        // When
        List<String> completions = completer.complete(typedWords);

        // Then
        assertThat(completions, hasSize(0));
    }

    @Test
    void doesNotCompletesIfLastWordIsClusterUrl() {
        // Given
        completer = completerFrom("root: { subRoot: value }");
        // And --url is the last typed word
        String[] typedWords = {"cluster", "config", "show", "--url"};

        // When
        List<String> completions = completer.complete(typedWords);

        // Then
        assertThat(completions, hasSize(0));
    }

    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-17418")
    void doesNotCompletesIfLastWordIsClusterUrlAndEmptyString() {
        // Given
        completer = completerFrom("root: { subRoot: value }");
        // And --url is the last typed word
        String[] typedWords = {"cluster", "config", "show", "--url", ""};

        // When
        List<String> completions = completer.complete(typedWords);

        // Then
        assertThat(completions, hasSize(0));
    }

    @Test
    void shouldAlwaysCompleteIfActivationPostfixIsEmptyString() {
        // Given
        completer = completerFrom("root: { subRoot: value }");
        // And
        String[] typedWords = {"cluster", "config", "update", ""};

        // When
        List<String> completions = completer.complete(typedWords);

        // Then
        assertThat(completions, containsInAnyOrder("root"));
    }
}
