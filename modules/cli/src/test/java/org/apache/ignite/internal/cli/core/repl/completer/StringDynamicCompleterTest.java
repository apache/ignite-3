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

import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;


/** Tests for {@link StringDynamicCompleter}. */
class StringDynamicCompleterTest {
    private static final Set<String> candidates = Set.of("node1", "node2", "remoteNode");
    private StringDynamicCompleter completer;

    private static Stream<Arguments> words() {
        return Stream.of(
                Arguments.of(new String[]{"n"}, Set.of("node1", "node2")),
                Arguments.of(new String[]{""}, candidates),
                Arguments.of(new String[]{" "}, candidates),
                Arguments.of(new String[]{"node1", ""}, candidates),
                Arguments.of(new String[]{"-n", "node"}, Set.of("node1", "node2"))
        );
    }

    @BeforeEach
    void setUp() {
        completer = new StringDynamicCompleter(candidates);
    }

    @ParameterizedTest
    @MethodSource("words")
    void complete(String[] words, Set<String> expectedCompletions) {
        assertThat(completer.complete(words), containsInAnyOrder(expectedCompletions.toArray(new String[0])));
    }
}
