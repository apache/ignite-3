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

package org.apache.ignite.internal.cli.core.repl.completer.filter;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayContaining;

import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import java.util.Arrays;
import java.util.stream.Stream;
import org.apache.ignite.internal.cli.commands.cluster.unit.NodesAlias;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;


@MicronautTest
class DeployUnitsOptionsFilterTest {
    private final DeployUnitsOptionsFilter filter = new DeployUnitsOptionsFilter();

    @Test
    void nodesAreNotExcludedWhenAliasIsNotPresent() {
        String[] words = {"cluster", "unit", "deploy", "--nodes", "node1", "node2"};
        String[] candidates = {"--nodes", "--path", "--version"};
        String[] filteredCandidates = filter.filter(words, candidates);
        assertThat(filteredCandidates, arrayContaining(candidates));
    }

    @ParameterizedTest
    @MethodSource("aliases")
    void nodesAreExcludedWhenAliasIsPresent(NodesAlias alias) {
        String[] words = {"cluster", "unit", "deploy", "--nodes", alias.toString()};
        String[] candidates = {"--nodes", "--path", "--version"};
        String[] filteredCandidates = filter.filter(words, candidates);
        assertThat(filteredCandidates, arrayContaining("--path", "--version"));
    }

    private static Stream<NodesAlias> aliases() {
        return Arrays.stream(NodesAlias.values());
    }
}
