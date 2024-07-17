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

package org.apache.ignite.internal.cli.core.repl.completer.unit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import org.junit.jupiter.api.Test;

class UnitNodesCompleterFilterTest {
    private final UnitNodesCompleterFilter filter = new UnitNodesCompleterFilter();

    @Test
    void suggestsAllTheFirstTime() {
        String[] words = {"cluster", "unit", "deploy", "--nodes"};
        String[] candidates = {"node1", "node2", "ALL", "MAJORITY"};

        String[] filteredCandidates = filter.filter(words, candidates);
        assertArrayEquals(candidates, filteredCandidates);
    }

    @Test
    void doesNotSuggestAlreadyUsedNodes() {
        String[] words = {"cluster", "unit", "deploy", "--nodes", "node1, node2", "--nodes"};
        String[] candidates = {"node1", "node2", "node3", "node4"};

        String[] filteredCandidates = filter.filter(words, candidates);
        assertArrayEquals(new String[]{"node3", "node4"}, filteredCandidates);
    }

    @Test
    void doesNotSuggestAliasesWhenNodesAreSpecified() {
        String[] words = {"cluster", "unit", "deploy", "--nodes", "node1", "--nodes"};
        String[] candidates = {"node1", "node2", "ALL", "MAJORITY"};

        String[] filteredCandidates = filter.filter(words, candidates);
        assertArrayEquals(new String[]{"node2"}, filteredCandidates);
    }

    @Test
    void emptySuggestions() {
        String[] words = {"cluster", "unit", "deploy", "--nodes", "node1", "--nodes"};
        String[] candidates = {"node1", "ALL", "MAJORITY"};

        String[] filteredCandidates = filter.filter(words, candidates);
        assertArrayEquals(new String[]{}, filteredCandidates);
    }
}
