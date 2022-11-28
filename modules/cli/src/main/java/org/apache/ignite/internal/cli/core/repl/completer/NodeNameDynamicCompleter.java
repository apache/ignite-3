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

import static org.apache.ignite.internal.cli.util.ArrayUtils.findLastNotEmptyWord;
import static org.apache.ignite.internal.cli.util.ArrayUtils.findLastNotEmptyWordBeforeWordFromEnd;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.internal.cli.NodeNameRegistry;

/**
 * Completes typed words with node names.
 */
public class NodeNameDynamicCompleter implements DynamicCompleter {

    /** Words, after those the completer should have been activated. */
    private final Set<String> activationPostfixes;

    /** Node names registry. */
    private final NodeNameRegistry nodeNameRegistry;

    /** Default constructor that creates an instance from a given set of postfixes that trigger the completion. */
    public NodeNameDynamicCompleter(Set<String> activationPostfixes, NodeNameRegistry nodeNameRegistry) {
        this.activationPostfixes = activationPostfixes;
        this.nodeNameRegistry = nodeNameRegistry;
    }

    @Override
    public List<String> complete(String[] words) {
        String lastWord = findLastNotEmptyWord(words);
        String beforeLastWord = findLastNotEmptyWordBeforeWordFromEnd(words, lastWord);
        if (activationPostfixes.contains(lastWord)) {
            return new ArrayList<>(nodeNameRegistry.getAllNames());
        } else if (activationPostfixes.contains(beforeLastWord)) {
            return nodeNameRegistry.getAllNames().stream()
                    .filter(it -> it.startsWith(lastWord))
                    .collect(Collectors.toList());
        } else {
            return Collections.emptyList();
        }
    }
}
