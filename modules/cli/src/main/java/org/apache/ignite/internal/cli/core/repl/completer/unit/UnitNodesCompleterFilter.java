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

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.internal.cli.commands.Options;
import org.apache.ignite.internal.cli.commands.cluster.unit.NodesAlias;
import org.apache.ignite.internal.cli.core.repl.completer.filter.CompleterFilter;

/**
 * Filter for deployment target nodes.
 * This filter removes all nodes aliases from candidates if words contain the activation word multiple times.
 */
public class UnitNodesCompleterFilter implements CompleterFilter {

    private final Set<String> activationWords;

    /**
     * Constructor.
     *
     * @param options command options.
     */
    public UnitNodesCompleterFilter(Options options) {
        this.activationWords = Stream.of(options.fullName(), options.shortName()).collect(Collectors.toSet());
    }

    @Override
    public String[] filter(String[] words, String[] candidates) {
        long count = Arrays.stream(words)
                .filter(activationWords::contains)
                .count();

        // if it is the first time we see activation word, then we should return all candidates.
        // else we should filter out all nodes aliases.
        if (count <= 1) {
            return candidates;
        } else {
            return Arrays.stream(candidates)
                    .filter(it -> {
                        NodesAlias a = NodesAlias.parse(it);
                        return a == null;
                    }).toArray(String[]::new);
        }
    }
}
