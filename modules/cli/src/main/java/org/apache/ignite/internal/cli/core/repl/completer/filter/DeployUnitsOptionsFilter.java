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

import java.util.Arrays;
import java.util.Set;
import org.apache.ignite.internal.cli.commands.Options;
import org.apache.ignite.internal.cli.commands.cluster.unit.NodesAlias;
import org.apache.ignite.internal.cli.util.ArrayUtils;

/**
 * Deployment nodes completer filter. This filter removes activation word from candidates if nodes alias is present.
 */
public class DeployUnitsOptionsFilter implements CompleterFilter {
    private final String[] activationWords = {"cluster", "unit", "deploy"};
    private final Set<String> options = Options.UNIT_NODES.names();

    @Override
    public String[] filter(String[] words, String[] candidates) {
        if (!ArrayUtils.firstStartsWithSecond(words, activationWords)) {
            return candidates;
        }

        // find activation word.
        int cursor = 0;
        boolean isActivationWordFound = false;
        while (cursor < words.length) {
            if (options.contains(words[cursor])) {
                isActivationWordFound = true;
                break;
            }
            cursor++;
        }

        if (isActivationWordFound) {
            // skip activation word.
            cursor++;

            boolean aliasFound = false;
            // nodes alias is present?
            while (cursor < words.length && !words[cursor].startsWith("-")) {
                NodesAlias alias = NodesAlias.parse(words[cursor]);
                if (alias != null) {
                    aliasFound = true;
                    break;
                }
                cursor++;
            }

            // remove options from candidates if nodes alias is present.
            if (aliasFound) {
                return Arrays.stream(candidates)
                        .filter(it -> !options.contains(it))
                        .toArray(String[]::new);
            }
        }

        return candidates;
    }
}
