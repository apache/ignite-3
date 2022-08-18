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

import static org.apache.ignite.cli.commands.OptionsConstants.CLUSTER_URL_OPTION;
import static org.apache.ignite.cli.commands.OptionsConstants.NODE_URL_OPTION;

import jakarta.inject.Singleton;
import java.util.Arrays;
import org.apache.ignite.cli.core.repl.Session;

/**
 * Filters completions according to the current session state.
 */
@Singleton
public class DynamicCompleterFilter implements CompleterFilter {
    private final Session session;

    public DynamicCompleterFilter(Session session) {
        this.session = session;
    }

    private static boolean optionTyped(String[] words) {
        String lastWord = words[words.length - 1];
        // user wants to complete an option
        return lastWord.startsWith("-");
    }

    @Override
    public String[] filter(String[] words, String[] candidates) {
        return Arrays.stream(candidates)
                .filter(candidate -> filterClusterUrl(words, candidate))
                .filter(candidate -> filterHelp(words, candidate))
                .toArray(String[]::new);
    }

    private boolean filterHelp(String[] words, String candidate) {
        if (optionTyped(words)) {
            return true;
        }

        return !(candidate.equals("--help") || candidate.equals("-h"));
    }

    private boolean filterClusterUrl(String[] words, String candidate) {
        if (optionTyped(words)) {
            return true;
        }

        return !session.isConnectedToNode() || (!candidate.equals(CLUSTER_URL_OPTION) && !candidate.equals(NODE_URL_OPTION));
    }
}
