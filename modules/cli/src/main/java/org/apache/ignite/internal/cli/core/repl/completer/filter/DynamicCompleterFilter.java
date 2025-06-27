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

import static org.apache.ignite.internal.cli.commands.Options.Constants.HELP_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.HELP_OPTION_SHORT;
import static org.apache.ignite.internal.cli.commands.Options.Constants.NODE_URL_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.PROFILE_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.VERBOSE_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.VERBOSE_OPTION_SHORT;

import jakarta.inject.Singleton;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.internal.cli.core.repl.Session;

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
        if (optionTyped(words)) {
            return candidates;
        }

        List<String> notOptionsCandidates = Arrays.stream(candidates)
                .filter(candidate -> !candidate.startsWith("-"))
                .collect(Collectors.toList());

        if (!notOptionsCandidates.isEmpty()) {
            return notOptionsCandidates.toArray(String[]::new);
        }

        return Arrays.stream(candidates)
                .filter(candidate -> filterClusterUrl(words, candidate))
                .filter(this::filterCommonOptions)
                .toArray(String[]::new);
    }

    private boolean filterCommonOptions(String candidate) {
        return !(HELP_OPTION.equals(candidate)
                || HELP_OPTION_SHORT.equals(candidate)
                || PROFILE_OPTION.equals(candidate)
                || VERBOSE_OPTION_SHORT.equals(candidate)
                || VERBOSE_OPTION.equals(candidate));
    }

    private boolean filterClusterUrl(String[] words, String candidate) {
        return optionTyped(words)
                || session.info() == null
                || (!candidate.equals(NODE_URL_OPTION));
    }
}
