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
import java.util.stream.Collectors;
import org.apache.ignite.internal.cli.commands.Options;

/** Filters out short option names from candidates. */
public class ShortOptionsFilter implements CompleterFilter {

    private final Set<String> shortOptionNames = shortOptionNames();

    private static Set<String> shortOptionNames() {
        return Arrays.stream(Options.values())
                .filter(it -> !it.fullName().equals(it.shortName()))
                .map(Options::shortName)
                .collect(Collectors.toSet());
    }

    /** Filters candidates. */
    @Override
    public String[] filter(String[] ignored, String[] candidates) {
        return Arrays.stream(candidates)
                .filter(it -> !shortOptionNames.contains(it))
                .toArray(String[]::new);
    }
}
