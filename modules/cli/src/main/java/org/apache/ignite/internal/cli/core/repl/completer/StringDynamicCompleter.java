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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.internal.cli.core.repl.completer.DynamicCompleter;

/**
 * Completes typed words with provided list of strings.
 */
public class StringDynamicCompleter implements DynamicCompleter {

    /** Values that will be suggested. */
    private final Set<String> values;

    /** Default constructor. */
    public StringDynamicCompleter(Set<String> values) {
        this.values = values;
    }

    @Override
    public List<String> complete(String[] words) {
        if (words[words.length - 1].isBlank()) {
            return new ArrayList<>(values);
        }

        String lastWord = findLastNotEmptyWord(words);
        return values.stream()
                .filter(it -> it.startsWith(lastWord))
                .collect(Collectors.toList());
    }
}
