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

package org.apache.ignite.internal.cli.core.repl.completer.hocon;

import static org.apache.ignite.internal.cli.util.ArrayUtils.findLastNotEmptyWord;

import com.typesafe.config.Config;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.internal.cli.core.repl.completer.DynamicCompleter;

/** Completes typed words with hocon schema keys. */
public class HoconDynamicCompleter implements DynamicCompleter {

    private final Config config;

    /** Stores all keys that exist in the given config. Strings are stored in format: "root.subkey.subsubkey". */
    private final Set<String> leafs;

    /** List of all possible completions. */
    private final List<String> completions;

    /** Default constructor that creates an instance from a given set of postfixes that trigger the completion. */
    public HoconDynamicCompleter(Config config) {
        this.config = config;
        this.leafs = config.entrySet().stream().map(Entry::getKey).collect(Collectors.toSet());
        this.completions = this.compile();
    }

    private List<String> compile() {
        ArrayList<String> result = new ArrayList<>();

        String rootPrefix = "";
        walkAndAdd(rootPrefix, config.root().keySet(), result);

        return result;
    }

    /**
     * Completes the given typed words with the config keys that are in the same level as the last typed words.
     *
     * <p>Example: given typed words ["cluster", "config", "show", "a"], The last word is "a", only root config values will be suggested to
     * autocomplete: "aimem", "aipersist". If user hits "aimem" and types dot "." then only subkeys of "aimem." will be suggested:
     * "aimem.pageSizeBytes", "aimem.regions".
     */
    @Override
    public List<String> complete(String[] words) {
        String lastWord = findLastNotEmptyWord(words);

        if (words[words.length - 1].isEmpty()) {
            // roots
            return completions.stream().filter(s -> s.split("\\.").length == 1).collect(Collectors.toList());
        }

        int deepLevel = lastWord.endsWith(".")
                ? lastWord.split("\\.").length + 1
                : lastWord.split("\\.").length;

        return completions.stream()
                .filter(s -> s.startsWith(lastWord) && deepLevel == s.split("\\.").length)
                .collect(Collectors.toList());
    }

    private void walkAndAdd(String keyPrefix, Set<String> keySet, List<String> result) {
        keySet.forEach(key -> {
            if (!leafs.contains(keyPrefix + key)) {
                Set<String> nextKeySet = config.getConfig(keyPrefix + key).root().keySet();
                walkAndAdd(keyPrefix + key + ".", nextKeySet, result);
            }
            result.add(keyPrefix + key);
        });
    }
}
