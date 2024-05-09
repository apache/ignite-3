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

import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.jetbrains.annotations.Nullable;

/**
 * Registry that holds all available dynamic completers.
 */
@Singleton
public class DynamicCompleterRegistry {

    private final List<CompletionStrategy> completionStrategiesList;

    private final DynamicCompletionInsider dynamicCompletionInsider;

    public DynamicCompleterRegistry() {
        completionStrategiesList = new ArrayList<>();
        dynamicCompletionInsider = new DynamicCompletionInsider();
    }

    /** Returns the list of dynamic completers that can provide completions for given typed words. */
    public List<DynamicCompleter> findCompleters(String[] words) {
        return completionStrategiesList.stream()
                .filter(strategy -> strategy.canBeApplied(words))
                .map(strategy -> strategy.completer(words))
                .collect(Collectors.toList());
    }

    /** Registers dynamic completer that can be found by given predicate. */
    public void register(CompleterConf conf, DynamicCompleterFactory factory) {
        if (conf.isExclusiveEnableOptions()) {
            // add disable option for all strategies because current configuration has exclusive enable option
            completionStrategiesList.forEach(strategy -> strategy.exclusiveDisableOptions.addAll(conf.enableOptions()));
        }

        Set<String> exclusiveDisableOptions = completionStrategiesList.stream()
                .filter(strategy -> strategy.conf.isExclusiveEnableOptions())
                .flatMap(strategy -> strategy.conf.enableOptions().stream())
                .collect(Collectors.toSet());

        CompletionStrategy strategy = new CompletionStrategy(conf, factory);
        strategy.exclusiveDisableOptions.addAll(exclusiveDisableOptions);
        completionStrategiesList.add(strategy);
    }

    private class CompletionStrategy {
        private final CompleterConf conf;

        private final DynamicCompleterFactory factory;

        private final Set<String> exclusiveDisableOptions = new HashSet<>();

        private CompletionStrategy(CompleterConf conf, DynamicCompleterFactory factory) {
            this.conf = conf;
            this.factory = factory;
        }

        private boolean samePrefix(String[] words, String[] prefixWords) {
            if (words.length < prefixWords.length) {
                return false;
            }
            for (int i = 0; i < prefixWords.length; i++) {
                if (!words[i].equals(prefixWords[i])) {
                    return false;
                }
            }
            return true;
        }

        boolean canBeApplied(String[] words) {
            // If it is a single positional parameter completer, it can be applied only once
            if (conf.isSinglePositionalParameter()) {
                boolean alreadyHasPositionalArgument = dynamicCompletionInsider.wasPositionalParameterCompleted(words);
                if (alreadyHasPositionalArgument) {
                    return false;
                }
            }

            // empty command means can be applied to all words
            if (!conf.commandSpecific()) {
                return canBeAppliedCommandMatch(words);
            }

            Optional<String[]> commandsMatch = conf.commands().stream().filter(command -> samePrefix(words, command)).findFirst();
            return commandsMatch.isPresent() && canBeAppliedCommandMatch(words);
        }

        private boolean canBeAppliedCommandMatch(String[] words) {
            String cursorWord = words[words.length - 1];
            String lastNotEmptyWord = findLastNotEmptyWord(words);
            String preLastNotEmptyWord = findLastNotEmptyWordBeforeWordFromEnd(words, lastNotEmptyWord);

            if (shouldBeDisabled(preLastNotEmptyWord, lastNotEmptyWord, cursorWord)) {
                return false;
            }

            if (conf.hasEnableOptions()) {
                if (cursorWord.equals(lastNotEmptyWord)) {
                    return conf.enableOptions().contains(lastNotEmptyWord) // command subcommand --enable-option
                            || conf.enableOptions().contains(preLastNotEmptyWord); // command subcommand --enable-option lastWord
                } else {
                    return conf.enableOptions().contains(lastNotEmptyWord); // command subcommand --enable-option <space>
                }
            }

            if (conf.hasDisableOptions()) {
                if (cursorWord.equals(lastNotEmptyWord)) {
                    return !conf.disableOptions().contains(lastNotEmptyWord)
                             && !conf.disableOptions().contains(preLastNotEmptyWord);
                } else {
                    return !conf.disableOptions().contains(lastNotEmptyWord);
                }
            }

            return true;
        }

        DynamicCompleter completer(String[] words) {
            if (conf.hasFilter()) {
                return input -> {
                    List<String> candidates = factory.getDynamicCompleter(input).complete(input);
                    return List.of(conf.getFilter().filter(input, candidates.toArray(new String[0])));
                };
            } else {
                return factory.getDynamicCompleter(words);
            }
        }

        private boolean shouldBeDisabled(String preLastNotEmptyWord, String lastNotEmptyWord, String cursorWord) {
            boolean enableOverridesDisable = intersect(conf.enableOptions(), exclusiveDisableOptions);
            if (enableOverridesDisable) {
                return false;
            }

            if (cursorWord.equals(lastNotEmptyWord)) {
                if (exclusiveDisableOptions.contains(lastNotEmptyWord) || exclusiveDisableOptions.contains(preLastNotEmptyWord)) {
                    return true;
                }
            } else if (exclusiveDisableOptions.contains(lastNotEmptyWord)) {
                return true;
            }

            return false;
        }

        private boolean intersect(@Nullable Set<String> a, @Nullable Set<String> b) {
            if (a == null || b == null) {
                return false;
            }

            for (String s : a) {
                if (b.contains(s)) {
                    return true;
                }
            }

            return false;
        }
    }
}
