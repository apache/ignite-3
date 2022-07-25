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

import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Registry that holds all available dynamic completers.
 */
@Singleton
public class DynamicCompleterRegistry {

    private final List<CompletionStrategy> completionStrategiesList = new ArrayList<>();

    /** Returns the list of dynamic completers that can provide completions for given typed words. */
    public List<DynamicCompleter> findCompleters(String[] words) {
        return completionStrategiesList.stream()
                .filter(strategy -> strategy.canBeApplied(words))
                .map(CompletionStrategy::completer)
                .collect(Collectors.toList());
    }

    /** Registers dynamic completer that can be found by given predicate. */
    public void register(Predicate<String[]> predicate, DynamicCompleter completer) {
        completionStrategiesList.add(new CompletionStrategy(predicate, completer));
    }

    /** Registers dynamic completer that can be found by given prefix. */
    public void register(String[] prefixWords, DynamicCompleter completer) {
        register((String[] words) -> samePrefix(words, prefixWords), completer);
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

    private static class CompletionStrategy {
        private final Predicate<String[]> predicate;
        private final DynamicCompleter completer;

        private CompletionStrategy(Predicate<String[]> predicate, DynamicCompleter completer) {
            this.predicate = predicate;
            this.completer = completer;
        }

        boolean canBeApplied(String[] words) {
            return predicate.test(words);
        }

        DynamicCompleter completer() {
            return completer;
        }
    }
}
