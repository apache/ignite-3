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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Model.ITypeInfo;

/** Filters out non-repeatable options from candidates. */
public class NonRepeatableOptionsFilter implements CompleterFilter {

    private final CommandSpec topCommandSpec;

    private final Map<CommandSpec, List<CompleterFilter>> additionalFilters;

    /**
     * Constructor.
     *
     * @param topCommandSpec Top command spec.
     */
    public NonRepeatableOptionsFilter(CommandSpec topCommandSpec) {
        this(topCommandSpec, List.of());
    }

    /**
     * Constructor.
     *
     * @param spec Top command spec.
     * @param additionalFilters Additional filters.
     */
    public NonRepeatableOptionsFilter(CommandSpec spec, List<CommandCompleterFilter> additionalFilters) {
        this.topCommandSpec = spec;
        this.additionalFilters = additionalFilters.stream()
                .collect(Collectors.groupingBy(CommandCompleterFilter::commandSpec, Collectors.toList()));
    }

    /** Filters candidates. */
    @Override
    public String[] filter(String[] words, String[] candidates) {
        CommandSpec commandSpec = findCommandSpec(words);
        Map<String, ITypeInfo> optionTypes = commandSpec.options().stream()
                .flatMap(it -> Arrays.stream(it.names()).map(name -> new OptionInfo(name, it.typeInfo())))
                .collect(Collectors.toMap(OptionInfo::getName, OptionInfo::getType));
        Set<String> shouldBeExcludedFromCandidates = Arrays.stream(words)
                .filter(optionTypes::containsKey)
                .filter(it -> !optionTypes.get(it).isMultiValue())
                .collect(Collectors.toSet());
        String[] filteredCandidates = Arrays.stream(candidates)
                .filter(it -> !shouldBeExcludedFromCandidates.contains(it))
                .toArray(String[]::new);

        return applyAdditionalFilters(commandSpec, words, filteredCandidates);
    }

    private String[] applyAdditionalFilters(CommandSpec commandSpec, String[] words, String[] candidates) {
        List<CompleterFilter> applicableFilters = additionalFilters.getOrDefault(commandSpec, List.of());
        String[] tmpCandidates = candidates;
        for (CompleterFilter filter : applicableFilters) {
            tmpCandidates = filter.filter(words, tmpCandidates);
        }
        return tmpCandidates;
    }

    private CommandSpec findCommandSpec(String[] words) {
        int cursor = 0;
        CommandSpec commandSpec = topCommandSpec;
        while (cursor < words.length && commandSpec.subcommands().containsKey(words[cursor])) {
            commandSpec = commandSpec.subcommands().get(words[cursor]).getCommandSpec();
            cursor++;
        }
        return commandSpec;
    }

    private static class OptionInfo {
        private final String name;
        private final ITypeInfo type;

        private OptionInfo(String name, ITypeInfo type) {
            this.name = name;
            this.type = type;
        }

        private String getName() {
            return name;
        }

        private ITypeInfo getType() {
            return type;
        }
    }
}
