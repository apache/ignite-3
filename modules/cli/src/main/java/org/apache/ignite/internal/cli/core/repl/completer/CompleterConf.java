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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.internal.cli.commands.Options;
import org.apache.ignite.internal.cli.core.repl.completer.filter.CompleterFilter;

/**
 * Configuration for dynamic completer. It declares for what command and option the completer could be applied.
 * Also supports explicit declaration of options after which the completer should not be applied.
  */
public class CompleterConf {

    private final List<String[]> commands;

    private final Set<String> enableOptions;

    private final Set<String> disableOptions;

    private final boolean exclusiveEnableOptions;

    private final boolean isSinglePositionalParameter;

    private final CompleterFilter filter;

    private CompleterConf(List<String[]> commands,
            Set<String> enableOptions,
            Set<String> disableOptions,
            boolean exclusiveEnableOptions,
            boolean isSinglePositionalParameter,
            CompleterFilter filter) {
        if (commands == null) {
            throw new IllegalArgumentException("commands must not be null");
        }

        this.commands = commands;
        this.enableOptions = enableOptions;
        this.disableOptions = disableOptions;
        this.exclusiveEnableOptions = exclusiveEnableOptions;
        this.isSinglePositionalParameter = isSinglePositionalParameter;
        this.filter = filter;
    }

    public static CompleterConf everytime() {
        return builder().build();
    }

    public static CompleterConf forCommand(String... words) {
        return builder().command(words).build();
    }

    public static CompleterConfBuilder builder() {
        return new CompleterConfBuilder();
    }

    public List<String[]> commands() {
        return commands;
    }

    public Set<String> enableOptions() {
        return enableOptions;
    }

    public Set<String> disableOptions() {
        return disableOptions;
    }

    public boolean commandSpecific() {
        return !commands.isEmpty();
    }

    public boolean hasEnableOptions() {
        return enableOptions != null;
    }

    public boolean hasDisableOptions() {
        return disableOptions != null;
    }

    public boolean isExclusiveEnableOptions() {
        return exclusiveEnableOptions;
    }

    public boolean isSinglePositionalParameter() {
        return isSinglePositionalParameter;
    }

    public boolean hasFilter() {
        return filter != null;
    }

    public CompleterFilter getFilter() {
        return filter;
    }

    /** Builder for {@link CompleterConf}. */
    public static class CompleterConfBuilder {
        private final List<String[]> command = new ArrayList<>();

        private Set<String> enableOptions;

        private Set<String> disableOptions;

        private boolean exclusiveEnableOptions;

        private boolean isSinglePositionalParameter;

        private CompleterFilter filter;

        private CompleterConfBuilder() {
        }

        /** Setup commands after those the completer should be called. */
        public CompleterConfBuilder command(String... words) {
            this.command.add(Arrays.copyOf(words, words.length));
            return this;
        }

        /** Setup options after those the completer should be called. */
        public CompleterConfBuilder enableOptions(Options... enableOptions) {
            this.enableOptions = Stream.of(enableOptions)
                    .flatMap(opt -> Stream.of(opt.fullName(), opt.shortName()))
                    .collect(Collectors.toSet());
            return this;
        }

        /** Setup options after those the completer should be called. */
        public CompleterConfBuilder enableOptions(String... enableOptions) {
            this.enableOptions = Set.of(enableOptions);
            return this;
        }

        /** Setup options after those the completer should NOT be called. */
        public CompleterConfBuilder disableOptions(String... disableOptions) {
            this.disableOptions = Set.of(disableOptions);
            return this;
        }

        /** Setup single positional parameter completer flag. It means that the completer should be applied for the parameter only once. */
        public CompleterConfBuilder singlePositionalParameter() {
            this.isSinglePositionalParameter = true;
            return this;
        }

        /** Setup filter for candidates. */
        public CompleterConfBuilder filter(CompleterFilter filter) {
            this.filter = filter;
            return this;
        }

        /**
         * If called than all enable options of current configuration will become disable options for all other completers. For example,
         * --name should be completed by only one completer.
         */
        public CompleterConfBuilder exclusiveEnableOptions() {
            this.exclusiveEnableOptions = true;
            return this;
        }

        public CompleterConf build() {
            return new CompleterConf(command, enableOptions, disableOptions, exclusiveEnableOptions, isSinglePositionalParameter, filter);
        }
    }
}
