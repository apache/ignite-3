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

package org.apache.ignite.internal.cli.commands.cluster;

import static org.apache.ignite.internal.cli.commands.Options.Constants.CLUSTER_NAME_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.CLUSTER_NAME_OPTION_DESC;

import java.util.Arrays;
import java.util.stream.Collectors;
import org.apache.ignite.internal.cli.util.ConfigurationArgsParseException;
import org.jetbrains.annotations.Nullable;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * Mixin class for cluster name option in REPL mode.
 */
public class ClusterNameMixin {
    @Parameters(arity = "0..1")
    private String[] args;

    /** Cluster name option. */
    @Option(
            names = CLUSTER_NAME_OPTION,
            description = CLUSTER_NAME_OPTION_DESC
    )
    private String name;

    private String nameFromArgs() {
        return Arrays.stream(args).map(ClusterNameMixin::unquote).collect(Collectors.joining(" "));
    }

    public boolean hasContent() {
        return args != null && args.length > 0;
    }

    /**
     * Gets cluster name from the command line.
     *
     * @return cluster name
     */
    @Nullable
    public String getName() {
        if (name == null) {
            if (!hasContent()) {
                throw new ConfigurationArgsParseException("Failed to parse name.");
            }

            return nameFromArgs();
        }

        return name;
    }

    private static String unquote(String rawString) {
        if (isQuoted(rawString, '"')
                || isQuoted(rawString, '\'')
                || isQuoted(rawString, '`')
        ) {
            return rawString.substring(1, rawString.length() - 1);
        }
        return rawString;
    }

    private static boolean isQuoted(String string, char quoteChar) {
        return string.charAt(0) == quoteChar && string.charAt(string.length() - 1) == quoteChar;
    }
}
