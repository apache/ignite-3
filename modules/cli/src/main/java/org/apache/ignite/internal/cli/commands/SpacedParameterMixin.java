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

package org.apache.ignite.internal.cli.commands;

import static org.apache.ignite.internal.cli.commands.Options.Constants.CONFIG_UPDATE_FILE_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.CONFIG_UPDATE_FILE_OPTION_DESC;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import java.io.File;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.stream.Collectors;
import org.apache.ignite.internal.cli.core.exception.IgniteCliException;
import org.apache.ignite.internal.cli.util.ConfigurationArgsParseException;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * A mixin for a parameter that combines all parameters into one.
 * With this mixin, we allow the user to specify one parameter with spaces without quotation.
 */
public class SpacedParameterMixin {
    @Parameters(arity = "0..1")
    private String[] args;

    /** Configuration from file that will be updated. */
    @Option(names = CONFIG_UPDATE_FILE_OPTION, description = CONFIG_UPDATE_FILE_OPTION_DESC)
    public File configFile;

    private String configUpdateFromArgs() {
        return Arrays.stream(args).map(SpacedParameterMixin::unquote).collect(Collectors.joining(" "));
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

    public boolean hasContent() {
        return args != null && args.length > 0;
    }

    /**
     * Merge config from file and from CLI if both provided, config from CLI overrides config from file.
     * Otherwise, returns provided non-null config.
     *
     * @return String representation of the config.
     */
    public String formUpdateConfig() {
        if (configFile == null && !hasContent()) {
            throw new ConfigurationArgsParseException("Failed to parse config content. "
                    + "Please, specify config file or provide config content directly.");
        }

        if (configFile == null) {
            return configUpdateFromArgs();
        } else {
            if (!Files.exists(configFile.toPath())) {
                throw new IgniteCliException("File [" + configFile.getAbsolutePath() + "] not found");
            }

            Config result = ConfigFactory.parseFile(configFile);

            if (hasContent()) {
                Config configFromArgs = ConfigFactory.parseString(configUpdateFromArgs());

                result = configFromArgs.withFallback(result);
            }

            return result.resolve().root().render(ConfigRenderOptions.concise().setFormatted(true).setJson(false));
        }
    }
}
