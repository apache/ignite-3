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

import static org.apache.ignite.internal.cli.commands.Options.Constants.CONFIG_FORMAT_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.CONFIG_FORMAT_OPTION_DESC;
import static org.apache.ignite.internal.cli.commands.treesitter.parser.Parser.isTreeSitterParserAvailable;
import static picocli.CommandLine.Help.Visibility.ALWAYS;

import jakarta.inject.Inject;
import org.apache.ignite.internal.cli.call.configuration.JsonString;
import org.apache.ignite.internal.cli.config.CliConfigKeys;
import org.apache.ignite.internal.cli.config.ConfigManagerProvider;
import org.apache.ignite.internal.cli.core.decorator.Decorator;
import org.apache.ignite.internal.cli.core.decorator.TerminalOutput;
import org.apache.ignite.internal.cli.decorators.HoconDecorator;
import org.apache.ignite.internal.cli.decorators.JsonDecorator;
import picocli.CommandLine.Option;

/** Mixin for config format option. */
public class FormatMixin {
    private enum Format {
        JSON,
        HOCON
    }

    @Option(
            names = CONFIG_FORMAT_OPTION,
            description = CONFIG_FORMAT_OPTION_DESC,
            defaultValue = "HOCON",
            showDefaultValue = ALWAYS
    )
    private Format format;

    @Inject
    private ConfigManagerProvider configManagerProvider;

    /**
     * Gets the decorator corresponding to the format.
     *
     * @return Decorator.
     */
    public Decorator<JsonString, TerminalOutput> decorator() {
        if (format == Format.JSON) {
            return new JsonDecorator(isHighlightEnabled());
        }
        // HOCON is default
        return new HoconDecorator(isHighlightEnabled());
    }

    private boolean isHighlightEnabled() {
        return isTreeSitterParserAvailable()
                && Boolean.parseBoolean(configManagerProvider.get().getCurrentProperty(CliConfigKeys.SYNTAX_HIGHLIGHTING.value()));
    }
}
