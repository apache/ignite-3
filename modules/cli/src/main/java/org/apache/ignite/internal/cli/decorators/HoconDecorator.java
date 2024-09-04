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

package org.apache.ignite.internal.cli.decorators;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import org.apache.ignite.internal.cli.call.configuration.JsonString;
import org.apache.ignite.internal.cli.commands.treesitter.highlighter.HoconAnsiHighlighter;
import org.apache.ignite.internal.cli.core.decorator.Decorator;
import org.apache.ignite.internal.cli.core.decorator.TerminalOutput;

/**
 * Pretty hocon decorator.
 */
public class HoconDecorator implements Decorator<JsonString, TerminalOutput> {

    private final boolean highlight;

    public HoconDecorator(boolean highlight) {
        this.highlight = highlight;
    }

    /** {@inheritDoc} */
    @Override
    public TerminalOutput decorate(JsonString json) {
        return () -> {
            try {
                Config config = ConfigFactory.parseString(json.getValue());
                String text = config.root().render(ConfigRenderOptions.concise().setFormatted(true).setJson(false));
                if (text.endsWith("\n")) {
                    // Config renders with the trailing line separator, but we also add a separator in the handleResult.
                    text = text.substring(0, text.length() - 1);
                }

                return highlight ? HoconAnsiHighlighter.highlight(text) : text;

            } catch (ConfigException.Parse e) {
                return json.getValue(); // no-op
            }
        };
    }
}
