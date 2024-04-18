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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.ignite.internal.cli.call.configuration.JsonString;
import org.apache.ignite.internal.cli.commands.treesitter.highlighter.JsonAnsiHighlighter;
import org.apache.ignite.internal.cli.core.decorator.Decorator;
import org.apache.ignite.internal.cli.core.decorator.TerminalOutput;

/**
 * Pretty json decorator.
 */
public class JsonDecorator implements Decorator<JsonString, TerminalOutput> {

    private final boolean highlight;

    public JsonDecorator(boolean highlight) {
        this.highlight = highlight;
    }

    /** {@inheritDoc} */
    @Override
    public TerminalOutput decorate(JsonString json) {
        ObjectMapper mapper = new ObjectMapper();
        return () -> {
            try {
                String text = mapper.writerWithDefaultPrettyPrinter()
                        .writeValueAsString(mapper.readValue(json.getValue(), JsonNode.class));

                return highlight ? JsonAnsiHighlighter.highlight(text) : text;

            } catch (JsonProcessingException e) {
                return json.getValue(); // no-op
            }
        };
    }
}
