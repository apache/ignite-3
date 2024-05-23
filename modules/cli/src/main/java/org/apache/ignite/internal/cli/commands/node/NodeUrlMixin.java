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

package org.apache.ignite.internal.cli.commands.node;

import static org.apache.ignite.internal.cli.commands.Options.Constants.NODE_NAME_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.NODE_NAME_OPTION_DESC;
import static org.apache.ignite.internal.cli.commands.Options.Constants.NODE_NAME_OPTION_SHORT;
import static org.apache.ignite.internal.cli.commands.Options.Constants.NODE_URL_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.NODE_URL_OPTION_DESC;

import jakarta.inject.Inject;
import java.net.URL;
import org.apache.ignite.internal.cli.core.converters.UrlConverter;
import org.apache.ignite.internal.cli.core.exception.IgniteCliException;
import org.apache.ignite.internal.cli.core.repl.registry.NodeNameRegistry;
import org.jetbrains.annotations.Nullable;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

/**
 * Mixin class for node URL option.
 */
public class NodeUrlMixin {

    @ArgGroup
    private Options options;

    @Inject
    private NodeNameRegistry nodeNameRegistry;

    private static class Options {

        /**
         * Node URL option.
         */
        @Option(names = NODE_URL_OPTION, description = NODE_URL_OPTION_DESC, converter = UrlConverter.class)
        private URL nodeUrl;

        /**
         * Node name option.
         */
        @Option(names = {NODE_NAME_OPTION_SHORT, NODE_NAME_OPTION}, description = NODE_NAME_OPTION_DESC)
        private String nodeName;
    }

    /**
     * Returns node URL.
     *
     * @return Node URL
     */
    @Nullable
    public String getNodeUrl() {
        if (options != null) {
            if (options.nodeUrl != null) {
                return options.nodeUrl.toString();
            }
            if (options.nodeName != null) {
                return nodeNameRegistry.nodeUrlByName(options.nodeName)
                        .orElseThrow(() -> new IgniteCliException("Node " + options.nodeName
                                + " not found. Provide a valid name or use a URL"));
            }
        }
        return null;
    }
}
