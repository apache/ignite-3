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

package org.apache.ignite.internal.cli.core.converters;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.regex.Pattern;
import org.apache.ignite.internal.cli.core.repl.registry.impl.NodeNameRegistryImpl;
import org.apache.ignite.internal.cli.commands.node.NodeNameOrUrl;
import picocli.CommandLine;
import picocli.CommandLine.TypeConversionException;

/** Converter for {@link NodeNameOrUrl}. */
public class NodeNameOrUrlConverter implements CommandLine.ITypeConverter<NodeNameOrUrl> {

    private static final Pattern URL_PATTERN = Pattern.compile("^.*[/:].*");

    private final NodeNameRegistryImpl nodeNameRegistry;

    public NodeNameOrUrlConverter(NodeNameRegistryImpl nodeNameRegistry) {
        this.nodeNameRegistry = nodeNameRegistry;
    }

    private static URL stringToUrl(String str) {
        try {
            return new URL(str);
        } catch (MalformedURLException e) {
            throw new TypeConversionException("Invalid URL '" + str + "' (" + e.getMessage() + ")");
        }
    }

    @Override
    public NodeNameOrUrl convert(String input) throws Exception {
        boolean isUrl = URL_PATTERN.matcher(input).matches();
        if (isUrl) {
            return new NodeNameOrUrl(stringToUrl(input));
        } else {
            return new NodeNameOrUrl(findNodeUrlByNodeName(input));
        }
    }

    private URL findNodeUrlByNodeName(String name) {
        return nodeNameRegistry.nodeUrlByName(name)
                .orElseThrow(() -> new TypeConversionException("Node " + name + " not found. Provide valid name or use URL"));
    }
}
