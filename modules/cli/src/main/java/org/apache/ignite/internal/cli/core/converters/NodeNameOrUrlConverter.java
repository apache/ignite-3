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
import org.apache.ignite.internal.cli.NodeNameRegistry;
import org.apache.ignite.internal.cli.commands.node.NodeUrl;
import picocli.CommandLine;
import picocli.CommandLine.TypeConversionException;

/** Converter for {@link NodeUrl}. */
public class NodeNameOrUrlConverter implements CommandLine.ITypeConverter<NodeUrl> {

    private final NodeNameRegistry nodeNameRegistry;

    public NodeNameOrUrlConverter(NodeNameRegistry nodeNameRegistry) {
        this.nodeNameRegistry = nodeNameRegistry;
    }

    @Override
    public NodeUrl convert(String input) throws Exception {
        try {
            return new NodeUrl(nodeUrl(input));
        } catch (MalformedURLException e) {
            String message = String.format("Node [%s] not found. Provide correct node url or node name", input);
            throw new TypeConversionException(message);
        }
    }

    private URL nodeUrl(String input) throws MalformedURLException {
        try {
            return new URL(input);
        } catch (MalformedURLException e) {
            return new URL(nodeNameRegistry.getNodeUrl(input));
        }
    }
}
