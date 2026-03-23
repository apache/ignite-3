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

package org.apache.ignite.internal.configuration.compatibility.framework;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.List;
import org.apache.ignite.internal.configuration.compatibility.framework.ConfigNode.Node;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;

/**
 * Helper class provides method for configuration tree objects serialization/deserialization.
 */
public class ConfigNodeSerializer {
    /**
     * Writes a list of trees to the output.
     */
    public static void writeAsJson(List<ConfigNode> nodes, IgniteDataOutput out) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();

        objectMapper.writerFor(ConfigNode.class).writeValues(out).writeAll(nodes);
    }

    /**
     * Reads all the trees from the input.
     */
    public static List<ConfigNode> readAsJson(IgniteDataInput in) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        // To support {Object val} where val can be both long and int, deserialize always ints as longs.
        objectMapper.configure(DeserializationFeature.USE_LONG_FOR_INTS, true);

        MappingIterator<ConfigNode> objectMappingIterator = objectMapper.readerFor(ConfigNode.class).readValues(in);

        List<ConfigNode> configNodes = objectMappingIterator.readAll();

        // Init roots, then restore parent links.
        configNodes.forEach(root -> root.init(null));
        configNodes.forEach(ConfigNodeSerializer::restoreParentLinks);

        return configNodes;
    }

    /*
     * Recursively set parent links for all children
     */
    private static void restoreParentLinks(ConfigNode node) {
        for (Node child : node.children().values()) {
            if (child.isPolymorphic()) {
                for (ConfigNode instanceNode : child.nodes().values()) {
                    instanceNode.init(node);
                    restoreParentLinks(instanceNode);
                }
            } else {
                child.node().init(node);
                restoreParentLinks(child.node());
            }
        }
    }
}
