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

import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;
import org.apache.ignite.internal.util.io.IgniteDataOutput.ObjectWriter;

/**
 * Helper class provides method for configuration tree objects serialization/deserialization.
 */
public class ConfigNodeSerializer {
    private static final ObjectWriter<String> UTF_WRITER = (k, o) -> o.writeUTF(k);

    /**
     * Writes a tree to the output.
     */
    private static void write(ConfigNode node, IgniteDataOutput out) throws IOException {
        out.writeUTF(node.name());
        out.writeUTF(node.type());
        out.writeBoolean(node.isValue());

        if (node.isValue()) {
            out.writeMap(((ValueNode) node).additionalAttributes(), UTF_WRITER, UTF_WRITER);
        } else {
            // Write children
            List<ConfigNode> children = ((InnerNode) node).children();
            out.writeVarInt(children.size());
            for (ConfigNode child : children) {
                write(child, out);
            }
        }
    }

    /**
     * Reads a tree from the input.
     */
    public static ConfigNode read(IgniteDataInput in) throws IOException {
        return read(in, null);
    }

    /**
     * Reads a single ConfigNode tree from the input.
     */
    private static ConfigNode read(IgniteDataInput in, ConfigNode parent) throws IOException {
        String name = in.readUTF();
        String type = in.readUTF();
        boolean isValue = in.readBoolean();

        if (isValue) {
            HashMap<String, String> properties = in.readMap(HashMap::new, IgniteDataInput::readUTF, DataInput::readUTF);

            return new ValueNode(name, type, parent, properties);
        } else {
            int childrenSize = in.readVarIntAsInt();
            List<ConfigNode> children = new ArrayList<>(childrenSize);
            ConfigNode innerNode = new InnerNode(name, type, parent, children);
            for (int i = 0; i < childrenSize; i++) {
                children.add(read(in, innerNode));
            }
            return innerNode;
        }
    }

    /**
     * Writes a list of trees to the output.
     */
    public static void writeAll(List<ConfigNode> nodes, IgniteDataOutput out) throws IOException {
        out.writeVarInt(nodes.size());
        for (ConfigNode node : nodes) {
            write(node, out);
        }
    }

    /**
     * Reads all the trees from the input.
     */
    public static List<ConfigNode> readAll(IgniteDataInput in) throws IOException {
        int count = in.readVarIntAsInt();
        List<ConfigNode> nodes = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            nodes.add(read(in));
        }
        return nodes;
    }
}
