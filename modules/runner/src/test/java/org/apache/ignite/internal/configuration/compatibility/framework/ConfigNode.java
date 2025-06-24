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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Collection;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.annotation.ConfigurationType;

/**
 * Tree node that describes a configuration tree item.
 */
public class ConfigNode {
    private ConfigNode parent;
    @JsonProperty
    private Map<String, String> attributes;
    @JsonProperty
    private Map<String, ConfigNode> childNodeMap = new LinkedHashMap<>();
    @JsonProperty
    private boolean deprecated;
    @JsonProperty
    private boolean value;

    ConfigNode() {
        // Default constructor for Jackson deserialization.
    }

    /**
     * Constructor is used when node is created in the code.
     */
    ConfigNode(ConfigNode parent, Map<String, String> attributes, EnumSet<Flags> flags) {
        this.parent = parent;
        this.attributes = attributes;
        this.value = flags.contains(Flags.IS_VALUE);
        this.deprecated = flags.contains(Flags.IS_DEPRECATED);
    }

    /**
     * Creates a root configuration node.
     */
    public static ConfigNode createRoot(String rootName, Class<?> className, ConfigurationType type, boolean internal) {
        Map<String, String> attrs = new LinkedHashMap<>();
        attrs.put(Attributes.NAME, rootName);
        attrs.put(Attributes.CLASS, className.getCanonicalName());
        attrs.put("TYPE", type.toString());
        attrs.put("INTERNAL", String.valueOf(internal));

        return new ConfigNode(null, attrs, EnumSet.of(Flags.IS_ROOT));
    }

    /**
     * Returns the name of this node.
     */
    public String name() {
        return attributes.get(Attributes.NAME);
    }

    /**
     * Returns the type (class name) of this node.
     */
    public String type() {
        return attributes.get(Attributes.CLASS);
    }

    /**
     * Returns the child nodes of this node.
     */
    public Collection<ConfigNode> childNodes() {
        return childNodeMap.values();
    }

    public void setParent(ConfigNode parent) {
        this.parent = parent;
    }

    /**
     * Returns the child nodes of this node.
     */
    void addChildNodes(Collection<ConfigNode> childNodes) {
        childNodes.forEach(e -> childNodeMap.put(e.name(), e));
    }

    /**
     * Returns {@code true} if this node is a root node, {@code false} otherwise.
     */
    @JsonIgnore
    public boolean isRoot() {
        return parent == null;
    }

    /**
     * Returns {@code true} if this node represents a value, {@code false} otherwise.
     */
    public boolean isValue() {
        return value;
    }

    /**
     * Returns {@code true} if this node is marked as deprecated, {@code false} otherwise.
     */
    public boolean isDeprecated() {
        return deprecated;
    }

    /**
     * Returns the raw attributes of this node. This method is used for serialization purposes.
     */
    public Map<String, String> rawAttributes() {
        return attributes;
    }

    /**
     * Constructs the full path of this node in the configuration tree.
     */
    public final String path() {
        String name = name();

        return parent == null ? name : parent.path() + '.' + name;
    }

    /**
     * Accepts a visitor to traverse this node and its children.
     */
    public void accept(ConfigShuttle visitor) {
        visitor.visit(this);

        for (ConfigNode child : childNodes()) {
            child.accept(visitor);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        return toString().equals(o.toString());
    }

    @Override
    public int hashCode() {
        return Objects.hash(name(), type());
    }

    @Override
    public final String toString() {
        return path() + ": [" + attributes.entrySet().stream()
                .map((e) -> e.getKey() + "=" + e.getValue())
                .collect(Collectors.joining(", "))
                + (childNodeMap.isEmpty() ? "" : ", children=" + childNodeMap.size())
                + ']';
    }

    /**
     * Node flags that describe its properties.
     */
    enum Flags {
        IS_ROOT(1),
        IS_VALUE(1 << 1),
        IS_DEPRECATED(1 << 2);

        private final int mask;

        Flags(int mask) {
            this.mask = mask;
        }

        public int mask() {
            return mask;
        }

        static EnumSet<Flags> parseFlags(String hex) {
            if (hex == null || hex.isEmpty()) {
                return EnumSet.noneOf(Flags.class);
            }
            int mask = Integer.parseInt(hex, 16);
            EnumSet<Flags> result = EnumSet.noneOf(Flags.class);
            for (Flags flag : Flags.values()) {
                if ((mask & flag.mask()) != 0) {
                    result.add(flag);
                }
            }
            return result;
        }

        static String toHexString(EnumSet<Flags> flags) {
            return Integer.toHexString(flags.stream().mapToInt(Flags::mask).reduce(0, (a, b) -> a | b));
        }
    }

    /**
     * Common attribute keys.
     */
    static class Attributes {
        static String NAME = "name";
        static String CLASS = "class";
        static String FLAGS = "flags";
        static String ANNOTATIONS = "annotations";
    }
}
