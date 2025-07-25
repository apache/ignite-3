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
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.ignite.configuration.annotation.ConfigurationType;
import org.jetbrains.annotations.TestOnly;

/**
 * Tree node that describes a configuration tree item.
 */
public class ConfigNode {
    @JsonProperty
    private Map<String, String> attributes;
    @JsonProperty
    private List<ConfigAnnotation> annotations = new ArrayList<>();
    @JsonProperty
    private Map<String, Node> childNodeMap = new LinkedHashMap<>();
    @JsonProperty
    private String flagsHexString;
    @JsonProperty
    private Set<String> legacyPropertyNames;
    @JsonProperty
    private Collection<String> deletedPrefixes = Set.of();

    // Non-serializable fields.
    @JsonIgnore
    @Nullable
    private ConfigNode parent;
    @JsonIgnore
    private EnumSet<Flags> flags;

    @SuppressWarnings("unused")
    ConfigNode() {
        // Default constructor for Jackson deserialization.
    }

    @TestOnly
    ConfigNode(
            @Nullable ConfigNode parent,
            Map<String, String> attributes,
            List<ConfigAnnotation> annotations,
            EnumSet<Flags> flags
    ) {
        this(parent, attributes, annotations, flags, Set.of(), List.of());
    }

    /**
     * Constructor is used when node is created in the code.
     */
    public ConfigNode(
            @Nullable ConfigNode parent,
            Map<String, String> attributes,
            List<ConfigAnnotation> annotations,
            EnumSet<Flags> flags,
            Set<String> legacyPropertyNames,
            Collection<String> deletedPrefixes
    ) {
        this.parent = parent;
        this.attributes = attributes;
        this.annotations = annotations;
        this.flags = flags;
        this.flagsHexString = Flags.toHexString(flags);
        this.legacyPropertyNames = legacyPropertyNames;
        this.deletedPrefixes = deletedPrefixes;
    }

    @TestOnly
    static ConfigNode createRoot(
            String rootName,
            Class<?> className,
            ConfigurationType type,
            boolean internal
    ) {
        return createRoot(rootName, className, type, internal, Set.of());
    }

    /**
     * Creates a root configuration node.
     */
    public static ConfigNode createRoot(
            String rootName,
            Class<?> className,
            ConfigurationType type,
            boolean internal,
            Collection<String> deletedPrefixes
    ) {
        Map<String, String> attrs = new LinkedHashMap<>();
        attrs.put(Attributes.NAME, rootName);
        attrs.put(Attributes.CLASS, className.getCanonicalName());
        attrs.put(Attributes.KIND, type.toString());

        EnumSet<Flags> flags = EnumSet.of(Flags.IS_ROOT);
        if (internal) {
            flags.add(Flags.IS_INTERNAL);
        }

        return new ConfigNode(null, attrs, List.of(), flags, Set.of(), deletedPrefixes);
    }

    /**
     * Initialize node after deserialization.
     *
     * @param parent Parent node to links with.
     */
    public void init(@Nullable ConfigNode parent) {
        this.parent = parent;

        this.flags = Flags.parseFlags(flagsHexString);
    }

    /**
     * Returns the name of this node.
     */
    public String name() {
        return attributes.get(Attributes.NAME);
    }

    /** Returns root node type. */
    public String kind() {
        return attributes.get(Attributes.KIND);
    }

    /** Returns value node type. */
    public String type() {
        return attributes.get(Attributes.CLASS);
    }

    /**
     * Returns flags for this node.
     */
    public Set<Flags> flags() {
        return flags;
    }

    /**
     * Returns the child nodes of this node.
     */
    public Map<String, Node> children() {
        return childNodeMap;
    }

    /**
     * Returns the parent node of this node.
     */
    public @Nullable ConfigNode getParent() {
        return parent;
    }

    /**
     * Add the child nodes to this node.
     */
    void addChildNodes(Collection<ConfigNode> childNodes) {
        assert !flags.contains(Flags.IS_VALUE) : "Value node can't have children.";

        childNodes.forEach(e -> {
            e.parent = this;
            addChildNode(e.name(), new Node(e));
        });
    }

    /**
     * Shortcut to {@link #addChildNodes(Collection)}.
     */
    @TestOnly
    void addChildNodes(ConfigNode... childNodes) {
        addChildNodes(List.of(childNodes));
    }

    /**
     * Add a polymorphic child node to this node.
     */
    void addPolymorphicNode(String name, Map<String, ConfigNode> childNodes, @Nullable String defaultId) {
        boolean nameMatches = childNodes.values().stream().allMatch(n -> n.name().equals(name));
        if (!nameMatches) {
            throw new IllegalArgumentException("All nodes name should be equal to: " + name);
        }
        for (ConfigNode n : childNodes.values()) {
            n.parent = this;
        }
        addChildNode(name, new Node(childNodes, defaultId));
    }

    private void addChildNode(String name, Node node) {
        Node existing = childNodeMap.put(name, node);
        if (existing != null) {
            throw new IllegalArgumentException("Child node already exists: " + name);
        }
    }

    /**
     * Returns {@code true} if this node represents a value, {@code false} otherwise.
     */
    @JsonIgnore
    public boolean isValue() {
        return flags.contains(Flags.IS_VALUE);
    }

    /**
     * Returns {@code true} if this node represents a named list node, {@code false} otherwise.
     */
    @JsonIgnore
    public boolean isNamedNode() {
        return flags.contains(Flags.IS_NAMED_NODE);
    }

    /**
     * Returns {@code true} if this node represents an inner config node, {@code false} otherwise.
     */
    @JsonIgnore
    public boolean isInnerNode() {
        return flags.contains(Flags.IS_INNER_NODE);
    }

    /**
     * Returns {@code true} if a configuration value that this node presents has a default value, otherwise {@code false}.
     */
    @JsonIgnore
    public boolean hasDefault() {
        return flags.contains(Flags.HAS_DEFAULT);
    }

    /**
     * Returns {@code true} if this node represents internal part of configuration, {@code false} otherwise.
     */
    @JsonIgnore
    public boolean isInternal() {
        return flags.contains(Flags.IS_INTERNAL);
    }

    /**
     * Returns {@code true} if this node is marked as deprecated, {@code false} otherwise.
     */
    @JsonIgnore
    public boolean isDeprecated() {
        return flags.contains(Flags.IS_DEPRECATED);
    }

    /**
     * Returns node annotations.
     */
    public List<ConfigAnnotation> annotations() {
        return annotations;
    }

    /**
     * Returns node legacy names.
     */
    Set<String> legacyPropertyNames() {
        return legacyPropertyNames;
    }

    /**
     * Returns deleted prefixes.
     */
    Collection<String> deletedPrefixes() {
        return deletedPrefixes;
    }

    /**
     * Returns an id of a polymorphic instance.
     */
    @Nullable String polymorphicInstanceId() {
        return attributes.get(Attributes.POLYMORPHIC_INSTANCE_ID);
    }


    /**
     * Constructs the full path of this node in the configuration tree.
     */
    @JsonIgnore
    public String path() {
        String name = name();

        return parent == null ? name : parent.path() + '.' + name;
    }

    /**
     * Accepts a visitor to traverse this node and its children.
     */
    public void accept(ConfigShuttle visitor) {
        visitor.visit(this);

        for (Node child : children().values()) {
            if (child.isPolymorphic()) {
                // Make traversal order deterministic
                for (ConfigNode instanceNode : new TreeMap<>(child.nodes()).values()) {
                    instanceNode.accept(visitor);
                }
            } else {
                child.node().accept(visitor);
            }
        }
    }

    String digest() {
        // Avoid actual class name from being compared for non-value nodes.
        Predicate<Entry<String, String>> filter = isValue()
                ? e -> true
                : e -> !e.getKey().equals(Attributes.CLASS);

        String attributes = this.attributes.entrySet().stream()
                .filter(filter)
                .map(Entry::toString)
                .collect(Collectors.joining(", "));

        return path() + ": ["
                + attributes
                + ", annotations=" + annotations().stream().map(ConfigAnnotation::digest).collect(Collectors.joining(",", "[", "]"))
                + ", flags=" + flagsHexString
                + (childNodeMap.isEmpty() ? "" : ", children=" + childNodeMap.size())
                + ']';
    }

    @Override
    public final String toString() {
        return path() + ": ["
                + attributes.entrySet().stream().map(Map.Entry::toString).collect(Collectors.joining(","))
                + ", annotations=" + annotations().stream().map(ConfigAnnotation::toString).collect(Collectors.joining(",", "[", "]"))
                + ", flags=" + flags
                + (childNodeMap.isEmpty() ? "" : ", children=" + childNodeMap.size())
                + ']';
    }

    /**
     * Node flags that describe its properties.
     */
    enum Flags {
        IS_ROOT(1),
        IS_VALUE(1 << 1),
        IS_DEPRECATED(1 << 2),
        IS_INTERNAL(1 << 3),
        IS_NAMED_NODE(1 << 4),
        IS_INNER_NODE(1 << 5),
        HAS_DEFAULT(1 << 6);

        private final int mask;

        Flags(int mask) {
            this.mask = mask;
        }

        int mask() {
            return mask;
        }

        static EnumSet<Flags> parseFlags(String hex) {
            if (hex == null || hex.isEmpty()) {
                return EnumSet.noneOf(Flags.class);
            }
            int mask = Integer.parseInt(hex, 16);
            EnumSet<Flags> result = EnumSet.noneOf(Flags.class);
            for (Flags flag : values()) {
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
        static final String NAME = "name";
        static final String KIND = "kind";
        static final String CLASS = "class";
        static final String POLYMORPHIC_INSTANCE_ID = "polymorphicInstanceId";
    }

    /**
     * Node holds a references either to a single node or to a map of possible polymorphic nodes with extra `base` node that 
     * includes fields common to all polymorphic instances. Each polymorphic node has child nodes that represent its fields 
     * and fields common to its polymorphic configuration as well.
     */
    public static class Node {
        /**
         * Non-polymorphic node.
         */
        @JsonProperty
        private @Nullable ConfigNode single;

        @JsonProperty
        private @Nullable String defaultPolymorphicInstanceId;

        /**
         * All polymorphic instances.
         */
        @JsonProperty
        private @Nullable Map<String, ConfigNode> polymorphicNodes = new LinkedHashMap<>();

        @SuppressWarnings("unused")
        Node() {
            // Default constructor for Jackson deserialization.
        }

        Node(ConfigNode single) {
            this.single = single;
            this.polymorphicNodes = null;
            this.defaultPolymorphicInstanceId = null;
        }

        Node(Map<String, ConfigNode> polymorphicNodes, @Nullable String defaultPolymorphicInstanceId) {
            this.single = null;
            this.defaultPolymorphicInstanceId = defaultPolymorphicInstanceId;
            this.polymorphicNodes = Map.copyOf(polymorphicNodes);
        }

        /**
         * Returns {@code true} if this node represents a polymorphic configuration node.
         */
        @JsonIgnore
        boolean isPolymorphic() {
            return polymorphicNodes != null;
        }

        /** Returns the id of default polymorphic configuration. */
        public @Nullable String defaultPolymorphicInstanceId() {
            return defaultPolymorphicInstanceId;
        }

        /**
         * Returns a single node if this node is a simple node or throws.
         */
        ConfigNode node() {
            assert single != null : "Use the nodes() method instead.";
            return single;
        }

        /**
         * Returns a map of polymorphic instances if this node is polymorphic or throws.
         */
        Map<String, ConfigNode> nodes() {
            assert polymorphicNodes != null : "Use the node() method instead.";
            return polymorphicNodes;
        }

        /**
         * Returns {@code true} if this node represents a value node.
         *
         * @see ConfigNode#isValue()
         */
        @JsonIgnore
        boolean isValue() {
            return anyNode().isValue();
        }

        /**
         * Returns {@code true} if this node has a default value.
         *
         * @see ConfigNode#hasDefault()
         */
        boolean hasDefault() {
            return anyNode().hasDefault();
        }

        /**
         * Returns legacy names.
         *
         * @see ConfigNode#legacyPropertyNames()
         */
        Set<String> legacyPropertyNames() {
            return anyNode().legacyPropertyNames();
        }

        /**
         * Returns the full path of this node in the configuration tree.
         */
        String path() {
            return anyNode().path();
        }

        /**
         * A shortcut to access a node. Should only be used to access node's flags or other common attributes such as path, because a
         * configuration field is guaranteed to have the same values for them for every polymorphic instance.
         */
        private ConfigNode anyNode() {
            if (single != null) {
                return single;
            } else {
                return polymorphicNodes.values().iterator().next();
            }
        }
    }
}
