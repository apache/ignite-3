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
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
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
    private List<NodeReference> childReferences = new ArrayList<>();
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
     * Returns the child nodes of this node.
     */
    public Collection<NodeReference> childNodes() {
        return childReferences;
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
    void addChildReferences(Collection<NodeReference> references) {
        assert !flags.contains(Flags.IS_VALUE) : "Value node can't have children.";

        childReferences.addAll(references);
    }

    /**
     * Add the child nodes to this node.
     */
    @TestOnly
    void linkChildReferences(NodeReference... references) {
        assert !flags.contains(Flags.IS_VALUE) : "Value node can't have children.";

        for (NodeReference ref : references) {
            for (ConfigNode node : ref.nodes()) {
                node.parent = this;
            }
        }

        childReferences.addAll(List.of(references));
    }

    /**
     * Add the child nodes to this node.
     */
    @TestOnly
    void addChildNodes(Collection<ConfigNode> childNodes) {
        childNodes.forEach(n -> n.parent = this);

        addChildReferences(childNodes.stream().map(n -> new NodeReference(List.of(n))).collect(Collectors.toList()));
    }

    /**
     * Shortcut to {@link #addChildNodes(Collection)}.
     */
    @TestOnly
    void addChildNodes(ConfigNode... childNodes) {
        addChildNodes(Arrays.asList(childNodes));
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
    @JsonIgnore
    public boolean isValue() {
        return flags.contains(Flags.IS_VALUE);
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

    public boolean hasDefault() {
        return flags.contains(Flags.HAS_DEFAULT);
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
     * Returns a class name.
     */
    String className() {
        return attributes.get(Attributes.CLASS);
    }

    /**
     * Returns instance type of a polymorphic config.
     */
    @Nullable
    String instanceType() {
        return attributes.get(Attributes.INSTANCE_TYPE);
    }

    /** Returns attributes of this node. */
    @JsonIgnore
    Map<String, String> attributes() {
        return attributes;
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

        for (NodeReference ref : childNodes()) {
            for (var node : ref.nodes()) {
                node.accept(visitor);
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
                + (childReferences.isEmpty() ? "" : ", children=" + childReferences.size())
                + ']';
    }

    @Override
    public final String toString() {
        return path() + ": ["
                + attributes.entrySet().stream().map(Map.Entry::toString).collect(Collectors.joining(","))
                + ", annotations=" + annotations().stream().map(ConfigAnnotation::toString).collect(Collectors.joining(",", "[", "]"))
                + ", flags=" + flags
                + (childReferences.isEmpty() ? "" : ", children=" + childReferences.size())
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
        HAS_DEFAULT(1 << 4);

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
        static String NAME = "name";
        static String KIND = "kind";
        static String CLASS = "class";
        static String INSTANCE_TYPE = "instanceType";
    }

    /**
     * Child node reference.
     */
    public static class NodeReference {
        @JsonProperty
        private List<ConfigNode> nodes = new ArrayList<>();

        @SuppressWarnings("unused")
        NodeReference() {
            // Default constructor for Jackson deserialization.
        }

        /**
         * Constructor for a multi node reference (used by polymorphic configuration).
         */
        NodeReference(List<ConfigNode> nodes) {
            this.nodes = nodes;
        }

        /** Nodes. */
        public List<ConfigNode> nodes() {
            return nodes;
        }
    }
}
