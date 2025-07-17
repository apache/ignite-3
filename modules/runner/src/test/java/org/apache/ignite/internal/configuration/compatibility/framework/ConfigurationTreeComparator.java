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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.ConfigurationModule;
import org.apache.ignite.configuration.KeyIgnorer;
import org.apache.ignite.internal.configuration.compatibility.framework.ConfigNode.NodeReference;
import org.jetbrains.annotations.Nullable;

/**
 * Compares two configuration trees (snapshot and current).
 */
public class ConfigurationTreeComparator {

    private static final ConfigAnnotationsValidator ANNOTATION_VALIDATOR = new ConfigAnnotationsValidator();

    /**
     * Validates the current configuration is compatible with the snapshot.
     */
    public static void ensureCompatible(
            List<ConfigNode> snapshotTrees,
            List<ConfigNode> actualTrees,
            ComparisonContext compContext
    ) {
//        LeafNodesVisitor shuttle = new LeafNodesVisitor(new Validator(actualTrees), compContext);
//
//        for (ConfigNode tree : snapshotTrees) {
//            tree.accept(shuttle);
//        }
        Comp2 comp2 = new Comp2(compContext);
        comp2.ensureCompatible(snapshotTrees, actualTrees);
    }

    /**
     * Compares the configuration trees are equals by dumping their state to string.
     */
    public static void compare(List<ConfigNode> tree1, List<ConfigNode> tree2) {
        String dump1 = dumpTree(tree1);
        String dump2 = dumpTree(tree2);
        assertEquals(dump1, dump2, "Configuration metadata mismatch");
        
        Comp2 comp2 = new Comp2();
        comp2.ensureCompatible(tree1, tree2);
    }

    /**
     * Traverses the tree and triggers validation for leaf nodes.
     */
    private static class LeafNodesVisitor implements ConfigShuttle {
        private final Consumer<ConfigNode> validator;
        private final ComparisonContext compContext;

        private LeafNodesVisitor(Consumer<ConfigNode> validator, ComparisonContext compContext) {
            this.validator = validator;
            this.compContext = compContext;
        }

        @Override
        public void visit(ConfigNode node) {
            if (node.isValue() && !compContext.shouldIgnore(node.path())) {
                validator.accept(node);
            }
        }
    }

    /**
     * Validates value nodes.
     */
    private static class Validator implements Consumer<ConfigNode> {
        private final List<ConfigNode> roots;

        private Validator(List<ConfigNode> roots) {
            this.roots = roots;
        }

        @Override
        public void accept(ConfigNode leafNode) {
            List<ConfigNode> path = getPath(leafNode);

            // Validate path starting from the root.
            List<Collection<ConfigNode>> candidates = List.of(roots);

            for (ConfigNode node : path) {
                String instanceType = node.instanceType();
                ConfigNode found = find(node, candidates, instanceType);

                if (found == null) {
                    reportError(path, node, candidates);
                    return;
                }

                // node is a snapshot node
                // candidate is a current configuration node.
                validateAnnotations(node, found);

                Collection<NodeReference> childRefs = found.childNodes();
                candidates = childRefs.stream().map(NodeReference::nodes).collect(Collectors.toList());
            }
        }

        /**
         * Returns first node from candidates collection that matches the given node.
         */
        private static @Nullable ConfigNode find(
                ConfigNode node,
                List<Collection<ConfigNode>> candidateList,
                @Nullable String instanceType
        ) {
            for (Collection<ConfigNode> candidates : candidateList) {
                for (ConfigNode candidate : candidates) {
                    if (match(node, candidate, instanceType)) {
                        return candidate;
                    }
                }
            }

            return null;
        }

        private static void reportError(
                List<ConfigNode> path,
                ConfigNode target,
                List<Collection<ConfigNode>> candidateList
        ) {
            /* No match for node: <node path>
             *
             * Node:
             *   <node details>
             *
             * Path:
             *   <path element node details>
             *   ...
             *   <path element node details>
             *
             * Candidates:
             *   <candidate node details>
             *   ...
             *   <candidate node details>
             */

            StringBuilder candidateText = new StringBuilder();

            for (Collection<ConfigNode> candidates : candidateList) {
                if (candidateText.length() > 0) {
                    candidateText.append(System.lineSeparator());
                }
                String text = renderNodeList(candidates);
                candidateText.append(text);
            }

            String pathText = renderNodeList(path);

            throw new IllegalStateException("No match found for node: " + target.path()
                    + "\n\nNode:\n\t" + target
                    + "\n\nPath:\n" + pathText
                    + "\n\nCandidates:\n" + candidateText
            );
        }

        private static String renderNodeList(Collection<ConfigNode> path) {
            StringBuilder pathText = new StringBuilder();

            for (ConfigNode node : path) {
                if (pathText.length() > 0) {
                    pathText.append(System.lineSeparator());
                }
                pathText.append('\t')
                        .append(node.path())
                        .append(" attributes: ")
                        .append(node.attributes())
                        .append(" annotations: ")
                        .append(node.annotations());
            }

            return pathText.toString();
        }
    }

    /**
     * Builds value node path.
     */
    private static List<ConfigNode> getPath(ConfigNode node) {
        List<ConfigNode> path = new ArrayList<>();
        while (node != null) {
            path.add(node);
            node = node.getParent();
        }

        Collections.reverse(path);

        return path;
    }

    /**
     * Returns {@code true} if given node is compatible with candidate node, {@code false} otherwise.
     */
    public static boolean match(ConfigNode node, ConfigNode candidate, @Nullable String instanceType) {
        return Objects.equals(candidate.kind(), node.kind())
                && matchNames(candidate, node)
                && validateFlags(candidate, node)
                && candidate.deletedPrefixes().containsAll(node.deletedPrefixes())
                && (!node.isValue() || Objects.equals(candidate.type(), node.type())) // Value node types can be changed.
                && (instanceType == null || Objects.equals(candidate.instanceType(), node.instanceType()))
                // TODO https://issues.apache.org/jira/browse/IGNITE-25747 Validate annotations properly.
                && candidate.annotations().containsAll(node.annotations()); // Annotations can't be removed.
    }

    private static void validateAnnotations(ConfigNode candidate, ConfigNode node) {
        List<String> errors = new ArrayList<>();

        ANNOTATION_VALIDATOR.validate(candidate, node, errors);

        if (errors.isEmpty()) {
            return;
        }

        StringBuilder sb = new StringBuilder();
        sb.append("Configuration compatibility issues for ")
                .append(node.path())
                .append(':')
                .append(System.lineSeparator());

        for (var error : errors) {
            sb.append("\t\t").append(error).append(System.lineSeparator());
        }

        throw new IllegalStateException(sb.toString());
    }

    private static boolean matchNames(ConfigNode candidate, ConfigNode node) {
        return Objects.equals(candidate.name(), node.name())
                || (node.isValue() && candidate.isValue() && compareUsingLegacyNames(candidate, node));
    }

    private static boolean compareUsingLegacyNames(ConfigNode candidate, ConfigNode node) {
        return candidate.legacyPropertyNames().contains(node.name());
    }

    private static boolean validateFlags(ConfigNode candidate, ConfigNode node) {
        return node.isRoot() == candidate.isRoot()
                && node.isValue() == candidate.isValue()
                && (!candidate.isInternal() || node.isInternal()) // Public property\tree can't be hidden.
                && (!node.isDeprecated() || candidate.isDeprecated()); // Deprecation shouldn't be removed.
    }

    private static String dumpTree(List<ConfigNode> nodes) {
        DumpingShuttle shuttle = new DumpingShuttle();
        nodes.forEach(n -> n.accept(shuttle));
        return shuttle.toString();
    }

    /**
     * Configuration tree visitor that dumps tree state to string.
     */
    private static class DumpingShuttle implements ConfigShuttle {
        private final StringBuilder sb = new StringBuilder();

        @Override
        public void visit(ConfigNode node) {
            sb.append(node.digest()).append('\n');
        }

        @Override
        public String toString() {
            return sb.toString();
        }
    }

    /** Holder class for comparison context. */
    public static class ComparisonContext {
        private final Set<ConfigurationModule> configurationModules;
        private Collection<KeyIgnorer> deletedItems;

        ComparisonContext() {
            this.configurationModules = Set.of();
        }

        public ComparisonContext(Set<ConfigurationModule> configurationModules) {
            this.configurationModules = configurationModules;
        }

        boolean shouldIgnore(String path) {
            if (deletedItems == null) {
                deletedItems = new ArrayList<>(configurationModules.size());

                for (ConfigurationModule module : configurationModules) {
                    KeyIgnorer keyIgnorer = KeyIgnorer.fromDeletedPrefixes(module.deletedPrefixes());

                    deletedItems.add(keyIgnorer);
                }
            }

            return deletedItems.stream().anyMatch(i -> i.shouldIgnore(path));
        }
    }
}
