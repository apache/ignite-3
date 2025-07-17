package org.apache.ignite.internal.configuration.compatibility.framework;

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.ignite.internal.configuration.compatibility.GenerateConfigurationSnapshot;
import org.apache.ignite.internal.configuration.compatibility.framework.ConfigNode.NodeReference;
import org.apache.ignite.internal.configuration.compatibility.framework.ConfigurationTreeComparator.ComparisonContext;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.util.CollectionUtils;

public class Comp2 {

    private static final IgniteLogger LOG = Loggers.forClass(GenerateConfigurationSnapshot.class);

    private final ComparisonContext comparisonContext;

    public Comp2() {
        this.comparisonContext = new ComparisonContext();
    }

    public Comp2(ComparisonContext comparisonContext) {
        this.comparisonContext = comparisonContext;
    }

    public void ensureCompatible(List<ConfigNode> previousRoots, List<ConfigNode> currentRoots) {
        Map<String, Map<String, ConfigNode>> previousRootsByKind = groupByKind(previousRoots);
        Map<String, Map<String, ConfigNode>> currentRootsByKind = groupByKind(currentRoots);

        // Compare configuration kinds
        if (!previousRootsByKind.keySet().equals(currentRootsByKind.keySet())) {
            String error = format("Configuration kind do not match. Expected: {} but got {}",
                    previousRootsByKind.keySet(),
                    currentRootsByKind.keySet()
            );

            throw new IllegalStateException(error);
        }

        // Then compare roots one by one
        for (Entry<String, Map<String, ConfigNode>> entry : previousRootsByKind.entrySet()) {
            Map<String, ConfigNode> prev = entry.getValue();
            Map<String, ConfigNode> current = currentRootsByKind.get(entry.getKey());

            compareConfigRoots(prev, current);
        }
    }

    private void compareConfigRoots(Map<String, ConfigNode> previousRoots, Map<String, ConfigNode> currentRoots) {
        Set<String> removed = CollectionUtils.difference(previousRoots.keySet(), currentRoots.keySet());

        // Check config roots
        if (!removed.isEmpty()) {
            String error = format("Incompatible change. Some of the root keys has been removed.\n"
                            + "Removed root keys: {}\n"
                            + "Previous root keys: {}\n"
                            + "Current root keys: {}\n",
                    removed, previousRoots.keySet(), currentRoots.keySet());

            throw new IllegalStateException(error);
        }

        for (Map.Entry<String, ConfigNode> e : previousRoots.entrySet()) {
            ConfigNode current = currentRoots.get(e.getKey());
            if (current == null) {
                // adding a configuration root is a compatible change.
                continue;
            }

            ConfigNode previous = e.getValue();
            ensureCompareCompatible(previous, current);
        }
    }

    public void ensureCompareCompatible(ConfigNode previous, ConfigNode current) {
        // List of paths to support polymorphic nodes.
        // Converts configuration trees to a map of paths
        Map<String, List<Path>> previousNodes = buildPaths(previous);
        Map<String, List<Path>> currentNodes = buildPaths(current);

        if (LOG.isInfoEnabled()) {
            LOG.info("Previous: {}", pathsToString(previousNodes));
        }

        if (LOG.isInfoEnabled()) {
            LOG.info("Current: {}", pathsToString(previousNodes));
        }

        Set<String> removed = CollectionUtils.difference(previousNodes.keySet(), currentNodes.keySet());
        Set<String> added = CollectionUtils.difference(currentNodes.keySet(), previousNodes.keySet());
        Set<String> unchanged = CollectionUtils.intersect(previousNodes.keySet(), currentNodes.keySet());

        List<String> errors = new ArrayList<>();

        validateRemovals(removed, comparisonContext, errors);
        validateAdded(currentNodes, added, errors);
        validateRemaining(previousNodes, currentNodes, unchanged, errors);

        reportErrors(errors);
    }

    private static void validateRemovals(Set<String> keys, ComparisonContext comparisonContext, List<String> errors) {
        LOG.info("Removed keys: {}", keys);

        Set<String> removedFiltered = keys.stream().filter(r -> !comparisonContext.shouldIgnore(r)).collect(Collectors.toSet());
        if (!removedFiltered.isEmpty()) {
            errors.add("These properties has been removed: " + keys);
        }
    }

    private static void validateAdded(Map<String, List<Path>> currentNodes, Set<String> keys, List<String> errors) {
        LOG.info("Added keys: {}", keys);

        Set<String> nonDefaults = new HashSet<>();

        for (String key : keys) {
            List<Path> addedPaths = currentNodes.get(key);

            for (Path path : addedPaths) {
                for (ConfigNode node : path.nodes) {
                    if (node.isValue() && !node.hasDefault()) {
                        nonDefaults.add(node.path());
                    }
                }
            }
        }

        if (!nonDefaults.isEmpty()) {
            errors.add("Adding properties without default values is incompatible change: " + nonDefaults);
        }
    }

    private static void validateRemaining(
            Map<String, List<Path>> previousNodes,
            Map<String, List<Path>> currentNodes,
            Set<String> keys,
            List<String> errors
    ) {

        LOG.info("Remained keys: {}", keys);

        for (String key : keys) {
            List<Path> previous = previousNodes.get(key);
            List<Path> current = currentNodes.get(key);

            if (previous.size() == 1 && current.size() == 1 || previous.size() == current.size()) {
                // Both nodes are non-polymorphic or polymorphic nodes has not been moved between a base class and its subclasses. 
                for (int i = 0; i < previous.size(); i++) {
                    validatePath(previous.get(i), current.get(i), errors);
                }
            } else if (previous.size() == 1 && noPolymorphicNodes(previous)) {
                // From non-polymorphic to a polymorphic one
                validateNonPolymorphicToPolymorphic(previous.get(0), current, errors);
            } else if (current.size() == 1 && noPolymorphicNodes(current)) {
                // From non-polymorphic to a polymorphic one
                validatePolymorphicToNonPolymorphic(previous, current.get(0), errors);
            } else {
                // From one polymorphic case to another polymorphic
                validatePolymorphicToPolymorphic(previous, current, errors);
            }
        }

        reportErrors(errors);
    }

    private static void validatePath(Path previousPath, Path currentPath, List<String> errors) {
        for (int i = 0; i < previousPath.nodes.size(); i++) {
            ConfigNode previous = previousPath.nodes.get(i);
            ConfigNode current = currentPath.nodes.get(i);

            boolean match = ConfigurationTreeComparator.match(previous, current, null);
            if (!match) {
                errors.add("Node has incompatible changes: " + current.path());
            }
        }
    }

    private static void reportErrors(List<String> errors) {
        if (!errors.isEmpty()) {
            StringBuilder sb = new StringBuilder("Incompatible changes").append(System.lineSeparator());
            for (String message : errors) {
                sb.append('\t').append(message).append(System.lineSeparator());
            }

            throw new IllegalStateException(sb.toString());
        }
    }

    private static boolean noPolymorphicNodes(List<Path> paths) {
        return paths.stream().allMatch(p -> p.subclass.isEmpty());
    }

    private static void validateNonPolymorphicToPolymorphic(Path previous, List<Path> currentPaths, List<String> errors) {
        // Extracting a base class + subclasses is compatible 
        // unless there are incompatible changes between nodes.
        for (Path current : currentPaths) {
            validatePath(previous, current, errors);
        }
    }

    private static void validatePolymorphicToNonPolymorphic(List<Path> previousPaths, Path current, List<String> errors) {
        // Converting a base class with its subclasses to a simple config is compatible
        // unless there are incompatible changes between nodes.
        for (Path previous : previousPaths) {
            validatePath(previous, current, errors);
        }
    }

    private static void validatePolymorphicToPolymorphic(List<Path> previousPaths, List<Path> currentPaths, List<String> errors) {
        // Moving a node w/ default value from a subclass to its base class is incompatible change. 
        // Moving a node w/o default value from a subclass to its base class is valid only if there is a single subclass

        Optional<Path> previousBaseClass = previousPaths.stream().filter(p -> p.subclass.isEmpty()).findFirst();
        Optional<Path> currentBaseClass = currentPaths.stream().filter(p -> p.subclass.isEmpty()).findFirst();

        if (previousBaseClass.isEmpty() && currentBaseClass.isPresent()) {
            // Moving a value from a subclass to the base class
            Path current = currentBaseClass.orElseThrow(() -> new IllegalStateException("Unexpected"));
            ConfigNode valueNode = current.nodes.get(current.nodes.size() - 1);

            // A value w/o default and there are more than 2 subclasses
            if (!valueNode.hasDefault() && currentPaths.size() > 2) {
                errors.add("Unable to move non-default node from a subclass to its base class: " + valueNode.path());
            }

        } else if (previousBaseClass.isPresent() && currentBaseClass.isEmpty()) {
            // Moving a value from a base class to a subclass
            Path previous = previousBaseClass.orElseThrow(() -> new IllegalStateException("Unexpected"));
            ConfigNode valueNode = previous.nodes.get(previous.nodes.size() - 1);

            // A value w/o default and there are more than 2 subclasses 
            if (!valueNode.hasDefault() && previousPaths.size() > 2) {
                errors.add("Unable to move non-default node from a base class to a subclass: " + valueNode.path());
            }
        }
    }

    private static Map<String, Map<String, ConfigNode>> groupByKind(List<ConfigNode> nodes) {
        Map<String, Map<String, ConfigNode>> out = new LinkedHashMap<>();

        for (ConfigNode node : nodes) {
            Map<String, ConfigNode> byKind = out.computeIfAbsent(node.kind(), (k) -> new LinkedHashMap<>());
            ConfigNode prev = byKind.put(node.name(), node);
            assert prev == null : "Duplicate name";
        }

        return out;
    }

    private static Map<String, List<Path>> buildPaths(ConfigNode node) {
        Map<String, List<Path>> paths = new LinkedHashMap<>();

        collect(node, new CurrentPath(), paths);

        for (List<Path> path : paths.values()) {
            path.sort(Comparator.comparing(a -> a.subclass));
        }

        return paths;
    }

    /**
     * Converts a configuration tree into lists of paths from a root to leafs.
     * 
     * <pre>
     *    A
     *  /  \
     * B    C
     *     / \  
     *    D   E
     * </pre>
     * 
     * Gets converted to:
     * <pre>
     *     A.B: [A, B], 
     *     A.C.D: [A, C, D], 
     *     A.C.E: {A, C, E}
     * </pre>
     * 
     * <p>Renaming: 
     * <p>Each renamed node has legacy node names, in that case we simply visit the node under its legacy names.
     * 
     * <p>Polymorphic nodes:
     * <p>{@link NodeReference#nodes()} returns multiple nodes:
     *
     * <pre>
     *    A
     *  /  \
     * B    C
     *     / \ 
     *    t   f0
     * C has two subclasses C1 and C2 t is a field that stores instance type.
     *    C1  
     *   /  \
     *  f1   f2
     *
     *    C2  
     *   /
     *  f3
     * </pre>
     *
     * Gets converted to:
     * <pre>
     *     A.B: [A, B]
     *     A.C.t: [A, C, t] (C1), [A, C, t] (C2)
     *     A.C.f0: [A, C, f0] (C1), [A, C, f0] (C2)
     *     A.C.f1: [A, C, f1] (C1)
     *     A.C.f2: [A, C, f1] (C1)
     *     A.C.f3: [A, C, f1] (C2)
     * </pre>
     */
    private static void collect(ConfigNode node, CurrentPath path, Map<String, List<Path>> paths) {
        if (node.isValue()) {
            Set<String> names = getNodeNames(node);
            for (String name : names) {
                CurrentPath end = path.newChild(name, node);
                String key = String.join(".", end.keys);

                List<Path> out = paths.computeIfAbsent(key, (k) -> new ArrayList<>());
                out.add(new Path(path.providence, end.nodes));
            }
        } else {
            Set<String> names = getNodeNames(node);
            for (String name : names) {
                CurrentPath subPath;
                if (node.instanceType() != null) {
                    subPath = new CurrentPath(path, node).newChild(name, node);
                } else {
                    subPath = path.newChild(name, node);
                }

                for (var ref : node.childNodes()) {
                    List<ConfigNode> nodes = new ArrayList<>(ref.nodes());
                    nodes.sort(Comparator.comparing(ConfigNode::name).thenComparing(ConfigNode::className));

                    for (var child : nodes) {
                        collect(child, subPath, paths);
                    }
                }
            }
        }
    }

    private static class Path {
        final List<ConfigNode> nodes;
        // To distinguish a base class and subclasses 
        final String subclass;

        private Path(List<String> providence, List<ConfigNode> nodes) {
            this.nodes = List.copyOf(nodes);
            this.subclass = String.join(".", providence);
        }

        @Override
        public String toString() {
            return "Path{" +
                    "subclass=" + subclass +
                    ", nodes=" + nodes.stream().map(n -> n.path() + " " + n.attributes()).collect(Collectors.toSet()) +
                    '}';
        }
    }

    private static class CurrentPath {
        private final List<String> keys = new ArrayList<>();
        private final List<ConfigNode> nodes = new ArrayList<>();
        private final List<String> providence = new ArrayList<>();

        private CurrentPath() {

        }

        CurrentPath(CurrentPath src) {
            this.keys.addAll(src.keys);
            this.nodes.addAll(src.nodes);
            this.providence.addAll(src.providence);
        }

        CurrentPath(CurrentPath src, ConfigNode srcNode) {
            this.keys.addAll(src.keys);
            this.nodes.addAll(src.nodes);
            if (!providence.isEmpty()) {
                throw new IllegalStateException("Only one level of polymorphic config hierarchy is supported: " + srcNode.path());
            }
            this.providence.add(srcNode.instanceType());
        }

        CurrentPath newChild(String name, ConfigNode node) {
            CurrentPath path = new CurrentPath(this);
            path.keys.add(name);
            path.nodes.add(node);
            return path;
        }
    }

    private static Set<String> getNodeNames(ConfigNode node) {
        if (node.legacyPropertyNames().isEmpty()) {
            return Set.of(node.name());
        } else {
            return new TreeSet<>(node.legacyPropertyNames());
        }
    }

    private static String pathsToString(Map<String, List<Path>> nodes) {
        StringBuilder sb = new StringBuilder().append(System.lineSeparator());

        for (Entry<String, List<Path>> e : new TreeMap<>(nodes).entrySet()) {
            List<Path> paths = e.getValue();
            boolean hasSubclasses = paths.isEmpty();

            sb.append("\t").append(e.getKey()).append("=").append(paths.size()).append(System.lineSeparator());
            for (var p : paths) {
                sb.append("\t\t").append(pathToString(p, hasSubclasses)).append(System.lineSeparator());
            }
        }

        return sb.toString();
    }

    private static String pathToString(Path path, boolean hasSubclasses) {
        StringBuilder sb = new StringBuilder();

        for (ConfigNode node : path.nodes) {
            if (sb.length() > 0) {
                sb.append(", ");
            }
            sb.append(node.name());
        }

        if (hasSubclasses) {
            return sb.toString();
        } else {
            return path.subclass + sb;
        }
    }
}
