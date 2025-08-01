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

package org.apache.ignite.internal.configuration.validation;

import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.impl.Parseable;
import com.typesafe.config.parser.ConfigDocument;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import org.apache.ignite.configuration.validation.ValidationIssue;
import org.jetbrains.annotations.Nullable;

/**
 * Validates that there are no duplicate keys in the configuration.
 *
 * <p>Imagine configuration like this:
 * <pre>
 * root {
 *   arrayField: [
 *     arrayMember1: { name = "123" },
 *     arrayMember2: { name = "234" }
 *   ]
 * }
 * </pre>
 *
 * <p>This configuration can be represented like this:</p>
 * <ul>
 *   <li>root children: [ ConfigNodePath("root"), ConfigNodeComplexValue(field) ]</li>
 *   <li>
 *       arrayField children: [ ConfigNodePath("arrayField"), ConfigNodeArray(arrayMember1), ConfigNodeArray(arrayMember2) ]
 *   </li>
 *   <li>arrayMember1 children: [ ConfigNodeComplexValue(name) ]</li>
 *   <li>arrayMember2 children: [ ConfigNodeComplexValue(name) ]</li>
 *   <li>name children: [ ConfigNodePath("name"), Value("123") ]</li>
 *   <li>name children: [ ConfigNodePath("name"), Value("234") ]</li>
 * </ul>
 */
public class ConfigurationDuplicatesValidator {
    /**
     * Validates that there are no duplicate keys in the passed configuration.
     *
     * @param cfg configuration in HOCON or JSON format.
     */
    public static Collection<ValidationIssue> validate(String cfg) {
        Object root = getConfigRoot(cfg);

        Queue<Node> queue = new ArrayDeque<>();
        queue.add(new Node(root, null, null));

        Set<String> paths = new HashSet<>();
        Set<ValidationIssue> issues = new HashSet<>();

        while (!queue.isEmpty()) {
            Node currentNode = queue.poll();
            List<Object> nodeChildren = getChildrenOrEmpty(currentNode.configNode);

            if (nodeChildren.isEmpty()) {
                continue;
            }

            Path currentPath = ConfigNodePath.path(currentNode.basePath, nodeChildren, currentNode.indexInArray);

            if (currentPath != null) {
                String pathString = currentPath.toString();

                if (!paths.add(pathString)) {
                    issues.add(new ValidationIssue(pathString, "Duplicated key"));
                }
            } else {
                currentPath = currentNode.basePath;
            }

            int index = 0;
            for (Object child : nodeChildren) {
                if (ConfigNodeComplexValue.isInstance(child) || ConfigNodeField.isInstance(child)) {
                    Integer indexInArray = ConfigNodeArray.isInstance(currentNode.configNode)
                            ? index
                            : null;

                    queue.add(new Node(child, currentPath, indexInArray));
                    index++;
                }
            }
        }

        return issues;
    }

    private static Object getConfigRoot(String cfg) {
        ConfigDocument configDocument = Parseable.newString(cfg, ConfigParseOptions.defaults()).parseConfigDocument();
        assert SimpleConfigDocument.isInstance(configDocument);

        Object root = SimpleConfigDocument.root(configDocument);
        assert ConfigNodeComplexValue.isInstance(root);

        return root;
    }

    private static List<Object> getChildrenOrEmpty(Object configNode) {
        if (ConfigNodeComplexValue.isInstance(configNode)) {
            return ConfigNodeComplexValue.children(configNode);
        } else if (ConfigNodeField.isInstance(configNode)) {
            return ConfigNodeField.children(configNode);
        } else {
            return List.of();
        }
    }

    private static class ConfigNodeComplexValue {
        private static final Class<?> CLASS;
        private static final Field CHILDREN_FIELD;

        static {
            try {
                CLASS = Class.forName("com.typesafe.config.impl.ConfigNodeComplexValue");

                CHILDREN_FIELD = CLASS.getDeclaredField("children");
                CHILDREN_FIELD.setAccessible(true);
            } catch (Exception e) {
                // Shouldn't happen unless library structure is changed
                throw new RuntimeException(e);
            }
        }

        private static boolean isInstance(Object configNode) {
            return CLASS.isInstance(configNode);
        }

        private static List<Object> children(Object configNodeComplexValue) {
            try {
                return (List<Object>) CHILDREN_FIELD.get(configNodeComplexValue);
            } catch (IllegalAccessException e) {
                // Shouldn't happen
                throw new RuntimeException(e);
            }
        }
    }

    private static class ConfigNodeArray {
        private static final Class<?> CLASS;

        static {
            try {
                CLASS = Class.forName("com.typesafe.config.impl.ConfigNodeArray");
            } catch (Exception e) {
                // Shouldn't happen unless library structure is changed
                throw new RuntimeException(e);
            }
        }

        private static boolean isInstance(Object configNode) {
            return CLASS.isInstance(configNode);
        }
    }

    private static class ConfigNodeField {
        private static final Class<?> CLASS;
        private static final Field CHILDREN_FIELD;

        static {
            try {
                CLASS = Class.forName("com.typesafe.config.impl.ConfigNodeField");

                CHILDREN_FIELD = CLASS.getDeclaredField("children");
                CHILDREN_FIELD.setAccessible(true);
            } catch (Exception e) {
                // Shouldn't happen unless library structure is changed
                throw new RuntimeException(e);
            }
        }

        private static boolean isInstance(Object configNode) {
            return CLASS.isInstance(configNode);
        }

        private static List<Object> children(Object configNode) {
            try {
                return (List<Object>) CHILDREN_FIELD.get(configNode);
            } catch (IllegalAccessException e) {
                // Shouldn't happen
                throw new RuntimeException(e);
            }
        }
    }

    private static class SimpleConfigDocument {
        private static final Class<?> CLASS;
        private static final Field NODE_TREE_FIELD;

        static {
            try {
                CLASS = Class.forName("com.typesafe.config.impl.SimpleConfigDocument");

                NODE_TREE_FIELD = CLASS.getDeclaredField("configNodeTree");
                NODE_TREE_FIELD.setAccessible(true);
            } catch (Exception e) {
                // Shouldn't happen unless library structure is changed
                throw new RuntimeException(e);
            }
        }

        private static boolean isInstance(Object configDocument) {
            return CLASS.isInstance(configDocument);
        }

        private static Object root(Object simpleConfigDocument) {
            try {
                return NODE_TREE_FIELD.get(simpleConfigDocument);
            } catch (IllegalAccessException e) {
                // Shouldn't happen
                throw new RuntimeException(e);
            }
        }
    }

    private static class ConfigNodePath {
        private static final Class<?> CLASS;
        private static final Class<?> PATH_CLASS;
        private static final Field PATH_FIELD;
        private static final Method PATH_RENDER_METHOD;

        static {
            try {
                CLASS = Class.forName("com.typesafe.config.impl.ConfigNodePath");
                PATH_CLASS = Class.forName("com.typesafe.config.impl.Path");

                PATH_FIELD = CLASS.getDeclaredField("path");
                PATH_FIELD.setAccessible(true);

                PATH_RENDER_METHOD = PATH_CLASS.getDeclaredMethod("render");
                PATH_RENDER_METHOD.setAccessible(true);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        private static boolean isInstance(Object configNode) {
            return CLASS.isInstance(configNode);
        }

        /** Returns full path to the given node. Elements of array don't have a NodePath child, so index is used instead. */
        private static @Nullable Path path(@Nullable Path basePath, List<Object> children, @Nullable Integer index) {
            if (index != null) {
                return new Path(basePath, "[" + index + "]");
            }

            Object pathNode = null;

            for (Object child : children) {
                if (isInstance(child)) {
                    pathNode = child;
                }
            }

            if (pathNode == null) {
                return null;
            }

            try {
                Object path = PATH_FIELD.get(pathNode);

                return new Path(basePath, PATH_RENDER_METHOD.invoke(path).toString());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static class Node {
        private final Object configNode;
        private final @Nullable Path basePath;
        private final @Nullable Integer indexInArray;

        private Node(Object configNode, @Nullable Path basePath, @Nullable Integer indexInArray) {
            this.configNode = configNode;
            this.basePath = basePath;
            this.indexInArray = indexInArray;
        }
    }

    private static class Path {
        private final @Nullable Path basePath;
        private final String path;

        private Path(@Nullable Path basePath, String path) {
            this.basePath = basePath;
            this.path = path;
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();

            appendTo(builder);

            return builder.toString();
        }

        private void appendTo(StringBuilder builder) {
            if (basePath != null) {
                basePath.appendTo(builder);
                builder.append('.');
            }

            builder.append(path);
        }
    }
}
