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

import static java.util.function.Predicate.not;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.ConfigurationModule;
import org.apache.ignite.configuration.annotation.AbstractConfiguration;
import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.ConfigValue;
import org.apache.ignite.configuration.annotation.ConfigurationExtension;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.configuration.annotation.PolymorphicConfig;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.internal.configuration.compatibility.framework.ConfigNode.Attributes;
import org.apache.ignite.internal.configuration.compatibility.framework.ConfigNode.Flags;
import org.apache.ignite.internal.configuration.util.ConfigurationUtil;

/*
 TODO: https://issues.apache.org/jira/browse/IGNITE-25575
   implement tree compatibility checks.
 TODO: https://issues.apache.org/jira/browse/IGNITE-25573
   support removed nodes. {@link ConfigurationModule#deletedPrefixes()}.
   support user names. See {@link org.apache.ignite.configuration.annotation.Name} annotation. @PublicName ?
   support renamed nodes. See {@link org.apache.ignite.configuration.annotation.PublicName} annotation.
 TODO: https://issues.apache.org/jira/browse/IGNITE-25571
   support named lists. See {@link org.apache.ignite.configuration.annotation.NamedConfigValue} annotation.
 TODO: https://issues.apache.org/jira/browse/IGNITE-25572
   support polymorphic nodes. See {@link org.apache.ignite.configuration.annotation.PolymorphicConfig} annotation.
 TODO https://issues.apache.org/jira/browse/IGNITE-25747
   support {@link java.lang.Deprecated} annotation.
   support {@link org.apache.ignite.configuration.validation.Range} annotation.
   support {@link org.apache.ignite.configuration.validation.Endpoint} annotation.
   support {@link org.apache.ignite.configuration.validation.PowerOfTwo} annotation.
   support {@link org.apache.ignite.configuration.validation.OneOf} annotation.
   support {@link org.apache.ignite.configuration.validation.NotBlank} annotation.
   support {@link org.apache.ignite.configuration.validation.Immutable} annotation. ???
   support {@link org.apache.ignite.configuration.validation.ExceptKeys} annotation.
   support {@link org.apache.ignite.configuration.validation.CamelCaseKeys} annotation.
   support {@link org.apache.ignite.internal.network.configuration.MulticastAddress} annotation. ???
   support {@link org.apache.ignite.internal.network.configuration.SslConfigurationValidator} annotation. ???
*/

/**
 * Provides method to extract metadata from project configuration classes.
 */
public class ConfigurationTreeScanner {
    private static final Set<Class<?>> SUPPORTED_FIELD_ANNOTATIONS = Set.of(
            Value.class
    );

    /**
     * Scans the given configuration class and populates the configuration tree structure.
     *
     * @param currentNode The current node in the configuration tree.
     * @param schemaClass The configuration schema class to scan.
     * @param context The context containing dependency information.
     */
    public static void scan(ConfigNode currentNode, Class<?> schemaClass, ScanContext context) {
        assert schemaClass != null && schemaClass.getName().startsWith("org.apache.ignite");

        Set<Class<?>> extensions = context.getExtensions(schemaClass);
        if (!extensions.isEmpty()) {
            extensions.stream()
                    .sorted(Comparator.comparing(Class::getName)) // Sort for test stability.
                    .forEach(ext -> scan(currentNode, ext, context));

            return;
        }

        List<ConfigNode> children = new ArrayList<>();
        configurationClasses(schemaClass).stream()
                .flatMap(c -> Arrays.stream(c.getDeclaredFields()))
                .filter(field -> !Modifier.isStatic(field.getModifiers()))
                .sorted(Comparator.comparing(Field::getName)) // Sort for test stability.
                .forEach(field -> {
                    ConfigNode node = createNodeForField(currentNode, field);

                    children.add(node);
                    if (!node.isValue()) {
                        scan(node, field.getType(), context);
                    }
                });

        currentNode.addChildNodes(children);
    }

    private static List<Class<?>> configurationClasses(Class<?> configClass) {
        List<Class<?>> classes = new ArrayList<>();
        Class<?> current = configClass;
        while (current != Object.class) {
            assert current.isAnnotationPresent(Config.class)
                    || current.isAnnotationPresent(ConfigurationRoot.class)
                    || current.isAnnotationPresent(PolymorphicConfig.class)
                    || current.isAnnotationPresent(ConfigurationExtension.class)
                    || current.isAnnotationPresent(AbstractConfiguration.class) : current;

            classes.add(current);

            current = current.getSuperclass();
        }

        return classes;
    }

    private static String collectAdditionalAnnotations(Field field) {
        return Arrays.stream(field.getDeclaredAnnotations())
                .map(Annotation::annotationType)
                .filter(not(SUPPORTED_FIELD_ANNOTATIONS::contains))
                .map(Class::getSimpleName)
                .collect(Collectors.joining(",", "[", "]"));
    }

    private static ConfigNode createNodeForField(ConfigNode parent, Field field) {
        String annotations = collectAdditionalAnnotations(field);

        EnumSet<ConfigNode.Flags> flags = extractFlags(field);

        Map<String, String> attributes = new LinkedHashMap<>();
        attributes.put(Attributes.NAME, field.getName());
        attributes.put(Attributes.CLASS, field.getType().getCanonicalName());
        attributes.put(Attributes.ANNOTATIONS, annotations);

        return new ConfigNode(parent, attributes, flags);
    }

    private static EnumSet<ConfigNode.Flags> extractFlags(Field field) {
        EnumSet<ConfigNode.Flags> flags = EnumSet.noneOf(ConfigNode.Flags.class);

        if (!field.isAnnotationPresent(NamedConfigValue.class)
                && !field.isAnnotationPresent(ConfigValue.class)) {
            flags.add(Flags.IS_VALUE);
        }

        if (field.isAnnotationPresent(Deprecated.class)) {
            flags.add(Flags.IS_DEPRECATED);
        }

        return flags;
    }

    /**
     * Context holder contains required metadata to resolve dependencies between configuration classes.
     */
    public static class ScanContext {
        /**
         * Factory method to create a new scan context based on the provided configuration module.
         */
        public static ScanContext create(ConfigurationModule module) {
            return new ScanContext(module.schemaExtensions(), module.polymorphicSchemaExtensions());
        }

        private final Map<Class<?>, Set<Class<?>>> extensions;
        private final Map<Class<?>, Set<Class<?>>> polymorphicExtensions;

        ScanContext(Collection<Class<?>> extensions, Collection<Class<?>> polymorphicExtensions) {
            this.extensions = ConfigurationUtil.schemaExtensions(extensions);
            this.polymorphicExtensions = ConfigurationUtil.polymorphicSchemaExtensions(polymorphicExtensions);
        }

        /**
         * Returns the set of extension classes for the given extended class.
         *
         * @param extendedClass The class whose extensions should be retrieved.
         */
        public Set<Class<?>> getExtensions(Class<?> extendedClass) {
            return extensions.getOrDefault(extendedClass, Set.of());
        }


        /**
         * Returns the set of polymorphic instance classes for the given polymorphic class.
         *
         * @param polymorphicClass The polymorphic class whose implementations instances should be retrieved.
         */
        public Set<Class<?>> getPolymorphicInstances(Class<?> polymorphicClass) {
            return polymorphicExtensions.getOrDefault(polymorphicClass, Set.of());
        }
    }
}

