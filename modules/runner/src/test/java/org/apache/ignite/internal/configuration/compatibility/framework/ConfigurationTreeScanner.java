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
import java.util.Comparator;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.annotation.ConfigValue;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.internal.configuration.compatibility.framework.ConfigNode.Attributes;
import org.apache.ignite.internal.configuration.compatibility.framework.ConfigNode.Flags;

/*
 TODO: support extension nodes. see {@link org.apache.ignite.configuration.annotation.ConfigurationExtension}.
 TODO: support removed nodes. {@link ConfigurationModule#deletedPrefixes()}.
 TODO: support user names. See {@link org.apache.ignite.configuration.annotation.Name} annotation. @PublicName ?
 TODO: support renamed nodes. See {@link org.apache.ignite.configuration.annotation.PublicName} annotation.
 TODO: support polimorphic nodes. See {@link org.apache.ignite.configuration.annotation.PolymorphicConfig} annotation.
 TODO: support named lists. See {@link org.apache.ignite.configuration.annotation.NamedConfigValue} annotation.
 TODO: support {@link java.lang.Deprecated} annotation.
 TODO: support {@link org.apache.ignite.configuration.validation.Range} annotation.
 TODO: support {@link org.apache.ignite.configuration.validation.Endpoint} annotation.
 TODO: support {@link org.apache.ignite.configuration.validation.PowerOfTwo} annotation.
 TODO: support {@link org.apache.ignite.configuration.validation.OneOf} annotation.
 TODO: support {@link org.apache.ignite.configuration.validation.NotBlank} annotation.
 TODO: support {@link org.apache.ignite.configuration.validation.Immutable} annotation. ???
 TODO: support {@link org.apache.ignite.configuration.validation.ExceptKeys} annotation.
 TODO: support {@link org.apache.ignite.configuration.validation.CamelCaseKeys} annotation.
 TODO: support {@link org.apache.ignite.internal.network.configuration.MulticastAddress} annotation. ???
 TODO: support {@link org.apache.ignite.internal.network.configuration.SslConfigurationValidator} annotation. ???
 TODO: validate name uniqueness. ???
 TODO: support storing flags instead of booleans (isRoot, isValue, isDeprecated) ??
*/

/**
 * Provides method to extract metadata from project configuration classes.
 */
public class ConfigurationTreeScanner {
    private static final Set<Class<?>> SUPPORTED_FIELD_ANNOTATIONS = Set.of(
            Value.class
    );

    /**
     * Scans the given configuration class and returns a tree that describes configuration structure.
     */
    public static void scanClass(Class<?> nodeClass, ConfigNode currentNode, Map<Class<?>, Set<Class<?>>> extensions) {
        assert nodeClass != null && nodeClass.getName().startsWith("org.apache.ignite");

        if (extensions.containsKey(nodeClass)) {
            extensions.get(nodeClass).stream()
                    .sorted(Comparator.comparing(Class::getName)) // Sort for test stability.
                    .forEach(ext -> scanClass(ext, currentNode, extensions));

            return;
        }

        List<ConfigNode> childs = new ArrayList<>();
        Arrays.stream(nodeClass.getFields())
                .filter(field -> !Modifier.isStatic(field.getModifiers()))
                .sorted(Comparator.comparing(Field::getName)) // Sort for test stability.
                .forEach(field -> {
                    ConfigNode node = createNodeForField(currentNode, field);
                    childs.add(node);
                    if (!node.isValue()) {
                        scanClass(field.getType(), node, extensions);
                    }
                });

        currentNode.addChilds(childs);
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

}

