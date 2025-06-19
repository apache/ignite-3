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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.annotation.ConfigValue;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.configuration.annotation.Value;
import org.jetbrains.annotations.Nullable;

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
    public static InnerNode scanClass(String name, Class<?> clazz, @Nullable InnerNode parent) {
        assert clazz != null && clazz.getName().startsWith("org.apache.ignite");

        List<ConfigNode> children = new ArrayList<>();

        InnerNode currentNode = new InnerNode(name, clazz.getCanonicalName(), parent, children);
        Arrays.stream(clazz.getFields())
                .filter(field -> !Modifier.isStatic(field.getModifiers()))
                .sorted(Comparator.comparing(Field::getName)) // Sort for test stability.
                .forEach(field -> {
                    boolean isLeaf = !field.isAnnotationPresent(NamedConfigValue.class)
                            && !field.isAnnotationPresent(ConfigValue.class);

                    if (isLeaf) {
                        children.add(createLeafNode(currentNode, field));
                    } else {
                        children.add(createInnerNode(currentNode, field));
                    }
                });

        return currentNode;
    }

    private static String collectAdditionalAnnotations(Field field) {
        return Arrays.stream(field.getDeclaredAnnotations())
                .map(Annotation::annotationType)
                .filter(not(SUPPORTED_FIELD_ANNOTATIONS::contains))
                .map(Class::getSimpleName)
                .collect(Collectors.joining(",", "[", "]"));
    }

    private static InnerNode createInnerNode(InnerNode parentNode, Field field) {
        InnerNode innerNode = scanClass(field.getName(), field.getType(), parentNode);

        assert innerNode != null : parentNode.type() + "#" + field.getName();

        return innerNode;
    }

    private static ConfigNode createLeafNode(ConfigNode parent, Field field) {
        String annotations = collectAdditionalAnnotations(field);

        Map<String, String> additionalAttributes = annotations.isEmpty() ? Map.of() : Map.of("annotations", annotations);

        return new ValueNode(field.getName(), field.getType().getCanonicalName(), parent, additionalAttributes);
    }
}

